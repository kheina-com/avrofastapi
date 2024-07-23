import asyncio
import json
from asyncio import Lock
from collections import OrderedDict
from contextlib import AsyncExitStack
from email.message import Message as EmailMessage
from enum import Enum
from hashlib import md5
from logging import Logger, getLogger
from typing import Any, Callable, Dict, Hashable, Iterator, List, Optional, Sequence, Set, Tuple, Type, Union
from uuid import UUID, uuid4
from warnings import warn

from avro.compatibility import ReaderWriterCompatibilityChecker, SchemaCompatibilityResult, SchemaCompatibilityType
from avro.schema import Schema, parse
from fastapi import params
from fastapi.datastructures import Default, DefaultPlaceholder
from fastapi.dependencies.models import Dependant
from fastapi.dependencies.utils import solve_dependencies
from fastapi.exceptions import RequestValidationError
from fastapi.responses import Response
from fastapi.routing import APIRoute, APIRouter, run_endpoint_function, serialize_response
from fastapi.types import DecoratedCallable
from fastapi.utils import generate_unique_id
from pydantic import BaseModel
from pydantic.error_wrappers import ErrorWrapper
from pydantic.fields import ModelField, Undefined
from starlette.datastructures import Headers
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import BaseRoute
from starlette.types import ASGIApp, Lifespan, Receive, Scope, Send

from avrofastapi.handshake import MD5, AvroMessage, AvroProtocol, CallRequest, CallResponse, HandshakeMatch, HandshakeRequest, HandshakeResponse
from avrofastapi.models import Error, ValidationError, ValidationErrorDetail
from avrofastapi.repo import name
from avrofastapi.schema import AvroSchema, convert_schema
from avrofastapi.serialization import AvroDeserializer, AvroSerializer, avro_frame, read_avro_frames


SetIntStr = Set[Union[int, str]]
DictIntStrAny = Dict[Union[int, str], Any]


class CalcDict(dict) :

	def __init__(self, default: Callable[[Hashable], Any]) -> None :
		self.default: Callable = default


	def setdefault(self, default: Callable[[Hashable], Any]) -> None :
		self.default = default


	def __missing__(self, key: Hashable) -> Any :
		self[key] = self.default(key)
		return self[key]


# number of client protocols to cache per endpoint
# this should be set to something reasonable based on the number of expected consumers per endpoint
# TODO: potentially dynamically set this based on number of clients in a given timeframe?
client_protocol_max_size: int = 10

AvroChecker: ReaderWriterCompatibilityChecker = ReaderWriterCompatibilityChecker()
handshake_deserializer: AvroDeserializer = AvroDeserializer(HandshakeRequest)
call_request_deserializer: AvroDeserializer = AvroDeserializer(CallRequest)
handshake_serializer: AvroSerializer = AvroSerializer(HandshakeResponse)
call_serializer: AvroSerializer = AvroSerializer(CallResponse)

logger: Logger = getLogger('avrofastapi.routing')


class AvroDecodeError(Exception) :
	pass


class AvroJsonResponse(Response) :

	# items are written to the cache in the form of type: serializer
	# this only occurs with response models, error models are cached elsewhere
	# TODO: remove
	__writer_cache__ = CalcDict(AvroSerializer)

	def __init__(self: 'AvroJsonResponse', serializable_body: dict = None, model: BaseModel = None, *args: Any, serializer: AvroSerializer = None, handshake: HandshakeResponse = None, error: bool = False, **kwargs: Any) :
		super().__init__(None, *args, **kwargs)
		self._serializable_body: Optional[dict] = serializable_body
		self._model: Optional[BaseModel] = model
		self._serializer: Optional[AvroSerializer] = serializer
		self._handshake: Optional[HandshakeResponse] = handshake
		self._error: bool = error


	async def __call__(self: 'AvroJsonResponse', scope: Scope, receive: Receive, send: Send) :
		request: Request = Request(scope, receive, send)

		if 'avro/binary' in request.headers.get('accept', '') :

			handshake: HandshakeResponse = request.scope['avro_handshake'] if 'avro_handshake' in request.scope else self._handshake
			serializer: AvroSerializer = self._serializer or self.__writer_cache__[type(self._model)]

			if handshake and not (
				# check to see if they are using header protocol and we can skip returning handshake
				handshake.match == HandshakeMatch.both and
				'avro-server-hash' in request.headers and
				'avro-client-hash' in request.headers and
				'avro-handshake' in request.headers and not
				request.headers['avro-handshake'] == 'true'
			) :
				if self._model :
					logger.debug('response: handshake.%s + model: %s', handshake.match.name, type(self._model).__name__)
					self.body = (
						avro_frame(handshake_serializer(handshake)) +
						avro_frame(
							call_serializer(
								CallResponse(
									error=self._error,
									response=serializer(self._model)
								),
							)
						) +
						avro_frame()
					)

				else :
					logger.debug('response: handshake.%s only', handshake.match.name)
					self.body = (
						avro_frame(handshake_serializer(handshake)) +
						avro_frame()
					)

			elif self._model :
				print('response: only model: %s', type(self._model).__name__)
				self.body = (
					avro_frame(
						call_serializer(
							CallResponse(
								error=self._error,
								response=serializer(self._model)
							),
						)
					) +
					avro_frame()
				)

			else :
				raise ValueError('at least a handshake or model is required to return an avro response')

			self.status_code = 200
			self.headers.update({
				'content-type': 'avro/binary',
				'avro-handshake-match': handshake.match.value if handshake else HandshakeMatch.none.value,
			})

		elif self._serializable_body :
			self.body = json.dumps(self._serializable_body).encode()

		elif self._model :
			self.body = self._model.json().encode()

		if self.body :
			self.headers['content-length'] = str(len(self.body))

		await super().__call__(scope, receive, send)


class AvroRoute(APIRoute) :

	def __init__(
		self: 'AvroRoute',
        path: str,
        endpoint: Callable[..., Any],
        *,
        response_model: Any = None,
        status_code: Optional[int] = None,
        tags: Optional[List[Union[str, Enum]]] = None,
        dependencies: Optional[Sequence[params.Depends]] = None,
        summary: Optional[str] = None,
        description: Optional[str] = None,
        response_description: str = 'Successful Response',
        responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
        deprecated: Optional[bool] = None,
        name: Optional[str] = None,
        methods: Optional[Union[Set[str], List[str]]] = None,
        operation_id: Optional[str] = None,
        response_model_include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
        response_model_exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        include_in_schema: bool = True,
        response_class: Union[Type[Response], DefaultPlaceholder] = Default(AvroJsonResponse),
        dependency_overrides_provider: Optional[Any] = None,
        callbacks: Optional[List[BaseRoute]] = None,
        openapi_extra: Optional[Dict[str, Any]] = None,
        generate_unique_id_function: Union[
            Callable[['APIRoute'], str], DefaultPlaceholder
        ] = Default(generate_unique_id),
	) :
		"""
		in an effort to make this as user-friendly as possible and keep setup to a one-line
		change, we're going to override the `default_response_class` argument, but default
		it in case an endpoint uses a custom response we can't handle
		"""
		# TODO: router will always pass JSONResponse, but we want to override that (for now)
		response_class = Default(AvroJsonResponse)
		status_code = status_code or 200
		super().__init__(
			path,
			endpoint,
			response_model=response_model,
			status_code=status_code,
			tags=tags,
			dependencies=dependencies,
			summary=summary,
			description=description,
			response_description=response_description,
			responses=responses,
			deprecated=deprecated,
			name=name,
			methods=methods,
			operation_id=operation_id,
			response_model_include=response_model_include,
			response_model_exclude=response_model_exclude,
			response_model_by_alias=response_model_by_alias,
			response_model_exclude_unset=response_model_exclude_unset,
			response_model_exclude_defaults=response_model_exclude_defaults,
			response_model_exclude_none=response_model_exclude_none,
			include_in_schema=include_in_schema,
			response_class=response_class,
			dependency_overrides_provider=dependency_overrides_provider,
			callbacks=callbacks,
			openapi_extra=openapi_extra,
			generate_unique_id_function=generate_unique_id_function,
		)

		if self.body_field :
			body_schema = convert_schema(self.body_field.type_)
			self.body_schema = parse(json.dumps(body_schema))
			self.schema_name = body_schema['name']
			self.schema_namespace = body_schema['namespace']

		if self.response_model is None and self.status_code not in { 204 } :
			warn('in order for the avro handshake to be performed, a response model must be passed or the status code set to 204')

		if self.response_model :
			self.response_schema = convert_schema(self.response_model)


	async def handle_avro(self: 'AvroRoute', scope: Scope, receive: Receive, send: Send) -> None :
		return await self.app(scope, receive, send)


	def get_route_handler(self: 'AvroRoute') -> ASGIApp :
		# TODO: these don't need to be re-assigned
		dependant: Dependant = self.dependant
		body_field: Optional[ModelField] = self.body_field
		status_code: Optional[int] = self.status_code
		response_class: Union[Type[Response], DefaultPlaceholder] = self.response_class
		response_field: Optional[ModelField] = self.secure_cloned_response_field
		response_model_include: Optional[Union[SetIntStr, DictIntStrAny]] = self.response_model_include
		response_model_exclude: Optional[Union[SetIntStr, DictIntStrAny]] = self.response_model_exclude
		response_model_by_alias: bool = self.response_model_by_alias
		response_model_exclude_unset: bool = self.response_model_exclude_unset
		response_model_exclude_defaults: bool = self.response_model_exclude_defaults
		response_model_exclude_none: bool = self.response_model_exclude_none
		dependency_overrides_provider: Optional[Any] = self.dependency_overrides_provider

		assert dependant.call is not None, 'dependant.call must be a function'

		is_coroutine = asyncio.iscoroutinefunction(dependant.call)
		is_body_form = body_field and isinstance(body_field.field_info, params.Form)

		if isinstance(response_class, DefaultPlaceholder) :
			actual_response_class: Type[Response] = response_class.value

		else :
			actual_response_class = response_class


		async def app(request: Request) -> Response :
			# optimize
			try :
				body: Any = None
				content_type_value: Optional[str] = request.headers.get('content-type')

				if content_type_value == 'avro/binary' :
					body = request.scope['avro_body']

				elif body_field :

					if is_body_form :
						body = await request.form()

					else :
						body_bytes = await request.body()

						if body_bytes :
							json_body: Any = Undefined

							if not content_type_value :
								json_body = await request.json()

							else :
								message = EmailMessage()
								message['content-type'] = content_type_value

								if message.get_content_maintype() == 'application' :
									subtype = message.get_content_subtype()

									if subtype == 'json' or subtype.endswith('+json') :
										json_body = await request.json()

							if json_body != Undefined :
								body = json_body

							else :
								body = body_bytes

			except json.JSONDecodeError as e :
				# TODO: this will need to be replaced as well
				# print([ErrorWrapper(e, ('body', e.pos))])
				raise RequestValidationError([ErrorWrapper(e, ('body', e.pos))], body=e.doc)

			except AvroDecodeError :
				server_protocol: str
				protocol_hash: bytes
				serializer: AvroSerializer
				server_protocol, protocol_hash, serializer = request.scope['router']._server_protocol
				error: str = 'avro handshake failed, client protocol incompatible'

				return AvroJsonResponse(
					model=Error(
						status=400,
						error=error,
					),
					serializer=serializer,
					handshake=HandshakeResponse(
						match=HandshakeMatch.none,
						serverProtocol=server_protocol,
						serverHash=protocol_hash,
					),
					error=True,
				)

			except Exception as e :
				server_protocol: str
				protocol_hash: bytes
				serializer: AvroSerializer
				server_protocol, protocol_hash, serializer = request.scope['router']._server_protocol
				error: str = 'There was an error parsing the body: ' + str(e)

				return AvroJsonResponse(
					model=Error(
						status=400,
						error=error,
					),
					serializer=serializer,
					handshake=HandshakeResponse(
						match=HandshakeMatch.none,
						serverProtocol=server_protocol,
						serverHash=protocol_hash,
					),
					error=True,
				)

			async with AsyncExitStack() as async_exit_stack:
				solved_result = await solve_dependencies(
					request=request,
					dependant=dependant,
					body=body,
					dependency_overrides_provider=dependency_overrides_provider,
					async_exit_stack=async_exit_stack,
				)
				values, errors, background_tasks, sub_response, _ = solved_result
				# print(values, errors, background_tasks, sub_response, actual_response_class, self.response_class)

				if errors :
					serializer: AvroSerializer
					_, _, serializer = request.scope['router']._server_protocol
					error = ValidationError(detail=[ValidationErrorDetail(**e) for e in errors[0].exc.errors()])
					return AvroJsonResponse(
						model=error,
						serializer=serializer,
						error=True,
					)

				else :
					raw_response = await run_endpoint_function(
						dependant=dependant, values=values, is_coroutine=is_coroutine
					)

					if isinstance(raw_response, Response) :
						if raw_response.background is None :
							raw_response.background = background_tasks
						return raw_response

					response_data = await serialize_response(
						field=response_field,
						response_content=raw_response,
						include=response_model_include,
						exclude=response_model_exclude,
						by_alias=response_model_by_alias,
						exclude_unset=response_model_exclude_unset,
						exclude_defaults=response_model_exclude_defaults,
						exclude_none=response_model_exclude_none,
						is_coroutine=is_coroutine,
					)
					response_args: Dict[str, Any] = { 'background': background_tasks }

					# If status_code was set, use it, otherwise use the default from the
					# response class, in the case of redirect it's 307
					if status_code is not None :
						response_args['status_code'] = status_code

					response = actual_response_class(serializable_body=response_data, model=raw_response, **response_args)
					response.headers.raw.extend(sub_response.headers.raw)

					if sub_response.status_code :
						response.status_code = sub_response.status_code

					return response

		return app


class AvroRouter(APIRouter) :

	def __init__(
		self: 'AvroRouter',
		*,
		prefix: str = '',
		tags: Optional[List[Union[str, Enum]]] = None,
		dependencies: Optional[Sequence[params.Depends]] = None,
		default_response_class: Type[Response] = Default(AvroJsonResponse),
		responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
		callbacks: Optional[List[BaseRoute]] = None,
		routes: Optional[List[BaseRoute]] = None,
		redirect_slashes: bool = True,
		default: Optional[ASGIApp] = None,
		dependency_overrides_provider: Optional[Any] = None,
		route_class: Type[APIRoute] = AvroRoute,
		on_startup: Optional[Sequence[Callable[[], Any]]] = None,
		on_shutdown: Optional[Sequence[Callable[[], Any]]] = None,
		lifespan: Optional[Lifespan[Any]] = None,
		deprecated: Optional[bool] = None,
		include_in_schema: bool = True,
		generate_unique_id_function: Callable[[APIRoute], str] = Default(
			generate_unique_id
		),
	) -> None:
		super().__init__(
			prefix=prefix,
			tags=tags,
			dependencies=dependencies,
			default_response_class=default_response_class,
			responses=responses,
			callbacks=callbacks,
			routes=routes,
			redirect_slashes=redirect_slashes,
			default=default,
			dependency_overrides_provider=dependency_overrides_provider,
			route_class=route_class,
			on_startup=on_startup,
			on_shutdown=on_shutdown,
			lifespan=lifespan,
			deprecated=deprecated,
			include_in_schema=include_in_schema,
			generate_unique_id_function=generate_unique_id_function,
		)

		# format { route.unique_id: route }
		self._avro_routes: Dict[str, APIRoute] = { }

		protocol: str = AvroProtocol(
			protocol=name,
			namespace=name,
			messages={ },
		).json(exclude_none=True)

		# format: (protocol json, hash, serializer)
		# NOTE: these two errors are used automatically by this library and FastAPI, respectively
		self._server_protocol: Tuple[str, MD5, AvroSerializer] = (protocol, md5(protocol.encode()).digest(), AvroSerializer(Union[Error, ValidationError]))

		for route in self.routes :
			self.add_avro_route(route)
			self.add_server_protocol(route)

		# number of client protocols to cache per endpoint
		# this should be set to something reasonable based on the number of expected consumers per endpoint
		# optimize: potentially dynamically set this based on number of clients in a given timeframe?
		self._client_protocol_max_size: int = 100

		# format: { md5 hash: (request_deserializer, bool(client compatibility)) }
		self._client_protocol_cache: Dict[MD5, Tuple[AvroDeserializer, bool]] = OrderedDict()
		self._cache_lock: Lock = Lock()


	def add_avro_route(self: 'AvroRouter', route: APIRoute) -> None :
		if route.unique_id in self._avro_routes :
			warn(f'existing route found for route id {route.unique_id}. this route may be unreachable via avro: {self._avro_routes[route.unique_id]}')
		self._avro_routes[route.unique_id] = route


	def add_server_protocol(self: 'AvroRouter', route: APIRoute) -> None :
		# optimize: this function is extremely slow right now due to re-parsing and re-generating all schemas. it doesn't really matter too much because this is only run during server startup
		p, _, serializer = self._server_protocol
		protocol: AvroProtocol = AvroProtocol.parse_raw(p)

		types: List[dict] = list(map(convert_schema, serializer._model.__args__))

		# there needs to be a separte refs objects vs enames being a set is due to ordering and
		# serialization/deserialization of a union being order-dependent
		enames = list(map(lambda t : t.__name__, serializer._model.__args__))
		refs = set(enames)
		errors = serializer._model

		# print(dir(request.scope['router'].routes[-1]))
		# print(request.scope['router'].routes[-1].__dict__)

		for r in self.routes :
			for status, response in getattr(r, 'responses', { }).items() :
				if status >= 400 and 'model' in response :
					error = convert_schema(response['model'], error=True)
					if error['name'] not in refs :
						# errors = Union[errors, response['model']]
						types.append(error)
						refs.add(error['name'])
						enames.append(error['name'])

		# TODO: we should iterate over the other routes in protocol.messages here to add the new errors and types to other routes

		protocol.messages[route.unique_id] = AvroMessage(
			doc='the openapi description should go here. ex: V1Endpoint',
			types=types + ([route.response_schema] if route.response_model else []),
			# optimize
			request=convert_schema(route.body_field.type_)['fields'] if route.body_field else [],
			# optimize
			response=route.response_schema['name'] if route.response_model else 'null',
			errors=enames,
		)

		protocol_json: str = protocol.json(exclude_none=True)
		self._server_protocol = protocol_json, md5(protocol_json.encode()).digest(), AvroSerializer(errors)


	async def check_schema_compatibility(self: 'AvroRouter', handshake: HandshakeRequest) -> Tuple[Dict[str, AvroDeserializer], bool] :
		"""
		returns map of { route_id -> AvroDeserializer } and client compatibility bool
		client compatibility = true: HandshakeMatch.both
		client compatibility = false: HandshakeMatch.client
		raises AvroDecodeError: HandshakeMatch.none
		"""
		if handshake.clientHash in self._client_protocol_cache :
			return self._client_protocol_cache[handshake.clientHash]

		if not handshake.clientProtocol :
			raise AvroDecodeError('client request protocol was not included and client request hash was not cached.')

		client_protocol = AvroProtocol.parse_raw(handshake.clientProtocol)
		request_deserializers: Dict[str, AvroDeserializer] = { }
		client_compatible: bool = True

		for route_id, client_message in client_protocol.messages.items() :
			route: APIRoute = self._avro_routes.get(route_id)

			if route is None :
				raise AvroDecodeError(f'route does not exist for client protocol message {route_id}.')

			client_protocol_types = { v['name']: v for v in client_message.types }


			# Check client REQUEST for compatibility
			client_protocol_request_schema = None

			if client_message.request :

				if route.body_field :
					request_schema: AvroSchema = {
						'type': 'record',
						'name': route.schema_name,
						'namespace': route.schema_namespace,
						'fields': [
							({ 'type': client_protocol_types[r.pop('type')], **r } if r['type'] in client_protocol_types else r)
							for r in client_message.request
						],
					}

					client_protocol_request_schema = parse(json.dumps(request_schema))

					# optimize
					request_compatibility: SchemaCompatibilityResult = AvroChecker.get_compatibility(
						reader=parse(json.dumps(convert_schema(route.body_field.type_))),
						writer=client_protocol_request_schema,
					)

					if not request_compatibility.compatibility == SchemaCompatibilityType.compatible :
						raise AvroDecodeError('client request protocol is incompatible.')

					request_deserializers[route.unique_id] = AvroDeserializer(route.body_field.type_, route.body_schema, client_protocol_request_schema, parse=False)

				else :
					raise AvroDecodeError('client protocol provided a request but route does not expect one.')

			elif route.body_field :
				raise AvroDecodeError('client protocol did not provide a request but route expects one.')

			else :
				client_protocol_request_schema = None


			# Check client RESPONSE for compatibility
			if client_message.response != 'null' :
				response_schema = parse(json.dumps(
					client_protocol_types[client_message.response]
					if client_message.response in client_protocol_types
					else client_message.response
				))

			else :
				response_schema = None

			if response_schema :
				if route.response_model :
					# TODO: should this check error responses or only the successful response?
					# TODO: this also needs to check all message types in the CLIENT protocol
					# print('response_schema', response_schema, route.response_schema)
					response_compatibility: SchemaCompatibilityResult = AvroChecker.get_compatibility(
						reader=response_schema,
						# TODO: this should *definitely* be cached
						writer=parse(json.dumps(route.response_schema)),
					)
					client_compatible = client_compatible and response_compatibility.compatibility == SchemaCompatibilityType.compatible

			elif route.response_model :
				client_compatible = False

		data = self._client_protocol_cache[handshake.clientHash] = request_deserializers, client_compatible

		if len(self._client_protocol_cache) > self._client_protocol_max_size :
			# lock required in case two threads try to purge the cache at once
			async with self._cache_lock :
				# fetches all the keys that should be deleted
				for key in list(reversed(self._client_protocol_cache.keys()))[len(self._client_protocol_cache) - self._client_protocol_max_size:] :
					# TODO: potentially track the frequency with which protocols are removed from the cache
					# this should happen infrequently (or never) for greatest efficiency
					del self._client_protocol_cache[key]

		return data


	async def settle_avro_handshake(self: 'AvroRouter', request: Request) -> Optional[APIRoute] :
		body = await request.body()

		if not body :
			raise AvroDecodeError('no body was included with the avro request, a handshake must be provided with every request')

		frame_gen: Iterator = read_avro_frames(body)
		handshake_request: HandshakeRequest = None
		handshake_body: bytes = b''

		for frame in frame_gen :
			handshake_body += frame

			try :
				handshake_request = handshake_deserializer(handshake_body)
				del handshake_body
				break

			except TypeError :
				pass

		if not handshake_request :
			raise AvroDecodeError('There was an error parsing the avro handshake.')

		request_deserializers, response_compatibility = await self.check_schema_compatibility(handshake_request)
		server_protocol, protocol_hash, _ = self._server_protocol

		if handshake_request.serverHash == protocol_hash and response_compatibility :
			request.scope['avro_handshake'] = HandshakeResponse(
				match=HandshakeMatch.both,
			)

		else :
			request.scope['avro_handshake'] = HandshakeResponse(
				match=HandshakeMatch.client,
				serverHash=protocol_hash,
				serverProtocol=server_protocol,
			)

		call_request: CallRequest = None
		call_body: bytes = b''

		for frame in frame_gen :
			call_body += frame

			try :
				call_request = call_request_deserializer(call_body)
				del call_body
				break

			except TypeError :
				pass

		if not call_request :
			return None

		if call_request.message not in self._avro_routes :
			raise AvroDecodeError(f'{call_request.message} not found in valid messages ({", ".join(self._avro_routes.keys())})')

		route: APIRoute = self._avro_routes[call_request.message]

		if call_request.message not in request_deserializers :
			if route.body_field :
				raise AvroDecodeError(f'call request message {call_request.message} was not found in client protocol')

			else :
				request.scope['avro_body'] = None

		elif route.body_field : # optimize: is this check necessary?
			request.scope['avro_body'] = request_deserializers[call_request.message](call_request.request)

		return route


	async def __call__(self: 'AvroRouter', scope: Scope, receive: Receive, send: Send) -> None :
		if 'avro/binary' == Headers(scope=scope).get('content-type') :
			logger.debug('request path: %s', scope['path'])
			assert scope['type'] in {'http', 'websocket', 'lifespan'}

			if 'router' not in scope :
				scope['router'] = self

			if scope['type'] == 'lifespan' :
				await self.lifespan(scope, receive, send)
				return

			try :
				route: Optional[AvroRoute] = await self.settle_avro_handshake(Request(scope, receive, send))

				if not route :
					response: AvroJsonResponse = AvroJsonResponse(status_code=200)
					await response(scope, receive, send)
					return

				await route.handle_avro(scope, receive, send)

			except AvroDecodeError as e :
				server_protocol: str
				protocol_hash: bytes
				serializer: AvroSerializer

				server_protocol, protocol_hash, serializer = self._server_protocol
				response: Response

				if server_protocol :
					response = AvroJsonResponse(
						model=Error(
							status=400,
							error=str(e),
						),
						serializer=serializer,
						handshake=HandshakeResponse(
							match=HandshakeMatch.none,
							serverProtocol=server_protocol,
							serverHash=protocol_hash,
						),
						error=True,
						status_code=400,
					)

				else :
					response = JSONResponse(
						Error(
							status=404,
							error='avro handshake failed, there is no avro endpoint on this route',
						).dict(),
						status_code=404,
					)

				await response(scope, receive, send)

			except Exception :
				server_protocol: str
				protocol_hash: bytes
				serializer: AvroSerializer
				refid: UUID = uuid4()

				server_protocol, protocol_hash, serializer = self._server_protocol
				respsonse: Response

				logger.exception(f'Something went wrong while decoding the request body. path: {scope["path"]}, refid: {refid.hex}')

				if server_protocol :
					respsonse = AvroJsonResponse(
						model=Error(
							status=500,
							error='Internal Server Error: Something went wrong while decoding the request body.',
							refid=refid.bytes,
						),
						serializer=serializer,
						handshake=HandshakeResponse(
							match=HandshakeMatch.none,
							serverProtocol=server_protocol,
							serverHash=protocol_hash,
						),
						error=True,
						status_code=500,
					)

				else :
					respsonse = JSONResponse(
						Error(
							status=500,
							error='Internal Server Error: Something went wrong while decoding the request body.',
							refid=refid.bytes,
						).dict(),
						status_code=500,
					)

				await respsonse(scope, receive, send)

			return

		return await super().__call__(scope, receive, send)


	def add_api_route(
		self: 'AvroRouter',
		path: str,
		endpoint: Callable[..., Any],
		*,
		response_model: Any = None,
		status_code: Optional[int] = None,
		tags: Optional[List[Union[str, Enum]]] = None,
		dependencies: Optional[Sequence[params.Depends]] = None,
		summary: Optional[str] = None,
		description: Optional[str] = None,
		response_description: str = 'Successful Response',
		responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
		deprecated: Optional[bool] = None,
		methods: Optional[Union[Set[str], List[str]]] = None,
		operation_id: Optional[str] = None,
		response_model_include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_by_alias: bool = True,
		response_model_exclude_unset: bool = False,
		response_model_exclude_defaults: bool = False,
		response_model_exclude_none: bool = False,
		include_in_schema: bool = True,
		response_class: Union[Type[Response], DefaultPlaceholder] = Default(AvroJsonResponse),
		name: Optional[str] = None,
		route_class_override: Optional[Type[APIRoute]] = None,
		callbacks: Optional[List[BaseRoute]] = None,
		openapi_extra: Optional[Dict[str, Any]] = None,
		generate_unique_id_function: Union[
			Callable[[APIRoute], str], DefaultPlaceholder
		] = Default(generate_unique_id),
	) -> None:
		super().add_api_route(
			path,
			endpoint,
			response_model=response_model,
			status_code=status_code,
			tags=tags,
			dependencies=dependencies,
			summary=summary,
			description=description,
			response_description=response_description,
			responses=responses,
			deprecated=deprecated,
			methods=methods,
			operation_id=operation_id,
			response_model_include=response_model_include,
			response_model_exclude=response_model_exclude,
			response_model_by_alias=response_model_by_alias,
			response_model_exclude_unset=response_model_exclude_unset,
			response_model_exclude_defaults=response_model_exclude_defaults,
			response_model_exclude_none=response_model_exclude_none,
			include_in_schema=include_in_schema,
			response_class=response_class,
			name=name,
			route_class_override=route_class_override,
			callbacks=callbacks,
			openapi_extra=openapi_extra,
			generate_unique_id_function=generate_unique_id_function,
		)
		route = self.routes[-1]
		self.add_avro_route(route)
		self.add_server_protocol(route)

	def get(
		self: 'AvroRouter',
		path: str,
		*,
		response_model: Any = None,
		status_code: Optional[int] = None,
		tags: Optional[List[Union[str, Enum]]] = None,
		dependencies: Optional[Sequence[params.Depends]] = None,
		summary: Optional[str] = None,
		description: Optional[str] = None,
		response_description: str = 'Successful Response',
		responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
		deprecated: Optional[bool] = None,
		operation_id: Optional[str] = None,
		response_model_include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_by_alias: bool = True,
		response_model_exclude_unset: bool = False,
		response_model_exclude_defaults: bool = False,
		response_model_exclude_none: bool = False,
		include_in_schema: bool = True,
		response_class: Type[Response] = Default(AvroJsonResponse),
		name: Optional[str] = None,
		callbacks: Optional[List[BaseRoute]] = None,
		openapi_extra: Optional[Dict[str, Any]] = None,
		generate_unique_id_function: Callable[[APIRoute], str] = Default(
			generate_unique_id
		),
	) -> Callable[[DecoratedCallable], DecoratedCallable]:
		return self.api_route(
			path=path,
			response_model=response_model,
			status_code=status_code,
			tags=tags,
			dependencies=dependencies,
			summary=summary,
			description=description,
			response_description=response_description,
			responses=responses,
			deprecated=deprecated,
			methods=['GET'],
			operation_id=operation_id,
			response_model_include=response_model_include,
			response_model_exclude=response_model_exclude,
			response_model_by_alias=response_model_by_alias,
			response_model_exclude_unset=response_model_exclude_unset,
			response_model_exclude_defaults=response_model_exclude_defaults,
			response_model_exclude_none=response_model_exclude_none,
			include_in_schema=include_in_schema,
			response_class=response_class,
			name=name,
			callbacks=callbacks,
			openapi_extra=openapi_extra,
			generate_unique_id_function=generate_unique_id_function,
		)

	def put(
		self: 'AvroRouter',
		path: str,
		*,
		response_model: Any = None,
		status_code: Optional[int] = None,
		tags: Optional[List[Union[str, Enum]]] = None,
		dependencies: Optional[Sequence[params.Depends]] = None,
		summary: Optional[str] = None,
		description: Optional[str] = None,
		response_description: str = 'Successful Response',
		responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
		deprecated: Optional[bool] = None,
		operation_id: Optional[str] = None,
		response_model_include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_by_alias: bool = True,
		response_model_exclude_unset: bool = False,
		response_model_exclude_defaults: bool = False,
		response_model_exclude_none: bool = False,
		include_in_schema: bool = True,
		response_class: Type[Response] = Default(AvroJsonResponse),
		name: Optional[str] = None,
		callbacks: Optional[List[BaseRoute]] = None,
		openapi_extra: Optional[Dict[str, Any]] = None,
		generate_unique_id_function: Callable[[APIRoute], str] = Default(
			generate_unique_id
		),
	) -> Callable[[DecoratedCallable], DecoratedCallable]:
		return self.api_route(
			path=path,
			response_model=response_model,
			status_code=status_code,
			tags=tags,
			dependencies=dependencies,
			summary=summary,
			description=description,
			response_description=response_description,
			responses=responses,
			deprecated=deprecated,
			methods=['PUT'],
			operation_id=operation_id,
			response_model_include=response_model_include,
			response_model_exclude=response_model_exclude,
			response_model_by_alias=response_model_by_alias,
			response_model_exclude_unset=response_model_exclude_unset,
			response_model_exclude_defaults=response_model_exclude_defaults,
			response_model_exclude_none=response_model_exclude_none,
			include_in_schema=include_in_schema,
			response_class=response_class,
			name=name,
			callbacks=callbacks,
			openapi_extra=openapi_extra,
			generate_unique_id_function=generate_unique_id_function,
		)

	def post(
		self: 'AvroRouter',
		path: str,
		*,
		response_model: Any = None,
		status_code: Optional[int] = None,
		tags: Optional[List[Union[str, Enum]]] = None,
		dependencies: Optional[Sequence[params.Depends]] = None,
		summary: Optional[str] = None,
		description: Optional[str] = None,
		response_description: str = 'Successful Response',
		responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
		deprecated: Optional[bool] = None,
		operation_id: Optional[str] = None,
		response_model_include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_by_alias: bool = True,
		response_model_exclude_unset: bool = False,
		response_model_exclude_defaults: bool = False,
		response_model_exclude_none: bool = False,
		include_in_schema: bool = True,
		response_class: Type[Response] = Default(AvroJsonResponse),
		name: Optional[str] = None,
		callbacks: Optional[List[BaseRoute]] = None,
		openapi_extra: Optional[Dict[str, Any]] = None,
		generate_unique_id_function: Callable[[APIRoute], str] = Default(
			generate_unique_id
		),
	) -> Callable[[DecoratedCallable], DecoratedCallable]:
		return self.api_route(
			path=path,
			response_model=response_model,
			status_code=status_code,
			tags=tags,
			dependencies=dependencies,
			summary=summary,
			description=description,
			response_description=response_description,
			responses=responses,
			deprecated=deprecated,
			methods=['POST'],
			operation_id=operation_id,
			response_model_include=response_model_include,
			response_model_exclude=response_model_exclude,
			response_model_by_alias=response_model_by_alias,
			response_model_exclude_unset=response_model_exclude_unset,
			response_model_exclude_defaults=response_model_exclude_defaults,
			response_model_exclude_none=response_model_exclude_none,
			include_in_schema=include_in_schema,
			response_class=response_class,
			name=name,
			callbacks=callbacks,
			openapi_extra=openapi_extra,
			generate_unique_id_function=generate_unique_id_function,
		)

	def delete(
		self: 'AvroRouter',
		path: str,
		*,
		response_model: Any = None,
		status_code: Optional[int] = None,
		tags: Optional[List[Union[str, Enum]]] = None,
		dependencies: Optional[Sequence[params.Depends]] = None,
		summary: Optional[str] = None,
		description: Optional[str] = None,
		response_description: str = 'Successful Response',
		responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
		deprecated: Optional[bool] = None,
		operation_id: Optional[str] = None,
		response_model_include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_by_alias: bool = True,
		response_model_exclude_unset: bool = False,
		response_model_exclude_defaults: bool = False,
		response_model_exclude_none: bool = False,
		include_in_schema: bool = True,
		response_class: Type[Response] = Default(AvroJsonResponse),
		name: Optional[str] = None,
		callbacks: Optional[List[BaseRoute]] = None,
		openapi_extra: Optional[Dict[str, Any]] = None,
		generate_unique_id_function: Callable[[APIRoute], str] = Default(
			generate_unique_id
		),
	) -> Callable[[DecoratedCallable], DecoratedCallable]:
		return self.api_route(
			path=path,
			response_model=response_model,
			status_code=status_code,
			tags=tags,
			dependencies=dependencies,
			summary=summary,
			description=description,
			response_description=response_description,
			responses=responses,
			deprecated=deprecated,
			methods=['DELETE'],
			operation_id=operation_id,
			response_model_include=response_model_include,
			response_model_exclude=response_model_exclude,
			response_model_by_alias=response_model_by_alias,
			response_model_exclude_unset=response_model_exclude_unset,
			response_model_exclude_defaults=response_model_exclude_defaults,
			response_model_exclude_none=response_model_exclude_none,
			include_in_schema=include_in_schema,
			response_class=response_class,
			name=name,
			callbacks=callbacks,
			openapi_extra=openapi_extra,
			generate_unique_id_function=generate_unique_id_function,
		)

	def options(
		self: 'AvroRouter',
		path: str,
		*,
		response_model: Any = None,
		status_code: Optional[int] = None,
		tags: Optional[List[Union[str, Enum]]] = None,
		dependencies: Optional[Sequence[params.Depends]] = None,
		summary: Optional[str] = None,
		description: Optional[str] = None,
		response_description: str = 'Successful Response',
		responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
		deprecated: Optional[bool] = None,
		operation_id: Optional[str] = None,
		response_model_include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_by_alias: bool = True,
		response_model_exclude_unset: bool = False,
		response_model_exclude_defaults: bool = False,
		response_model_exclude_none: bool = False,
		include_in_schema: bool = True,
		response_class: Type[Response] = Default(AvroJsonResponse),
		name: Optional[str] = None,
		callbacks: Optional[List[BaseRoute]] = None,
		openapi_extra: Optional[Dict[str, Any]] = None,
		generate_unique_id_function: Callable[[APIRoute], str] = Default(
			generate_unique_id
		),
	) -> Callable[[DecoratedCallable], DecoratedCallable]:
		return self.api_route(
			path=path,
			response_model=response_model,
			status_code=status_code,
			tags=tags,
			dependencies=dependencies,
			summary=summary,
			description=description,
			response_description=response_description,
			responses=responses,
			deprecated=deprecated,
			methods=['OPTIONS'],
			operation_id=operation_id,
			response_model_include=response_model_include,
			response_model_exclude=response_model_exclude,
			response_model_by_alias=response_model_by_alias,
			response_model_exclude_unset=response_model_exclude_unset,
			response_model_exclude_defaults=response_model_exclude_defaults,
			response_model_exclude_none=response_model_exclude_none,
			include_in_schema=include_in_schema,
			response_class=response_class,
			name=name,
			callbacks=callbacks,
			openapi_extra=openapi_extra,
			generate_unique_id_function=generate_unique_id_function,
		)

	def head(
		self: 'AvroRouter',
		path: str,
		*,
		response_model: Any = None,
		status_code: Optional[int] = None,
		tags: Optional[List[Union[str, Enum]]] = None,
		dependencies: Optional[Sequence[params.Depends]] = None,
		summary: Optional[str] = None,
		description: Optional[str] = None,
		response_description: str = 'Successful Response',
		responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
		deprecated: Optional[bool] = None,
		operation_id: Optional[str] = None,
		response_model_include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_by_alias: bool = True,
		response_model_exclude_unset: bool = False,
		response_model_exclude_defaults: bool = False,
		response_model_exclude_none: bool = False,
		include_in_schema: bool = True,
		response_class: Type[Response] = Default(AvroJsonResponse),
		name: Optional[str] = None,
		callbacks: Optional[List[BaseRoute]] = None,
		openapi_extra: Optional[Dict[str, Any]] = None,
		generate_unique_id_function: Callable[[APIRoute], str] = Default(
			generate_unique_id
		),
	) -> Callable[[DecoratedCallable], DecoratedCallable]:
		return self.api_route(
			path=path,
			response_model=response_model,
			status_code=status_code,
			tags=tags,
			dependencies=dependencies,
			summary=summary,
			description=description,
			response_description=response_description,
			responses=responses,
			deprecated=deprecated,
			methods=['HEAD'],
			operation_id=operation_id,
			response_model_include=response_model_include,
			response_model_exclude=response_model_exclude,
			response_model_by_alias=response_model_by_alias,
			response_model_exclude_unset=response_model_exclude_unset,
			response_model_exclude_defaults=response_model_exclude_defaults,
			response_model_exclude_none=response_model_exclude_none,
			include_in_schema=include_in_schema,
			response_class=response_class,
			name=name,
			callbacks=callbacks,
			openapi_extra=openapi_extra,
			generate_unique_id_function=generate_unique_id_function,
		)

	def patch(
		self: 'AvroRouter',
		path: str,
		*,
		response_model: Any = None,
		status_code: Optional[int] = None,
		tags: Optional[List[Union[str, Enum]]] = None,
		dependencies: Optional[Sequence[params.Depends]] = None,
		summary: Optional[str] = None,
		description: Optional[str] = None,
		response_description: str = 'Successful Response',
		responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
		deprecated: Optional[bool] = None,
		operation_id: Optional[str] = None,
		response_model_include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_by_alias: bool = True,
		response_model_exclude_unset: bool = False,
		response_model_exclude_defaults: bool = False,
		response_model_exclude_none: bool = False,
		include_in_schema: bool = True,
		response_class: Type[Response] = Default(AvroJsonResponse),
		name: Optional[str] = None,
		callbacks: Optional[List[BaseRoute]] = None,
		openapi_extra: Optional[Dict[str, Any]] = None,
		generate_unique_id_function: Callable[[APIRoute], str] = Default(
			generate_unique_id
		),
	) -> Callable[[DecoratedCallable], DecoratedCallable]:
		return self.api_route(
			path=path,
			response_model=response_model,
			status_code=status_code,
			tags=tags,
			dependencies=dependencies,
			summary=summary,
			description=description,
			response_description=response_description,
			responses=responses,
			deprecated=deprecated,
			methods=['PATCH'],
			operation_id=operation_id,
			response_model_include=response_model_include,
			response_model_exclude=response_model_exclude,
			response_model_by_alias=response_model_by_alias,
			response_model_exclude_unset=response_model_exclude_unset,
			response_model_exclude_defaults=response_model_exclude_defaults,
			response_model_exclude_none=response_model_exclude_none,
			include_in_schema=include_in_schema,
			response_class=response_class,
			name=name,
			callbacks=callbacks,
			openapi_extra=openapi_extra,
			generate_unique_id_function=generate_unique_id_function,
		)

	def trace(
		self: 'AvroRouter',
		path: str,
		*,
		response_model: Any = None,
		status_code: Optional[int] = None,
		tags: Optional[List[Union[str, Enum]]] = None,
		dependencies: Optional[Sequence[params.Depends]] = None,
		summary: Optional[str] = None,
		description: Optional[str] = None,
		response_description: str = 'Successful Response',
		responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
		deprecated: Optional[bool] = None,
		operation_id: Optional[str] = None,
		response_model_include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
		response_model_by_alias: bool = True,
		response_model_exclude_unset: bool = False,
		response_model_exclude_defaults: bool = False,
		response_model_exclude_none: bool = False,
		include_in_schema: bool = True,
		response_class: Type[Response] = Default(AvroJsonResponse),
		name: Optional[str] = None,
		callbacks: Optional[List[BaseRoute]] = None,
		openapi_extra: Optional[Dict[str, Any]] = None,
		generate_unique_id_function: Callable[[APIRoute], str] = Default(
			generate_unique_id
		),
	) -> Callable[[DecoratedCallable], DecoratedCallable]:

		return self.api_route(
			path=path,
			response_model=response_model,
			status_code=status_code,
			tags=tags,
			dependencies=dependencies,
			summary=summary,
			description=description,
			response_description=response_description,
			responses=responses,
			deprecated=deprecated,
			methods=['TRACE'],
			operation_id=operation_id,
			response_model_include=response_model_include,
			response_model_exclude=response_model_exclude,
			response_model_by_alias=response_model_by_alias,
			response_model_exclude_unset=response_model_exclude_unset,
			response_model_exclude_defaults=response_model_exclude_defaults,
			response_model_exclude_none=response_model_exclude_none,
			include_in_schema=include_in_schema,
			response_class=response_class,
			name=name,
			callbacks=callbacks,
			openapi_extra=openapi_extra,
			generate_unique_id_function=generate_unique_id_function,
		)
