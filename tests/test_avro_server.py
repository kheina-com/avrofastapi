import json
from hashlib import md5
from typing import Union

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel, conint

from avrofastapi import avrofastapi
from avrofastapi.handshake import AvroMessage, AvroProtocol, CallRequest, CallResponse, HandshakeMatch, HandshakeRequest, HandshakeResponse
from avrofastapi.models import Error, ValidationError
from avrofastapi.schema import convert_schema
from avrofastapi.serialization import AvroDeserializer, AvroSerializer, avro_frame, read_avro_frames


endpoint = '/'
base_url = 'dev.kheina.com'
schema = 'https://'


class RequestModel(BaseModel) :
	A: str
	B: int
	C: float


class ResponseModel(BaseModel) :
	result: bool


model_serializer: AvroSerializer = AvroSerializer(RequestModel)
model_deserializer: AvroDeserializer = AvroDeserializer(ResponseModel)

call_serializer: AvroSerializer = AvroSerializer(CallRequest)
call_deserializer: AvroDeserializer = AvroDeserializer(CallResponse)

handshake_serializer: AvroSerializer = AvroSerializer(HandshakeRequest)
handshake_deserializer: AvroDeserializer = AvroDeserializer(HandshakeResponse)


def get_second_frame(response: bytes) :
	frames = read_avro_frames(response)
	next(frames)
	return next(frames)


def format_request(handshake: HandshakeRequest = None, request: RequestModel = None, message: str = None) :
	req = b''

	if handshake :
		req += avro_frame(handshake_serializer(handshake))

	if request and message :
		req += avro_frame(call_serializer(CallRequest(
			message=message,
			request=model_serializer(request),
		)))

	return req


@pytest.mark.asyncio
class TestAvroServer :

	def test_AvroRoute_GetNoHeaders_ReturnsJson(self) :

		# arrange
		app = avrofastapi()

		@app.post(endpoint, response_model=ResponseModel)
		async def test_func() :
			return ResponseModel(
				result=True,
			)

		client = TestClient(app, base_url=f'{schema}{base_url}')

		# act
		response = client.post(schema + base_url + endpoint)

		# assert
		assert 200 == response.status_code
		assert { 'result': True } == response.json()


	@pytest.mark.parametrize(
		"payload",
		[
			None,
			avro_frame(handshake_serializer(HandshakeRequest(clientHash=b'deadbeefdeadbeef', serverHash=b'deadbeefdeadbeef'))),
			b'abc',  # valid body, but unable to be deserialized
		],
	)
	def test_AvroRoute_AllAvroHeadersInvalidHandshake_ReturnsAvroHandshake(self, payload: bytes) :
		# arrange
		app = avrofastapi()

		@app.post(endpoint, response_model=ResponseModel)
		async def test_func() :
			return ResponseModel(
				result=True,
			)

		client = TestClient(app, base_url=f'{schema}{base_url}')

		# act
		response = client.post(
			schema + base_url + endpoint,
			headers={ 'accept': 'avro/binary', 'content-type': 'avro/binary' },
			content=payload,
		)

		# assert
		frame = read_avro_frames(response._content)
		assert 200 == response.status_code
		handshake: HandshakeResponse = handshake_deserializer(next(frame))
		assert HandshakeMatch.none == handshake.match
		assert handshake.serverHash == md5(handshake.serverProtocol.encode()).digest()
		server_protocol = json.loads(handshake.serverProtocol)
		assert { Error.__name__, ValidationError.__name__ } == set(server_protocol['messages']['test_func__post'].pop('errors'))
		assert server_protocol == {
			'namespace': 'kh-common',
			'protocol': '/',
			'messages': {
				'test_func__post': {
					'doc': 'the openapi description should go here. ex: V1Endpoint',
					'request': [],
					'response': ResponseModel.__name__,
					'oneWay': False,
					'types': [
						convert_schema(Error, error=True),
						convert_schema(ValidationError, error=True),
						convert_schema(ResponseModel),
					],
				},
			},
		}
		assert call_deserializer(next(frame)).error


	def test_AvroRoute_AllAvroHeadersValidHandshakeNoBody_ReturnsHandshakeAndResponse(self) :
		# arrange
		protocol = AvroProtocol(
			namespace='idk',
			protocol='idk',
			messages={
				'test_func__post': AvroMessage(
					types=[convert_schema(ResponseModel)],
					response=ResponseModel.__name__,
				),
			}
		).json()

		handshake = HandshakeRequest(
			clientHash=md5(protocol.encode()).digest(),
			serverHash=b'deadbeefdeadbeef',
			clientProtocol=protocol,
		)

		app = avrofastapi()

		@app.post(endpoint, response_model=ResponseModel)
		async def test_func() :
			return ResponseModel(
				result=True,
			)

		client = TestClient(app, base_url=f'{schema}{base_url}')

		# act
		response = client.post(
			schema + base_url + endpoint,
			headers={ 'accept': 'avro/binary', 'content-type': 'avro/binary' },
			content=format_request(handshake=handshake),
		)

		# assert
		frame = read_avro_frames(response._content)
		assert 200 == response.status_code
		handshake: HandshakeResponse = handshake_deserializer(next(frame))
		assert HandshakeMatch.client == handshake.match
		assert handshake.serverHash == md5(handshake.serverProtocol.encode()).digest()
		server_protocol = json.loads(handshake.serverProtocol)
		assert { Error.__name__, ValidationError.__name__ } == set(server_protocol['messages']['test_func__post'].pop('errors'))
		assert server_protocol == {
			'namespace': 'kh-common',
			'protocol': '/',
			'messages': {
				'test_func__post': {
					'doc': 'the openapi description should go here. ex: V1Endpoint',
					'request': [],
					'response': ResponseModel.__name__,
					'oneWay': False,
					'types': [
						convert_schema(Error, error=True),
						convert_schema(ValidationError, error=True),
						convert_schema(ResponseModel),
					],
				},
			},
		}
		assert ResponseModel(result=True) == model_deserializer(call_deserializer(next(frame)).response)


	def test_AvroRoute_AllAvroHeadersValidHandshakeHandshakeCached_ReturnsHandshakeAndResponse(self) :
		# arrange
		protocol = AvroProtocol(
			namespace='idk',
			protocol='idk',
			messages={
				'test_func__post': AvroMessage(
					types=[convert_schema(ResponseModel)],
					response=ResponseModel.__name__,
				),
			}
		).json()

		handshake = HandshakeRequest(
			clientHash=md5(protocol.encode()).digest(),
			serverHash=b'deadbeefdeadbeef',
			clientProtocol=protocol,
		)

		app = avrofastapi()

		@app.post(endpoint, response_model=ResponseModel)
		async def test_func() :
			return ResponseModel(
				result=True,
			)

		client = TestClient(app, base_url=f'{schema}{base_url}')
		response = client.post(
			schema + base_url + endpoint,
			headers={ 'accept': 'avro/binary', 'content-type': 'avro/binary' },
			content=format_request(handshake=handshake),
		)

		handshake = handshake_deserializer(next(read_avro_frames(response._content)))
		assert HandshakeMatch.client == handshake.match

		handshake = HandshakeRequest(
			clientHash=md5(protocol.encode()).digest(),
			serverHash=handshake.serverHash,
		)

		# act
		response = client.post(
			schema + base_url + endpoint,
			headers={ 'accept': 'avro/binary', 'content-type': 'avro/binary' },
			content=format_request(handshake=handshake),
		)

		# assert
		frame = read_avro_frames(response._content)
		assert 200 == response.status_code
		handshake: HandshakeResponse = handshake_deserializer(next(frame))
		assert HandshakeMatch.both == handshake.match
		assert None == handshake.serverHash
		assert None == handshake.serverProtocol
		assert ResponseModel(result=True) == model_deserializer(call_deserializer(next(frame)).response)


	def test_AvroRoute_AllAvroHeadersNullResponse_ReturnsHandshakeAndResponse(self) :
		# arrange
		protocol = AvroProtocol(
			namespace='idk',
			protocol='idk',
			messages={
				'test_func__post': AvroMessage(),
			}
		).json()

		handshake = HandshakeRequest(
			clientHash=md5(protocol.encode()).digest(),
			serverHash=b'deadbeefdeadbeef',
			clientProtocol=protocol,
		)

		app = avrofastapi()

		@app.post(endpoint, status_code=204)
		async def test_func() :
			assert True
			return

		client = TestClient(app, base_url=f'{schema}{base_url}')

		# act
		response = client.post(
			schema + base_url + endpoint,
			headers={ 'accept': 'avro/binary', 'content-type': 'avro/binary' },
			content=format_request(handshake=handshake),
		)

		# assert
		frame = read_avro_frames(response._content)
		assert 200 == response.status_code
		handshake: HandshakeResponse = handshake_deserializer(next(frame))
		assert HandshakeMatch.client == handshake.match
		assert handshake.serverHash == md5(handshake.serverProtocol.encode()).digest()
		server_protocol = json.loads(handshake.serverProtocol)
		assert { Error.__name__, ValidationError.__name__ } == set(server_protocol['messages']['test_func__post'].pop('errors'))
		assert server_protocol == {
			'namespace': 'kh-common',
			'protocol': '/',
			'messages': {
				'test_func__post': {
					'doc': 'the openapi description should go here. ex: V1Endpoint',
					'request': [],
					'response': 'null',
					'oneWay': True,
					'types': [
						convert_schema(Error, error=True),
						convert_schema(ValidationError, error=True),
					],
				},
			},
		}
		assert not next(frame)


	def test_AvroRoute_AllAvroHeadersCachedNullResponse_ReturnsHandshakeAndResponse(self) :
		# arrange
		protocol = AvroProtocol(
			namespace='idk',
			protocol='idk',
			messages={
				'test_func__post': AvroMessage(),
			}
		).json()

		handshake = HandshakeRequest(
			clientHash=md5(protocol.encode()).digest(),
			serverHash=b'deadbeefdeadbeef',
			clientProtocol=protocol,
		)

		app = avrofastapi()

		@app.post(endpoint, status_code=204)
		async def test_func() :
			assert True
			return

		client = TestClient(app, base_url=f'{schema}{base_url}')
		response = client.post(
			schema + base_url + endpoint,
			headers={ 'accept': 'avro/binary', 'content-type': 'avro/binary' },
			content=format_request(handshake=handshake),
		)

		handshake = handshake_deserializer(next(read_avro_frames(response._content)))
		assert HandshakeMatch.client == handshake.match

		handshake = HandshakeRequest(
			clientHash=md5(protocol.encode()).digest(),
			serverHash=handshake.serverHash, 
		)

		# act
		response = client.post(
			schema + base_url + endpoint,
			headers={ 'accept': 'avro/binary', 'content-type': 'avro/binary' },
			content=format_request(handshake=handshake),
		)

		# assert
		frame = read_avro_frames(response._content)
		assert 200 == response.status_code
		handshake: HandshakeResponse = handshake_deserializer(next(frame))
		assert HandshakeMatch.both == handshake.match
		assert None == handshake.serverHash
		assert None == handshake.serverProtocol
		assert not next(frame)


	def test_AvroRoute_AllAvroHeadersInvalidRequest_ReturnsHandshakeAndError(self) :
		# arrange
		protocol = AvroProtocol(
			namespace='idk',
			protocol='idk',
			messages={
				'test_func__post': AvroMessage(
					request=convert_schema(RequestModel)['fields'],
				),
			}
		).json()

		handshake = HandshakeRequest(
			clientHash=md5(protocol.encode()).digest(),
			serverHash=b'deadbeefdeadbeef',
			clientProtocol=protocol,
		)

		app = avrofastapi()

		class TestModel(BaseModel) :
			A: str
			B: conint(gt=0)
			C: float

		@app.post(endpoint, status_code=204)
		async def test_func(body: TestModel) :
			return

		client = TestClient(app, base_url=f'{schema}{base_url}')

		# act
		response = client.post(
			schema + base_url + endpoint,
			headers={ 'accept': 'avro/binary', 'content-type': 'avro/binary' },
			content=format_request(handshake=handshake, request=RequestModel(A='1', B=-2, C=3.1), message='test_func__post'),
		)

		# assert
		frame = read_avro_frames(response._content)
		assert 200 == response.status_code
		handshake: HandshakeResponse = handshake_deserializer(next(frame))
		assert HandshakeMatch.client == handshake.match
		call = call_deserializer(next(frame))
		assert call.error
		error = AvroDeserializer(Union[Error, ValidationError, str])(call.response)
		assert ValidationError == type(error)
