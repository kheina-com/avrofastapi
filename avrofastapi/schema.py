from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum, IntEnum
from re import Pattern
from re import compile as re_compile
from types import UnionType
from typing import Any, Callable, Dict, ForwardRef, List, Mapping, Optional, Self, Sequence, Set, Type, Union
from uuid import UUID

from avro.errors import AvroException
from pydantic import BaseModel, conint
from pydantic.types import ConstrainedBytes, ConstrainedDecimal


AvroInt: type[int] = conint(ge=-2147483648, le=2147483647)
_avro_name_format: Pattern = re_compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


class AvroFloat(float) :
	pass


class SerializableIntEnum(IntEnum) :

	@classmethod
	def __call__(cls, value: Union[str, int], *args: Any, **kwargs: Any) :
		if isinstance(value, str) :
			return cls[value]
		IntEnum.__call__(cls, value, *args, **kwargs)


AvroSchema = Union[str, Mapping[str, Union['AvroSchema', list[str], int]], list['AvroSchema']]


def convert_schema(model: Type[BaseModel], error: bool = False, conversions: dict[type, Union[Callable[['AvroSchemaGenerator', type], AvroSchema], AvroSchema]] = { }) -> AvroSchema :
	generator: AvroSchemaGenerator = AvroSchemaGenerator(model, error, conversions)
	return generator.schema()


def _validate_avro_name(name: str) -> str :
	if _avro_name_format.match(name) is None :
		raise AvroException(f'{name} does not match the avro name format: names must start with [A-Za-z_] and subsequently contain only [A-Za-z0-9_]')

	return name


def get_name(model: type | ForwardRef | str) -> str :
	if isinstance(model, str) :
		return model

	if isinstance(model, ForwardRef) :
		return model.__forward_arg__

	origin: Optional[type] = getattr(model, '__origin__', None)  # for types from typing library
	name: Optional[str] = None

	if origin or (model.__class__ and model.__class__.__module__ == 'types') :
		name = str(origin)
		if name.startswith('typing.') :
			name = name[7:]

		if name.startswith('types.') :
			name = name[6:]

		name += '_' + '_'.join(list(map(get_name, model.__args__)))

	elif issubclass(model, ConstrainedBytes) and model.__name__ == 'ConstrainedBytesValue' :
		name = 'Bytes_' + str(model.max_length)

	elif issubclass(model, ConstrainedDecimal) and model.__name__ == 'ConstrainedDecimalValue' :
		name = f'Decimal_{model.max_digits}_{model.decimal_places}'

	else :
		name = model.__name__

	return name


def _validate_avro_namespace(namespace: str, parent_namespace: Optional[str] = None) :
	if not all(map(_avro_name_format.match, namespace.split('.'))) :
		raise AvroException(f'{namespace} does not match the avro namespace format: A namespace is a dot-separated sequence of names. names must start with [A-Za-z_] and subsequently contain only [A-Za-z0-9_]')

	# break these into two different ifs for readability
	if parent_namespace and namespace != parent_namespace :
		if (
			len(parent_namespace) >= len(namespace) or
			not namespace.startswith(parent_namespace) or
			namespace[len(parent_namespace)] != '.'
		) :
			raise AvroException(f'the enclosing namespace, {parent_namespace}, must be a subpath of the namespace: {namespace}')


class AvroSchemaGenerator :

	def __init__(self, model: Type[BaseModel], error: bool = False, conversions: dict[type, Union[Callable[[Self, type], AvroSchema], AvroSchema]] = { }) -> None :
		"""
		:param model: the pydantic model to generate a schema for
		:param error: whether or not the model is an error
		:param conversions: additional type conversion functions. it's a good idea to make sure the avro type is convertable to the python type through pydantic for encoding/decoding.
		"""
		self.model: Type[BaseModel] = model
		self.name: str = get_name(model)
		_validate_avro_name(self.name)
		self.namespace: str = getattr(model, '__namespace__', self.name)
		_validate_avro_namespace(self.namespace)
		self.error: bool = error or self.name.lower().endswith('error')
		self.refs: Set[str] = set()
		self._conversions: dict[type, Union[Callable[[Self, type], AvroSchema], AvroSchema]] = {
			**AvroSchemaGenerator._conversions_,
			**conversions,
		}

	def schema(self: Self) -> AvroSchema :
		self.refs = set()
		schema: AvroSchema = self._get_type(self.model)

		if isinstance(schema, dict) :
			schema['namespace'] = self.namespace
			schema['name'] = self.name

			if schema.get('type') == 'record' and self.error:
				schema['type'] = 'error'

		return schema

	def _convert_array(self: Self, model: Type[Sequence[Any]]) -> AvroSchema :
		object_type: AvroSchema = self._get_type(model.__args__[0]) # type: ignore

		# TODO: does this do anything?
		if (
			isinstance(object_type, dict)
			and isinstance(object_type.get('type'), dict)
			and object_type['type'].get('logicalType') is not None # type: ignore
		) :
			object_type = object_type['type'] # type: ignore

		return {
			'type': 'array',
			'items': object_type,
		}


	def _convert_object(self: Self, model: Type[BaseModel]) -> AvroSchema :
		sub_namespace: Optional[str] = getattr(model, '__namespace__', None)
		parent_namespace: str = self.namespace
		if sub_namespace :
			_validate_avro_namespace(sub_namespace, self.namespace)
			self.namespace = sub_namespace

		fields: List[AvroSchema] = []

		for name, field in model.__fields__.items() :
			f: AvroSchema = { }
			f['name'] = _validate_avro_name(name)
			submodel = model.__annotations__[name]

			if getattr(submodel, '__origin__', None) is Union and len(submodel.__args__) == 2 and type(None) in submodel.__args__ and field.default is None :
				# this is a special case where the field is nullable and the default value is null, but the actual value can be omitted from the schema
				# we rearrange Optional[Type] and Union[Type, None] to Union[None, Type] so that null becomes the default type and the 'default' key is unnecessary
				type_index: int = 0 if submodel.__args__.index(type(None)) else 1
				f['type'] = self._get_type(Union[None, submodel.__args__[type_index]]) # type: ignore

			else :
				f['type'] = self._get_type(submodel)

				if not field.required :
					# TODO: does this value need to be avro-encoded?
					f['default'] = field.default

			fields.append(f)

		name: str = get_name(model)
		_validate_avro_name(name)

		schema: Dict[str, Union[str, List[AvroSchema]]] = {
			'type': 'record',
			'name': name,
			'fields': fields,
		}

		if sub_namespace :
			# we can omit namespace if it's null or an empty string
			schema['namespace'] = sub_namespace
			# reset namespace to the parent's
			self.namespace = parent_namespace

		return schema


	def _convert_union(self: Self, model: Type[Union[Any, Any]]) -> List[AvroSchema] :
		return list(map(self._get_type, model.__args__))


	def _convert_enum(self: Self, model: Type[Enum]) -> AvroSchema :
		name: str = get_name(model)
		_validate_avro_name(name)

		values: Optional[List[str]] = list(map(lambda x : x.value, model.__members__.values()))

		if len(values) != len(set(values)) :
			raise AvroException('enums must contain all unique values to be avro encoded')

		schema: Dict[str, Union[str, List[str]]] = {
			'type': 'enum',
			'name': name,
			'symbols': values,
		}

		self_namespace: Optional[str] = getattr(model, '__namespace__', None)
		if self_namespace :
			_validate_avro_namespace(self_namespace, self.namespace)
			schema['namespace'] = self_namespace

		return schema


	def _convert_int_enum(self: Self, model: Type[IntEnum]) -> AvroSchema :
		name: str = get_name(model) 
		_validate_avro_name(name)

		values: Optional[list[str]] = list(model.__members__.keys())

		if len(values) != len(set(model.__members__.values())) :
			raise AvroException('enums must contain all unique values to be avro encoded')

		schema: Dict[str, Union[str, List[str]]] = {
			'type': 'enum',
			'name': name,
			'symbols': values,
		}

		self_namespace: Optional[str] = getattr(model, '__namespace__', None)
		if self_namespace :
			_validate_avro_namespace(self_namespace, self.namespace)
			schema['namespace'] = self_namespace

		return schema


	def _convert_bytes(self: Self, model: Type[ConstrainedBytes]) -> AvroSchema :
		if model.min_length == model.max_length and model.max_length :
			schema: Dict[str, Union[str, int]] = {
				'type': 'fixed',
				'name': _validate_avro_name(get_name(model)),
				'size': model.max_length,
			}

			self_namespace: Optional[str] = getattr(model, '__namespace__', None)
			if self_namespace :
				_validate_avro_namespace(self_namespace, self.namespace)
				schema['namespace'] = self_namespace

			return schema

		return 'bytes'


	def _convert_map(self: Self, model: Type[dict[str, Any]]) -> AvroSchema :
		if not hasattr(model, '__args__') :
			raise AvroException('typing.Dict must be used to determine key/value type, not dict')

		if model.__args__[0] is not str : # type: ignore
			raise AvroException('maps must have string keys')

		return {
			'type': 'map',
			'values': self._get_type(model.__args__[1]), # type: ignore
		}


	def _convert_decimal(self: Self, _: Type[Decimal]) -> AvroSchema :
		raise AvroException('Support for unconstrained decimals is not possible due to the nature of avro decimals. please use pydantic.condecimal(max_digits=int, decimal_places=int)')


	def _convert_condecimal(self: Self, model: Type[ConstrainedDecimal]) -> AvroSchema :
		if not model.max_digits or model.decimal_places is None :
			raise AvroException('Decimal attributes max_digits and decimal_places must be provided in order to map to avro decimals')

		return {
			'type': 'bytes',
			'logicalType': 'decimal',
			'precision': model.max_digits,
			'scale': model.decimal_places,
		}

	_conversions_: dict[type, Union[Callable[[Self, type], AvroSchema], AvroSchema]] = {
		BaseModel: _convert_object,
		list: _convert_array,
		Enum: _convert_enum,
		IntEnum: _convert_int_enum,
		ConstrainedBytes: _convert_bytes,
		Dict: _convert_map,
		dict: _convert_map,
		Decimal: _convert_decimal,
		ConstrainedDecimal: _convert_condecimal,
		bool: 'boolean',
		AvroInt: 'int',
		int: 'long',
		AvroFloat: 'float',
		float: 'double',
		bytes: 'bytes',
		type(None): 'null',
		str: 'string',
		datetime: {
			'type': 'long',
			'logicalType': 'timestamp-micros',
		},
		date: {
			'type': 'int',
			'logicalType': 'date',
		},
		time: {
			'type': 'long',
			'logicalType': 'time-micros',
		},
		UUID: {
			'type': 'string',
			'logicalType': 'uuid',
		},
	}

	def _get_type(self: Self, model: type | ForwardRef | str) -> AvroSchema :
		if isinstance(model, str) :
			if model in self.refs :
				return model

			raise AvroException('received forward ref within model that is not constructed within the model')

		if isinstance(model, ForwardRef) :
			if model.__forward_arg__ in self.refs :
				return model.__forward_arg__

			raise AvroException('received forward ref within model that is not constructed within the model')

		name: str = get_name(model)

		if name in self.refs :
			return name

		origin: Optional[Type] = getattr(model, '__origin__', None)

		if origin and origin in self._conversions :
			c = self._conversions[origin]
			if callable(c) :
				if not name.lower().startswith('union') :
					self.refs.add(name)
				schema: AvroSchema = c(self, model)
				return schema
			return c

		clss: Optional[Type] = getattr(model, '__class__', None)

		if clss and clss in self._conversions :
			c = self._conversions[clss]
			if callable(c) :
				if not name.lower().startswith('union') :
					self.refs.add(name)
				schema: AvroSchema = c(self, model)
				return schema
			return c

		for cls in getattr(model, '__mro__', []) :
			if cls in self._conversions :
				c = self._conversions[cls]
				if callable(c) :
					if not name.lower().startswith('union') :
						self.refs.add(name)
					schema: AvroSchema = c(self, model)
					return schema
				return c

		raise NotImplementedError(f'{model} missing from conversion map.')


# I didn't want to ignore the whole definition above, so assign it down here
AvroSchemaGenerator._conversions_[Union] = AvroSchemaGenerator._convert_union # type: ignore
AvroSchemaGenerator._conversions_[UnionType] = AvroSchemaGenerator._convert_union # type: ignore
