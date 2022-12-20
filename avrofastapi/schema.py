try :
	from re import _pattern_type as Pattern
	from re import compile as re_compile
except ImportError :
	from re import Pattern, compile as re_compile

from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Type, Union
from uuid import UUID

from avro.errors import AvroException
from pydantic import BaseModel, ConstrainedBytes, ConstrainedDecimal, ConstrainedInt, conint


AvroInt: ConstrainedInt = conint(ge=-2147483648, le=2147483647)
_avro_name_format: Pattern = re_compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


class AvroFloat(float) :
	pass


AvroSchema: Type = Union[str, Dict[str, Union['AvroSchema', int]], List['AvroSchema']]


def convert_schema(model: Type[BaseModel], error: bool = False) -> AvroSchema :
	generator: AvroSchemaGenerator = AvroSchemaGenerator(model, error)
	return generator.schema()


def _validate_avro_name(name: str) :
	if _avro_name_format.match(name) is None :
		raise AvroException(f'{name} does not match the avro name format: names must start with [A-Za-z_] and subsequently contain only [A-Za-z0-9_]')


def get_name(model: Type) -> str :
	origin: Optional[Type] = getattr(model, '__origin__', None)  # for types from typing library
	name: str = None

	if origin :
		name = str(origin)
		if name.startswith('typing.') :
			name = name[7:]

		name += '_' + '_'.join(list(map(get_name, model.__args__)))

	elif issubclass(model, ConstrainedBytes) and model.__name__ == 'ConstrainedBytesValue' :
		name = 'Bytes_' + str(model.max_length)

	elif issubclass(model, ConstrainedDecimal) and model.__name__ == 'ConstrainedDecimalValue' :
		name = f'Decimal_{model.max_digits}_{model.decimal_places}'

	else :
		name = model.__name__

	return name


def _validate_avro_namespace(namespace: str, parent_namespace: str = None) :
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

	def __init__(self, model: Type[BaseModel], error: bool = False) -> None :
		self.model: Type[BaseModel] = model
		self.name: str = get_name(model)
		_validate_avro_name(self.name)
		self.namespace: str = getattr(model, '__namespace__', self.name)
		_validate_avro_namespace(self.namespace)
		self.error: bool = error or self.name.lower().endswith('error')
		self.refs: Optional[Set[str]] = None


	def schema(self: 'AvroSchemaGenerator') -> AvroSchema :
		self.refs = set()
		schema: AvroSchema = self._get_type(self.model)

		if isinstance(schema, dict) :
			schema['namespace'] = self.namespace
			schema['name'] = self.name

			if schema.get('type') == 'record' and self.error :
				schema['type'] = 'error'

		return schema


	def _convert_array(self: 'AvroSchemaGenerator', model: Type[Iterable[Any]]) -> Dict[str, AvroSchema] :
		object_type: AvroSchema = self._get_type(model.__args__[0])

		# TODO: does this do anything?
		if (
			isinstance(object_type, dict)
			and isinstance(object_type.get('type'), dict)
			and object_type['type'].get('logicalType') is not None
		):
			object_type = object_type['type']

		return {
			'type': 'array',
			'items': object_type,
		}


	def _convert_object(self: 'AvroSchemaGenerator', model: Type[BaseModel]) -> Dict[str, Union[str, List[AvroSchema]]] :
		sub_namespace: Optional[str] = getattr(model, '__namespace__', None)
		parent_namespace: str = self.namespace
		if sub_namespace :
			_validate_avro_namespace(sub_namespace, self.namespace)
			self.namespace = sub_namespace

		fields: List[AvroSchema] = []

		for name, field in model.__fields__.items() :
			_validate_avro_name(name)
			submodel = model.__annotations__[name]
			f: AvroSchema = { 'name': name }

			if getattr(submodel, '__origin__', None) is Union and len(submodel.__args__) == 2 and type(None) in submodel.__args__ and field.default is None :
				# this is a special case where the field is nullable and the default value is null, but the actual value can be omitted from the schema
				# we rearrange Optional[Type] and Union[Type, None] to Union[None, Type] so that null becomes the default type and the 'default' key is unnecessary
				type_index: int = 0 if submodel.__args__.index(type(None)) else 1
				f['type'] = self._get_type(Union[None, submodel.__args__[type_index]])

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


	def _convert_union(self: 'AvroSchemaGenerator', model: Type[Union[Any, Any]]) -> List[AvroSchema] :
		return list(map(self._get_type, model.__args__))


	def _convert_enum(self: 'AvroSchemaGenerator', model: Type[Enum]) -> Dict[str, Union[str, List[str]]] :
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


	def _convert_bytes(self: 'AvroSchemaGenerator', model: Type[ConstrainedBytes]) -> Dict[str, Union[str, int]] :
		if model.min_length == model.max_length and model.max_length :
			schema: Dict[str, Union[str, int]] = {
				'type': 'fixed',
				'name': get_name(model),
				'size': model.max_length,
			}
			_validate_avro_name(schema['name'])

			self_namespace: Optional[str] = getattr(model, '__namespace__', None)
			if self_namespace :
				_validate_avro_namespace(self_namespace, self.namespace)
				schema['namespace'] = self_namespace

			return schema

		return 'bytes'


	def _convert_map(self: 'AvroSchemaGenerator', model: Type[Dict[str, Any]]) -> Dict[str, AvroSchema] :
		if not hasattr(model, '__args__') :
			raise AvroException('typing.Dict must be used to determine key/value type, not dict')

		if model.__args__[0] != str :
			raise AvroException('maps must have string keys')

		return {
			'type': 'map',
			'values': self._get_type(model.__args__[1]),
		}


	def _convert_decimal(self: 'AvroSchemaGenerator', model: Type[Decimal]) -> None :
		raise AvroException('Support for unconstrained decimals is not possible due to the nature of avro decimals. please use pydantic.condecimal(max_digits=int, decimal_places=int)')


	def _convert_condecimal(self: 'AvroSchemaGenerator', model: Type[ConstrainedDecimal]) -> Dict[str, Union[str, int]] :
		if not model.max_digits or model.decimal_places is None :
			raise AvroException('Decimal attributes max_digits and decimal_places must be provided in order to map to avro decimals')

		return {
			'type': 'bytes',
			'logicalType': 'decimal',
			'precision': model.max_digits,
			'scale': model.decimal_places,
		}


	_conversions_ = {
		BaseModel: _convert_object,
		Union: _convert_union,
		list: _convert_array,
		Enum: _convert_enum,
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


	def _get_type(self: 'AvroSchemaGenerator', model: Type[BaseModel]) -> AvroSchema :
		name: str = get_name(model)

		if name in self.refs :
			return name

		origin: Optional[Type] = getattr(model, '__origin__', None)

		if origin in self._conversions_ :
			# none of these can be converted without funcs
			schema: AvroSchema = self._conversions_[origin](self, model)
			if isinstance(schema, dict) and 'name' in schema :
				self.refs.add(schema['name'])
			return schema

		for cls in model.__mro__ :
			if cls in self._conversions_ :
				if isinstance(self._conversions_[cls], Callable) :
					schema: AvroSchema = self._conversions_[cls](self, model)
					if 'name' in schema :
						self.refs.add(schema['name'])
					return schema
				return self._conversions_[cls]

		raise NotImplementedError(f'{model} missing from conversion map.')
