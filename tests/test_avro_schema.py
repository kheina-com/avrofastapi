from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Type, Union

import pytest
from avro.errors import AvroException
from pydantic import BaseModel, conbytes, condecimal
from pytest import raises

from avrofastapi.models import Error, ValidationError
from avrofastapi.schema import AvroFloat, AvroInt, convert_schema


class BasicModelBaseTypes(BaseModel) :
	A: str
	B: int
	C: float
	D: bytes
	E: bool


class BasicEnum(Enum) :
	__use_enum_names__: bool = False
	test1: str = 'TEST1'
	test2: str = 'TEST2'
	test3: str = 'TEST3'


class BasicModelAdvancedTypes(BaseModel) :
	A: datetime
	B: conbytes(max_length=10, min_length=10)
	C: condecimal(max_digits=5, decimal_places=3)
	D: BasicEnum
	E: date
	F: time


class NestedModelBasicTypes(BaseModel) :
	A: BasicModelBaseTypes
	B: int


class BasicModelTypingTypes(BaseModel) :
	A: List[int]
	B: Dict[str, int]
	C: Optional[int]
	D: Union[int, str]


class BasicModelCustomNamespace(BaseModel) :
	__namespace__: str = 'custom.namespace'
	A: int


class BasicModelCustomNestedNamespace(BaseModel) :
	__namespace__: str = 'custom'
	A: BasicModelCustomNamespace


class BasicEnumUsesNames(Enum) :
	__use_enum_names__: bool = True
	test1: str = 'TEST1'
	test2: str = 'TEST2'
	test3: str = 'TEST3'


class BasicModelDefaultValues(BaseModel) :
	A: str = '1'
	B: int = 2
	C: float = 3.1
	D: bytes = b'abc'
	E: bool = True


class BasicModelCustomTypes(BaseModel) :
	A: AvroInt
	B: AvroFloat


@pytest.mark.parametrize(
	'input_model, expected', [
		(BasicModelBaseTypes, { 'namespace': 'BasicModelBaseTypes', 'name': 'BasicModelBaseTypes', 'type': 'record', 'fields': [{ 'name': 'A', 'type': 'string' }, { 'name': 'B', 'type': 'long' }, { 'name': 'C', 'type': 'double' }, { 'name': 'D', 'type': 'bytes' }, { 'name': 'E', 'type': 'boolean' }] }),
		(BasicModelAdvancedTypes, { 'namespace': 'BasicModelAdvancedTypes', 'name': 'BasicModelAdvancedTypes', 'type': 'record', 'fields': [{ 'name': 'A', 'type': { 'type': 'long', 'logicalType': 'timestamp-micros' } }, { 'name': 'B', 'type': { 'name': 'Bytes_10', 'type': 'fixed', 'size': 10 } }, { 'name': 'C', 'type': { 'type': 'bytes', 'logicalType': 'decimal', 'precision': 5, 'scale': 3 } }, { 'name': 'D', 'type': { 'name': 'BasicEnum', 'type': 'enum', 'symbols': ['TEST1', 'TEST2', 'TEST3'] } }, { 'name': 'E', 'type': { 'type': 'int', 'logicalType': 'date' } }, { 'name': 'F', 'type': { 'type': 'long', 'logicalType': 'time-micros' } }] }),
		(NestedModelBasicTypes, { 'namespace': 'NestedModelBasicTypes', 'name': 'NestedModelBasicTypes', 'type': 'record', 'fields': [{ 'name': 'A', 'type': { 'name': 'BasicModelBaseTypes', 'type': 'record', 'fields': [{ 'name': 'A', 'type': 'string' }, { 'name': 'B', 'type': 'long' }, { 'name': 'C', 'type': 'double' }, { 'name': 'D', 'type': 'bytes' }, { 'name': 'E', 'type': 'boolean' }] } }, { 'name': 'B', 'type': 'long' }] }),
		(BasicModelTypingTypes, { 'namespace': 'BasicModelTypingTypes', 'name': 'BasicModelTypingTypes', 'type': 'record', 'fields': [{ 'name': 'A', 'type': { 'type': 'array', 'items': 'long' } }, { 'name': 'B', 'type': { 'type': 'map', 'values': 'long' } }, { 'name': 'C', 'type': ['null', 'long'] }, { 'name': 'D', 'type': ['long', 'string'] }] }),
		(BasicModelCustomNamespace, { 'namespace': 'custom.namespace', 'name': 'BasicModelCustomNamespace', 'type': 'record', 'fields': [{ 'name': 'A', 'type': 'long' }] }),
		(BasicModelCustomNestedNamespace, { 'namespace': 'custom', 'name': 'BasicModelCustomNestedNamespace', 'type': 'record', 'fields': [{ 'name': 'A', 'type': { 'namespace': 'custom.namespace', 'name': 'BasicModelCustomNamespace', 'type': 'record', 'fields': [{ 'name': 'A', 'type': 'long' }] } }] }),
		(Error, { 'namespace': 'Error', 'name': 'Error', 'type': 'error', 'fields': [{ 'name': 'refid', 'type': ['null', { 'type': 'fixed', 'name': 'RefId', 'size': 16 }] }, { 'name': 'status', 'type': 'long' }, { 'name': 'error', 'type': 'string' }] }),
		(ValidationError, { 'namespace': 'ValidationError', 'name': 'ValidationError', 'type': 'error', 'fields': [{ 'name': 'detail', 'type': { 'items': { 'fields': [{ 'name': 'loc', 'type': { 'items': 'string', 'type': 'array' } }, { 'name': 'msg', 'type': 'string' }, { 'name': 'type', 'type': 'string' }], 'name': 'ValidationErrorDetail', 'type': 'record' }, 'type': 'array' } }] }),
		(BasicModelDefaultValues, { 'namespace': 'BasicModelDefaultValues', 'name': 'BasicModelDefaultValues', 'type': 'record', 'fields': [{ 'name': 'A', 'type': 'string', 'default': '1' }, { 'name': 'B', 'type': 'long', 'default': 2 }, { 'name': 'C', 'type': 'double', 'default': 3.1 }, { 'name': 'D', 'type': 'bytes', 'default': b'abc' }, { 'name': 'E', 'type': 'boolean', 'default': True }] }),
		(BasicModelCustomTypes, { 'namespace': 'BasicModelCustomTypes', 'name': 'BasicModelCustomTypes', 'type': 'record', 'fields': [{ 'name': 'A', 'type': 'int' }, { 'name': 'B', 'type': 'float' }] }),
		# (BasicEnumUsesNames, { 'namespace': 'BasicEnumUsesNames', 'name': 'BasicEnumUsesNames', 'type': 'enum', 'symbols': ['test1', 'test2', 'test3'] }),
	],
)
def test_ConvertSchema_ValidInputError_ModelConvertedSuccessfully(input_model: Type[BaseModel], expected: dict) :

	# act
	schema: dict = convert_schema(input_model)

	# assert
	assert expected == schema


@pytest.mark.parametrize(
	'input_model, expected', [
		(BasicModelBaseTypes, { 'namespace': 'BasicModelBaseTypes', 'name': 'BasicModelBaseTypes', 'type': 'error', 'fields': [{ 'name': 'A', 'type': 'string' }, { 'name': 'B', 'type': 'long' }, { 'name': 'C', 'type': 'double' }, { 'name': 'D', 'type': 'bytes' }, { 'name': 'E', 'type': 'boolean' }] }),
		(BasicModelAdvancedTypes, { 'namespace': 'BasicModelAdvancedTypes', 'name': 'BasicModelAdvancedTypes', 'type': 'error', 'fields': [{ 'name': 'A', 'type': { 'type': 'long', 'logicalType': 'timestamp-micros' } }, { 'name': 'B', 'type': { 'name': 'Bytes_10', 'type': 'fixed', 'size': 10 } }, { 'name': 'C', 'type': { 'type': 'bytes', 'logicalType': 'decimal', 'precision': 5, 'scale': 3 } }, { 'name': 'D', 'type': { 'name': 'BasicEnum', 'type': 'enum', 'symbols': ['TEST1', 'TEST2', 'TEST3'] } }, { 'name': 'E', 'type': { 'type': 'int', 'logicalType': 'date' } }, { 'name': 'F', 'type': { 'type': 'long', 'logicalType': 'time-micros' } }] }),
		(NestedModelBasicTypes, { 'namespace': 'NestedModelBasicTypes', 'name': 'NestedModelBasicTypes', 'type': 'error', 'fields': [{ 'name': 'A', 'type': { 'name': 'BasicModelBaseTypes', 'type': 'record', 'fields': [{ 'name': 'A', 'type': 'string' }, { 'name': 'B', 'type': 'long' }, { 'name': 'C', 'type': 'double' }, { 'name': 'D', 'type': 'bytes' }, { 'name': 'E', 'type': 'boolean' }] } }, { 'name': 'B', 'type': 'long' }] }),
		(BasicModelTypingTypes, { 'namespace': 'BasicModelTypingTypes', 'name': 'BasicModelTypingTypes', 'type': 'error', 'fields': [{ 'name': 'A', 'type': { 'type': 'array', 'items': 'long' } }, { 'name': 'B', 'type': { 'type': 'map', 'values': 'long' } }, { 'name': 'C', 'type': ['null', 'long'] }, { 'name': 'D', 'type': ['long', 'string'] }] }),
		(BasicModelCustomNamespace, { 'namespace': 'custom.namespace', 'name': 'BasicModelCustomNamespace', 'type': 'error', 'fields': [{ 'name': 'A', 'type': 'long' }] }),
		(BasicModelCustomNestedNamespace, { 'namespace': 'custom', 'name': 'BasicModelCustomNestedNamespace', 'type': 'error', 'fields': [{ 'name': 'A', 'type': { 'namespace': 'custom.namespace', 'name': 'BasicModelCustomNamespace', 'type': 'record', 'fields': [{ 'name': 'A', 'type': 'long' }] } }] }),
		(Error, { 'namespace': 'Error', 'name': 'Error', 'type': 'error', 'fields': [{ 'name': 'refid', 'type': ['null', { 'type': 'fixed', 'name': 'RefId', 'size': 16 }] }, { 'name': 'status', 'type': 'long' }, { 'name': 'error', 'type': 'string' }] }),
		(ValidationError, { 'namespace': 'ValidationError', 'name': 'ValidationError', 'type': 'error', 'fields': [{ 'name': 'detail', 'type': { 'items': { 'fields': [{ 'name': 'loc', 'type': { 'items': 'string', 'type': 'array' } }, { 'name': 'msg', 'type': 'string' }, { 'name': 'type', 'type': 'string' }], 'name': 'ValidationErrorDetail', 'type': 'record' }, 'type': 'array' } }] }),
		(BasicModelDefaultValues, { 'namespace': 'BasicModelDefaultValues', 'name': 'BasicModelDefaultValues', 'type': 'error', 'fields': [{ 'name': 'A', 'type': 'string', 'default': '1' }, { 'name': 'B', 'type': 'long', 'default': 2 }, { 'name': 'C', 'type': 'double', 'default': 3.1 }, { 'name': 'D', 'type': 'bytes', 'default': b'abc' }, { 'name': 'E', 'type': 'boolean', 'default': True }] }),
		(BasicModelCustomTypes, { 'namespace': 'BasicModelCustomTypes', 'name': 'BasicModelCustomTypes', 'type': 'error', 'fields': [{ 'name': 'A', 'type': 'int' }, { 'name': 'B', 'type': 'float' }] }),
		# (BasicModelUseEnumNames, { 'namespace': 'BasicModelUseEnumNames', 'name': 'BasicModelUseEnumNames', 'type': 'error', 'fields': [{ 'name': 'A', 'type': { 'name': 'BasicEnum', 'type': 'enum', 'symbols': ['test1', 'test2', 'test3'] } }], }),
	],
)
def test_ConvertSchema_ValidInputError_ErrorModelConvertedSuccessfully(input_model: Type[BaseModel], expected: dict) :

	# act
	schema: dict = convert_schema(input_model, error=True)

	# assert
	assert expected == schema


class BasicModelInvalidType1(BaseModel) :
	A: Decimal


class BasicModelInvalidType2(BaseModel) :
	A: dict


class BasicModelInvalidType3(BaseModel) :
	A: condecimal(max_digits=10)


class BasicModelInvalidType4(BaseModel) :
	A: condecimal(decimal_places=10)


class BasicModelInvalidType5(BaseModel) :
	A: Dict[int, int]


class BasicModelInvalidType6(BaseModel) :
	A: condecimal(decimal_places=10)


class BasicEnumInvalidType7(Enum) :
	test1: str = 'TEST1'
	test2: str = 'TEST2'
	test3: str = 'TEST1'


class NestedModelInvalidNamespace1(BaseModel) :
	A: BasicModelCustomNamespace


class NestedModelInvalidNamespace2(BaseModel) :
	__namespace__: str = 'custom_namespace'
	A: BasicModelCustomNamespace


@pytest.mark.parametrize(
	'input_model', [
		BasicModelInvalidType1,
		BasicModelInvalidType2,
		BasicModelInvalidType3,
		BasicModelInvalidType4,
		BasicModelInvalidType5,
		BasicModelInvalidType6,
		BasicEnumInvalidType7,
		NestedModelInvalidNamespace1,
		NestedModelInvalidNamespace2,
	],
)
def test_ConvertSchema_InvalidModel_ConvertSchemaThrowsError(input_model: Type[BaseModel]) :

	# assert
	with raises(AvroException) :
		print(convert_schema(input_model))


# this is a special case, these schemas are defined by the apache organtization and the schemas generated by convert_schema MUST match
# https://avro.apache.org/docs/current/spec.html#handshake
from avrofastapi.handshake import HandshakeRequest, HandshakeResponse


@pytest.mark.parametrize(
	'input_model, expected', [
		(HandshakeRequest, {
			'type': 'record',
			'name': 'HandshakeRequest', 'namespace':'org.apache.avro.ipc',
			'fields': [
				{ 'name': 'clientHash', 'type': { 'type': 'fixed', 'name': 'MD5', 'size': 16 } },
				{ 'name': 'clientProtocol', 'type': ['null', 'string'] },
				{ 'name': 'serverHash', 'type': 'MD5' },
				{ 'name': 'meta', 'type': ['null', { 'type': 'map', 'values': 'bytes' }] }
			]
		}),
		(HandshakeResponse, {
			'type': 'record',
			'name': 'HandshakeResponse', 'namespace': 'org.apache.avro.ipc',
			'fields': [
				{ 'name': 'match', 'type': { 'type': 'enum', 'name': 'HandshakeMatch', 'symbols': ['BOTH', 'CLIENT', 'NONE'] } },
				{ 'name': 'serverProtocol', 'type': ['null', 'string'] },
				{ 'name': 'serverHash', 'type': ['null', { 'type': 'fixed', 'name': 'MD5', 'size': 16 }] },
				{ 'name': 'meta', 'type': ['null', { 'type': 'map', 'values': 'bytes' }] }
			]
		}),
	],
)
def test_ConvertSchema_HandshakeModels_HandshakeConvertedSuccessfully(input_model: Type[BaseModel], expected: dict) :

	# act
	schema: dict = convert_schema(input_model)

	# assert
	assert expected == schema
