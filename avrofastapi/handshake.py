from enum import Enum
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, conbytes, validator


class MD5(conbytes(min_length=16, max_length=16)) :
	pass


class HandshakeMatch(Enum) :
	__use_enum_names__: bool = False
	both: str = 'BOTH'
	client: str = 'CLIENT'
	none: str = 'NONE'


class HandshakeRequest(BaseModel) :
	__namespace__: str = 'org.apache.avro.ipc'
	clientHash: MD5
	clientProtocol: Optional[str]
	serverHash: MD5
	meta: Optional[Dict[str, bytes]]


class HandshakeResponse(BaseModel) :
	__namespace__: str = 'org.apache.avro.ipc'
	match: HandshakeMatch
	serverProtocol: Optional[str]
	serverHash: Optional[MD5]
	meta: Optional[Dict[str, bytes]]


__built_in_types__ = { 'null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string' }


class AvroMessage(BaseModel) :
	"""
	each 'dict' in this definition refers to a parsed avro schema. request is the 'fields' of a schema
	these can be obtained through the kh_common.avro.schema.convert_schema(Type[BaseModel]) function
	NOTE: these are never avro-encoded. only json-stringified.
	"""
	doc: Optional[str]
	types: List[dict] = []
	request: List[dict] = []
	response: Union[str, dict] = 'null'
	errors: Optional[List[Union[str, dict]]]
	oneWay: bool = None

	@validator('request', 'errors')
	def validate_models_exist(cls, value, values) :
		if value is None :
			return value

		types = { v['name'] for v in values.get('types', []) } | __built_in_types__
		missing_types = []

		for model in value :
			model = model if isinstance(model, str) else model['type']
			if isinstance(model, str) and model not in types :
				missing_types.append(model)

		assert not missing_types, f'string types must exist in types, [{", ".join(missing_types)}] missing from types'
		return value

	@validator('response')
	def validate_response_exists(cls, value, values) :
		if isinstance(value, str) and value not in __built_in_types__ :
			assert value in { v['name'] for v in values.get('types', []) }, f'string types must exist in types, {value} missing from types'
		return value

	@validator('oneWay', pre=True, always=True)
	def set_oneWay(cls, value, values) :
		return value or values.get('response', 'null') == 'null'


class AvroProtocol(BaseModel) :
	"""
	NOTE: these are never avro-encoded. only json-stringified.
	"""
	namespace: str
	protocol: str
	messages: Dict[str, AvroMessage]


class CallRequest(BaseModel) :
	__namespace__: str = 'org.apache.avro.ipc'
	meta: Optional[Dict[str, bytes]]  # a map with values of type bytes
	message: str  # an Avro string, this is used as lookup key in the response handshake's messages field
	request: bytes  # parameters are serialized according to the message's request declaration


class CallResponse(BaseModel) :
	__namespace__: str = 'org.apache.avro.ipc'
	meta: Optional[Dict[str, bytes]]  # a map with values of type bytes
	error: bool  # a one-byte error flag boolean, followed by either
	# if the error flag is false: the message response, serialized per the message's response schema.
	# if the error flag is true: the error, serialized per the message's effective error union schema.
	response: bytes
