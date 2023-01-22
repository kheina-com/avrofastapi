from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, conbytes, validator

from avrofastapi.schema import AvroInt


class RefId(conbytes(max_length=16, min_length=16)) :
	pass


# NOTE: all errors use a generated namespace. this is so that they can share a namespace with other generated models and pass handshakes
class Error(BaseModel) :
	refid: Optional[RefId]
	status: AvroInt
	error: str

	class Config:
		json_encoders = {
			bytes: bytes.hex,
		}

	@validator('refid', pre=True)
	def convert_uuid_bytes(value):
		if isinstance(value, UUID) :
			return value.bytes
		return value


class ValidationErrorDetail(BaseModel) :
	loc: List[str]
	msg: str
	type: str


class ValidationError(BaseModel) :
	detail: List[ValidationErrorDetail]
