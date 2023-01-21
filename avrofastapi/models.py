from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, conbytes, validator


class RefId(conbytes(max_length=16, min_length=16)) :
	pass


class Error(BaseModel) :
	refid: Optional[RefId]
	status: int
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
