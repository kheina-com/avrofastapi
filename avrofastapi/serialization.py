try :
	from typing import Literal
except ImportError :
	Literal: type = str

import datetime
from decimal import Decimal
from enum import Enum
from io import BytesIO
from json import dumps
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Type, Union
from uuid import UUID
from warnings import warn

from avro.constants import DATE, DECIMAL, TIME_MICROS, TIME_MILLIS, TIMESTAMP_MICROS, TIMESTAMP_MILLIS
from avro.errors import AvroException, AvroTypeException, IgnoredLogicalType
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from avro.schema import ArraySchema, EnumSchema, FixedSchema, MapSchema, RecordSchema, Schema, UnionSchema
from avro.schema import parse as parse_avro_schema
from pydantic import BaseModel, parse_obj_as

from avrofastapi.schema import convert_schema


def _validate_enum(self, datum: Union[Enum, str]) :
	"""
	Return self if datum is a valid member of this Enum, else None.
	python Enums are converted to their value
	"""
	datum: str = datum.value if isinstance(datum, Enum) else datum
	return self if datum in self.symbols else None


EnumSchema.validate = _validate_enum


def avro_frame(bytes_to_frame: bytes = None) -> bytes :
	if bytes_to_frame :
		return len(bytes_to_frame).to_bytes(4, 'big') + bytes_to_frame

	return b'\x00\x00\x00\x00'


def read_avro_frames(avro_bytes: bytes) -> bytes :
	while avro_bytes :
		frame_len = int.from_bytes(avro_bytes[:4], 'big') + 4
		yield avro_bytes[4:frame_len]
		avro_bytes = avro_bytes[frame_len:]


class ABetterDatumWriter(DatumWriter) :

	def write_enum(self, writers_schema: EnumSchema, datum: Union[Enum, str], encoder: BinaryEncoder) -> None :
		"""
		An enum is encoded by a int, representing the zero-based position of the symbol in the schema.
		python Enums are converted to their value
		"""
		datum: str = datum.value if isinstance(datum, Enum) else datum
		index_of_datum: int = writers_schema.symbols.index(datum)
		return encoder.write_int(index_of_datum)


	def _writer_type_null_(writers_schema: Schema, datum: None, encoder: BinaryEncoder) -> None :
		if datum is None :
			return encoder.write_null(datum)
		raise AvroTypeException(writers_schema, datum)


	def _writer_type_bool_(writers_schema: Schema, datum: bool, encoder: BinaryEncoder) -> None :
		if isinstance(datum, bool) :
			return encoder.write_boolean(datum)
		raise AvroTypeException(writers_schema, datum)


	def _writer_type_str_(writers_schema: Schema, datum: Union[str, UUID], encoder: BinaryEncoder) -> None :
		if isinstance(datum, str) :
			return encoder.write_utf8(datum)

		elif isinstance(datum, UUID) :
			return encoder.write_utf8(str(datum))

		raise AvroTypeException(writers_schema, datum)


	def _writer_type_int_(writers_schema: Schema, datum: int, encoder: BinaryEncoder) -> None :
		logical_type: Optional[Literal] = getattr(writers_schema, 'logical_type', None)

		if logical_type == DATE :
			if isinstance(datum, datetime.date) :
				return encoder.write_date_int(datum)
			warn(IgnoredLogicalType(f'{datum} is not a date type'))

		elif logical_type == TIME_MILLIS :
			if isinstance(datum, datetime.time) :
				return encoder.write_time_millis_int(datum)
			warn(IgnoredLogicalType(f'{datum} is not a time type'))

		if isinstance(datum, int) :
			return encoder.write_int(datum)

		raise AvroTypeException(writers_schema, datum)


	def _writer_type_float_(writers_schema: Schema, datum: Union[int, float], encoder: BinaryEncoder) -> None :
		if isinstance(datum, (int, float)) :
			return encoder.write_float(datum)
		raise AvroTypeException(writers_schema, datum)


	def _writer_type_double_(writers_schema: Schema, datum: Union[int, float], encoder: BinaryEncoder) -> None :
		if isinstance(datum, (int, float)) :
			return encoder.write_double(datum)
		raise AvroTypeException(writers_schema, datum)


	def _writer_type_long_(writers_schema: Schema, datum: int, encoder: BinaryEncoder) -> None :
		logical_type: Optional[Literal] = getattr(writers_schema, 'logical_type', None)

		if logical_type == TIME_MICROS :
			if isinstance(datum, datetime.time) :
				return encoder.write_time_micros_long(datum)
			warn(IgnoredLogicalType(f'{datum} is not a time type'))

		elif logical_type == TIMESTAMP_MILLIS :
			if isinstance(datum, datetime.datetime) :
				return encoder.write_timestamp_millis_long(datum)
			warn(IgnoredLogicalType(f'{datum} is not a datetime type'))

		elif logical_type == TIMESTAMP_MICROS :
			if isinstance(datum, datetime.datetime) :
				return encoder.write_timestamp_micros_long(datum)
			warn(IgnoredLogicalType(f'{datum} is not a datetime type'))

		if isinstance(datum, int) :
			return encoder.write_long(datum)

		raise AvroTypeException(writers_schema, datum)


	def _writer_type_bytes_(writers_schema: Schema, datum: Union[Decimal, bytes], encoder: BinaryEncoder) -> None :
		logical_type: Optional[Literal] = getattr(writers_schema, 'logical_type', None)

		if logical_type == DECIMAL :
			scale: int = writers_schema.get_prop('scale')

			if not (isinstance(scale, int) and scale > 0) :
				warn(IgnoredLogicalType(f'Invalid decimal scale {scale}. Must be a positive integer.'))

			elif not isinstance(datum, Decimal) :
				warn(IgnoredLogicalType(f'{datum} is not a decimal type'))

			elif not datum.same_quantum(Decimal(1) / 10 ** scale) :
				# round can be used here if we want to force all decimals to the correct scale
				# round(datum, scale)
				raise AvroTypeException(writers_schema, datum)

			else :
				return encoder.write_decimal_bytes(datum, scale)

		if isinstance(datum, bytes) :
			return encoder.write_bytes(datum)

		raise AvroTypeException(writers_schema, datum)


	def _writer_type_fixed_(self, writers_schema: Schema, datum: Union[Decimal, bytes], encoder: BinaryEncoder) -> None :
		logical_type: Optional[Literal] = getattr(writers_schema, 'logical_type', None)

		if logical_type == DECIMAL :
			scale: int = writers_schema.get_prop('scale')

			if not (isinstance(scale, int) and scale > 0) :
				warn(IgnoredLogicalType(f'Invalid decimal scale {scale}. Must be a positive integer.'))

			elif not isinstance(datum, Decimal) :
				warn(IgnoredLogicalType(f'{datum} is not a decimal type'))

			elif not datum.same_quantum(Decimal(1) / 10 ** scale) :
				raise AvroTypeException(writers_schema, datum)

			else :
				return encoder.write_decimal_fixed(datum, scale, writers_schema.size)

		if isinstance(datum, bytes) :
			return self.write_fixed(writers_schema, datum, encoder)

		raise AvroTypeException(writers_schema, datum)


	def _writer_type_enum_(self, writers_schema: Schema, datum: Union[str, Enum], encoder: BinaryEncoder) -> None :
		if isinstance(datum, (str, Enum)) :
			return self.write_enum(writers_schema, datum, encoder)

		raise AvroTypeException(writers_schema, datum)


	def _writer_type_array_(self, writers_schema: Schema, datum: Sequence, encoder: BinaryEncoder) -> None :
		if isinstance(datum, Sequence) :
			return self.write_array(writers_schema, datum, encoder)
		raise AvroTypeException(writers_schema, datum)


	def _writer_type_map_schema_(self, writers_schema: Schema, datum: Dict[str, Any], encoder: BinaryEncoder) -> None :
		if isinstance(datum, Mapping) :
			return self.write_map(writers_schema, datum, encoder)
		raise AvroTypeException(writers_schema, datum)


	def _writer_type_record_(self, writers_schema: Schema, datum: dict, encoder: BinaryEncoder) -> None :
		if isinstance(datum, Mapping) :
			return self.write_record(writers_schema, datum, encoder)
		raise AvroTypeException(writers_schema, datum)


	_writer_type_map_ = {
		'null': _writer_type_null_,
		'boolean': _writer_type_bool_,
		'string': _writer_type_str_,
		'int': _writer_type_int_,
		'long': _writer_type_long_,
		'float': _writer_type_float_,
		'double': _writer_type_double_,
		'bytes': _writer_type_bytes_,
		FixedSchema: _writer_type_fixed_,
		EnumSchema: _writer_type_enum_,
		ArraySchema: _writer_type_array_,
		MapSchema: _writer_type_map_schema_,
		UnionSchema: DatumWriter.write_union,
		RecordSchema: _writer_type_record_,
	}


	def write_data(self, writers_schema: Schema, datum: Any, encoder: BinaryEncoder) -> None :
		# we're re-writing the function to dispatch writing datum, cause, frankly, theirs sucks

		if writers_schema.type in self._writer_type_map_ :
			return self._writer_type_map_[writers_schema.type](writers_schema, datum, encoder)

		for cls in type(writers_schema).__mro__ :
			if cls in self._writer_type_map_ :
				return self._writer_type_map_[cls](self, writers_schema, datum, encoder)

		raise AvroException(f'Unknown type: {writers_schema.type}')


_data_converter_map = {
	dict: lambda d : d,
	list: lambda d : list(map(BaseModel.dict, d)),
	tuple: lambda d : list(map(BaseModel.dict, d)),
	BaseModel: BaseModel.dict,
}


class AvroSerializer :

	def __init__(self, model: Union[Schema, Type[BaseModel]]) :
		schema: Schema = model if isinstance(model, Schema) else parse_avro_schema(dumps(convert_schema(model)))
		self._writer: ABetterDatumWriter = ABetterDatumWriter(schema)


	def __call__(self, data: BaseModel) :
		io_object: BytesIO = BytesIO()
		encoder: BinaryEncoder = BinaryEncoder(io_object)

		for cls in type(data).__mro__ :
			if cls in _data_converter_map :
				self._writer.write_data(self._writer.writers_schema, _data_converter_map[cls](data), encoder)
				return io_object.getvalue()

		raise NotImplementedError(f'unable to convert "{type(data)}" for encoding')


class AvroDeserializer :

	def __init__(self, read_model: Type[BaseModel] = None, read_schema: Union[Schema, str] = None, write_model: Union[Schema, Type[BaseModel], str] = None, parse: bool = True) :
		assert read_model or (read_schema and parse == False), 'either read_model or read_schema must be provided. if only read schema is provided, parse must be false'
		write_schema: Schema

		if not read_schema :
			read_schema = parse_avro_schema(dumps(convert_schema(read_model)))

		elif isinstance(read_schema, str) :
			read_schema = parse_avro_schema(read_schema)

		elif not isinstance(read_schema, Schema) :
			raise NotImplementedError(f'the type for read_schema "{type(read_schema)}" is not supported.')


		if not write_model :
			write_schema = read_schema

		elif isinstance(write_model, Schema) :
			write_schema = write_model

		elif isinstance(write_model, str) :
			write_schema = parse_avro_schema(write_model)

		elif issubclass(write_model, BaseModel) :
			write_schema = parse_avro_schema(dumps(convert_schema(write_model)))

		else :
			raise NotImplementedError(f'the type for write_model "{type(write_model)}" is not supported.')

		reader: DatumReader = DatumReader(write_schema, read_schema)

		if parse :
			self._parser: Callable[[bytes], read_model] = lambda x : parse_obj_as(read_model, reader.read(x))

		else :
			self._parser: Callable[[bytes], dict] = reader.read


	def __call__(self, data: bytes) :
		return self._parser(BinaryDecoder(BytesIO(data)))
