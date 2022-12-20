import time
from collections import defaultdict
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Type

import aerospike


AerospikeKey: Type = Tuple[str, str, str]
OneMonth: int = 2629800  # one month in seconds since that's the default for our installation


class AerospikeClient :

	def __init__(self: 'AerospikeClient', config: Dict[str, Any] = None) :
		# tbh we really don't give a shit about the config since we're not setting anything up
		self._data = { }
		self._ttl = { }
		self.calls = defaultdict(lambda : [])


	def clear(self: 'AerospikeClient') -> None :
		self._data.clear()
		self._ttl.clear()
		self.calls.clear()


	def __assert_key_type__(self: 'AerospikeClient', key: AerospikeKey) -> None :
		assert type(key) == tuple
		assert len(key) == 3

		for t in map(type, key) :
			assert t in { str, bytes }


	def __assert_data_type__(self: 'AerospikeClient', data: Dict[str, Any]) -> None :
		assert set(map(type, data.keys())) == { str }


	def __get_ttl__(self: 'AerospikeClient', meta: Dict[str, Any] = None) -> int :
		return int(meta['ttl'] if meta and 'ttl' in meta else OneMonth) or OneMonth


	def put(self: 'AerospikeClient', key: AerospikeKey, data: Dict[str, Any], meta: Dict[str, Any] = None, policy: Dict[str, Any] = None) -> None :
		self.calls['put'].append((key, data, meta, policy))
		self.__assert_key_type__(key)
		self.__assert_data_type__(data)

		ttl: int = self.__get_ttl__(meta)

		# a couple of these I'm not sure what they're supposed to be
		# gen is set via meta or policy and the None I'm not sure of, but don't really care atm
		self._data[key] = ((*key, hash(key).to_bytes(8, 'big', signed=True)), { 'ttl': ttl, 'gen': 1 }, data)
		self._ttl[key] = time.time()


	def get(self: 'AerospikeClient', key: AerospikeKey) -> Any :
		self.calls['get'].append((key))
		self.__assert_key_type__(key)
		ex = aerospike.exception.RecordNotFound(2, '127.0.0.1:3000 AEROSPIKE_ERR_RECORD_NOT_FOUND', 'src/main/client/get.c', 118, False)

		if key not in self._data :
			raise ex

		data = deepcopy(self._data[key])
		data[1]['ttl'] = round(max(data[1]['ttl'] - (time.time() - self._ttl[key]), 0))

		if data[1]['ttl'] <= 0 and data[1]['ttl'] != -1 :
			raise ex

		return data


	def get_many(self: 'AerospikeClient', keys: List[AerospikeKey], meta: Dict[str, Any] = None, policy: Dict[str, Any] = None) -> List[Any] :
		self.calls['get_many'].append((keys, meta, policy))
		data = []
		for key in keys :
			try :
				data.append(self.get(key))
				# this is a total hack but I don't care
				del self.calls['get'][-1]

			except aerospike.exception.RecordNotFound :
				data.append(((*key, hash(key).to_bytes(8, 'big', signed=True)), None, None))

		return data


	def increment(self: 'AerospikeClient', key: AerospikeKey, bin: str, value: int, meta: Dict[str, Any] = None, policy: Dict[str, Any] = None) -> None :
		self.calls['increment'].append((key, bin, value, meta, policy))
		self.__assert_key_type__(key)
		assert type(bin) == str

		ttl: int = self.__get_ttl__(meta)

		if key not in self._data :
			# a couple of these I'm not sure what they're supposed to be
			# gen is set via meta or policy and the None I'm not sure of, but don't really care atm
			self._data[key] = ((*key, hash(key).to_bytes(8, 'big', signed=True)), { 'ttl': ttl, 'gen': 1 }, { bin: value })
			self._ttl[key] = time.time()
			return

		if bin not in self._data[key][2] :
			self._data[key][2][bin] = value
			self._data[key][1]['ttl'] = ttl
			self._ttl[key] = time.time()
			return

		self._data[key][2][bin] += value
		self._data[key][1]['ttl'] = ttl
		self._ttl[key] = time.time()


	def remove(self: 'AerospikeClient', key: AerospikeKey, meta: Dict[str, Any] = None, policy: Dict[str, Any] = None) -> None :
		self.calls['remove'].append((key, meta, policy))

		if key not in self._data :
			raise aerospike.exception.RecordNotFound(2, 'AEROSPIKE_ERR_RECORD_NOT_FOUND', 'src/main/client/remove.c', 124, False)

		del self._data[key]
		del self._ttl[key]


	def exists(self: 'AerospikeClient', key: AerospikeKey, meta: Dict[str, Any] = None, policy: Dict[str, Any] = None) -> Tuple[Tuple[str, str, str, bytes], Optional[Dict[str, Any]]] :
		self.calls['exists'].append((key, meta, policy))

		"""
		this class is supposed to mock the implemented functionality of the python aerospike client library and not the ideal functionality of the library.
		in this case, we have a difference between what the documentation of the library says it's supposed to do and what it actually does.

		at the time of writing, the behavior of the library is such that the RecordNotFound error is never thrown, rather, a None meta is returned.
		in the future, when this bug is potentially fixed, this function should be changed to throw an error instead of returning empty data.
		"""

		if key in self._data :
			return self._data[key][:2]

		return (*key, hash(key).to_bytes(8, 'big', signed=True)), None
