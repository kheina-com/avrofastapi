import time

from kh_common import caching
from kh_common.caching import key_value_store


class CachingTestClass :

	def setup_method(self) :
		# global setup
		caching.fake_time_store = 1
		def fake_time() :
			caching.fake_time_store += 1
			return caching.fake_time_store - 1

		caching.time = fake_time
		key_value_store.time = fake_time


	def teardown_method(self) :
		caching.time = time.time
		key_value_store.time = time.time
