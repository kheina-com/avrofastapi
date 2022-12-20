import ujson


class MockResponse :

	def __init__(self, load, status = 200) :
		self.status = status
	
		if isinstance(load, str) :
			self.content = load

		elif isinstance(load, dict) :
			self.content = ujson.dumps(load)


	async def json(self) :
		return ujson.loads(self.content)


	async def text(self) :
		return self.content


	async def __aexit__(self, *args):
		pass


	async def __aenter__(self):
		return self