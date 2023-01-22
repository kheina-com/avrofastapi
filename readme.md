![AvroFastAPI Logo](/logo.png)
<p align="center">
	<a href="https://github.com/kheina-com/avrofastapi/actions?query=workflow%3Apython-package+event%3Apush+branch%3Amain">
		<img src="https://github.com/kheina-com/avrofastapi/actions/workflows/python-package.yml/badge.svg?branch=main" alt="python-package.yml workflow">
	</a>
	<a href="https://pypi.org/project/avrofastapi">
		<img src="https://img.shields.io/pypi/v/avrofastapi?color=success&label=pypi%20package" alt="pypi package version">
	</a>
</p>
<p align="center">
	Add <a href="https://avro.apache.org/docs/1.11.1/specification/_print/#protocol-wire-format">Avro encoding</a> support to your FastAPI application with a one-line setup
</p>

```diff
- app = FastAPI()
+ app = AvroFastAPI()
```

# Installation
`pip install avrofastapi`

# Usage
## SERVER
```python
from avrofastapi import AvroFastAPI
from datetime import datetime, timezone
from pydantic import BaseModel


app = AvroFastAPI()

class TestResponseModel(BaseModel) :
	A: str
	B: int
	C: datetime


@app.get('/', response_model=TestResponseModel)
def v1Example() :
	return TestResponseModel(
		A='ayy',
		B=1337,
		C=datetime.now(timezone.utc),
	)


if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='0.0.0.0', port=5000)
```

## CLIENT
```python
from avrofastapi.gateway import Gateway
from datetime import datetime
from pydantic import BaseModel
import requests
import asyncio

class TestResponseModel(BaseModel) :
	A: str 
	B: int
	C: datetime

requests.get('http://localhost:5000/').json()
# returns: {'A': 'ayy', 'B': 1337, 'C': '2023-01-22T10:01:00.543317+00:00'}

gateway = Gateway('http://localhost:5000/', 'v1Example__get', response_model=TestResponseModel)
asyncio.run(gateway())
# returns: TestResponseModel(A='ayy', B=1337, C=datetime.datetime(2023, 1, 22, 10, 2, 29, 641314, tzinfo=<avro.timezones.UTCTzinfo object at 0x7efbe9cdb580>))
```

# Development
Fork the parent repository at https://github.com/kheina-com/avrofastapi and edit like any other python project.  
Tests are run with `pytest` in the command line and input sorting is run via `isort .`

# License
This work is licensed under the [Mozilla Public License 2.0](https://choosealicense.com/licenses/mpl-2.0/), allowing for public, private, and commercial use so long as access to this library's source code is provided. If this library's source code is modified, then the modified source code must be licensed under the same license or an [applicable GNU license](https://www.mozilla.org/en-US/MPL/2.0/#1.12) and made publicly available.
