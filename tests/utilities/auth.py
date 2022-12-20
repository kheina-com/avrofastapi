from asyncio import Future
from time import time
from uuid import UUID, uuid4

import ujson as json
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from kh_common.base64 import b64encode
from kh_common.config.constants import auth_host
from kh_common.utilities import int_to_bytes
from kh_common.utilities.json import json_stream

from tests.utilities.requests import MockResponse


private_key = Ed25519PrivateKey.generate()

public_key = private_key.public_key().public_bytes(
	encoding=serialization.Encoding.DER,
	format=serialization.PublicFormat.SubjectPublicKeyInfo,
)

pk_signature = private_key.sign(public_key)

expires = int(time() + 1000)
issued = time()


def mock_pk(mocker, key_id=1) -> None :
	mocker.patch(
		'kh_common.auth.async_request',
		side_effect=lambda *a, **kv : (
			None if kv['json'] != { 'key_id': key_id, 'algorithm': 'ed25519' } or a != ('POST', f'{auth_host}/v1/key',)
			else MockResponse({
				'signature': b64encode(pk_signature).decode(),
				'key': b64encode(public_key).decode(),
				'algorithm': 'ed25519',
				'expires': expires,
				'issued': issued,
			})
		)
	)


def mock_token(user_id: int, token_data:dict={ }, version:bytes=b'1', key_id=1, guid:UUID=None, valid_signature=True) -> str :
	load = b'.'.join([
		b'ed25519',                                        # algorithm
		b64encode(int_to_bytes(key_id)),                   # key_id
		b64encode(int_to_bytes(expires)),                  # expires timestamp
		b64encode(int_to_bytes(user_id)),                  # user id
		b64encode(guid.bytes if guid else uuid4().bytes),  # guid
		json.dumps(json_stream(token_data)).encode(),      # load
	])

	content = b64encode(version) + b'.' + b64encode(load)
	return (content + b'.' + b64encode(private_key.sign(content + (b'' if valid_signature else b'-')))).decode()
