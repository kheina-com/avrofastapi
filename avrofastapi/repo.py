from subprocess import PIPE, Popen
from typing import Optional


def stringSlice(string: str, start:str=None, end:str=None) -> str :
	if not string : return None
	assert start or end, 'start or end is required'
	start = string.rfind(start) + len(start) if start else None
	end = string.find(end) if end else None
	return string[start:end]

name: Optional[str] = None

output: Optional[bytes] = b''.join(Popen(['git', 'config', '--get', 'remote.origin.url'], stdout=PIPE, stderr=PIPE).communicate())
if output and not output.startswith(b'fatal'):
	name = stringSlice(output.decode(), '/', '.git')

else :
	output = b''.join(Popen(['git', 'rev-parse', '--show-toplevel'], stdout=PIPE, stderr=PIPE).communicate())
	if output and not output.startswith(b'fatal'):
		name = stringSlice(output.decode(), '/').strip()


short_hash: Optional[str] = None

output = b''.join(Popen(['git', 'rev-parse', '--short', 'HEAD'], stdout=PIPE, stderr=PIPE).communicate())
if output and not output.startswith(b'fatal'):
	short_hash = output.decode().strip()


full_hash: Optional[str] = None

output = b''.join(Popen(['git', 'rev-parse', 'HEAD'], stdout=PIPE, stderr=PIPE).communicate())
if output and not output.startswith(b'fatal'):
	full_hash = output.decode().strip()


del output, stringSlice, PIPE, Popen
