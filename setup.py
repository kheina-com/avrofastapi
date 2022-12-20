try :
	from re import _pattern_type as Pattern
	from re import compile as re_compile
except ImportError :
	from re import Pattern, compile as re_compile

from os import listdir

from setuptools import find_packages, setup

from avrofastapi import __version__


req_regex: Pattern = re_compile(r'^requirements-(\w+).txt$')


setup(
	name='avrofastapi',
	version=__version__,
	description='Automatic avro wire protocol support for FastAPI',
	long_description=open('readme.md').read(),
	long_description_content_type='text/markdown',
	author='kheina',
	url='https://github.com/kheina-com/avrofastapi',
	packages=find_packages(exclude=['tests']),
	install_requires=list(filter(None, map(str.strip, open('requirements.txt').read().split()))),
	python_requires='>=3.7.*',
	license='Mozilla Public License 2.0',
	extras_require=dict(map(lambda x : (x[1], open(x[0]).read().split()), filter(None, map(req_regex.match, listdir())))),
)