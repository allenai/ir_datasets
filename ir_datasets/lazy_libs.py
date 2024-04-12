# These libraries can add a bunch of overhead when imported -- which is bad for command line
# utilities. This file loads them lazily if they are needed.
_cache = {}

def numpy():
    if 'numpy' not in _cache:
        import numpy
        _cache['numpy'] = numpy
    return _cache['numpy']


def tqdm():
    if 'tqdm' not in _cache:
        import tqdm
        _cache['tqdm'] = tqdm
    return _cache['tqdm']


def requests():
    if 'requests' not in _cache:
        import requests
        _cache['requests'] = requests
    return _cache['requests']


def bs4():
    if 'bs4' not in _cache:
        import bs4
        _cache['bs4'] = bs4
    return _cache['bs4']


def inscriptis():
    if 'inscriptis' not in _cache:
        import inscriptis
        _cache['inscriptis'] = inscriptis
    return _cache['inscriptis']


def yaml():
    if 'yaml' not in _cache:
        import yaml
        _cache['yaml'] = yaml
    return _cache['yaml']


def json():
    if 'json' not in _cache:
        import json
        _cache['json'] = json
    return _cache['json']


def trec_car():
    if 'trec_car' not in _cache:
        import trec_car.read_data
        _cache['trec_car'] = trec_car
    return _cache['trec_car']

def warc():
    if 'warc' not in _cache:
        import warc
        _cache['warc'] = warc
    return _cache['warc']

def warc_clueweb09():
    if 'warc_clueweb09' not in _cache:
        import warc3_wet_clueweb09
        _cache['warc_clueweb09'] = warc3_wet_clueweb09
    return _cache['warc_clueweb09']

def lz4_block():
    if 'lz4_block' not in _cache:
        import lz4.block
        _cache['lz4_block'] = lz4
    return _cache['lz4_block']

def lz4_frame():
    if 'lz4_frame' not in _cache:
        import lz4.frame
        _cache['lz4_frame'] = lz4
    return _cache['lz4_frame']

def zlib_state():
    if 'zlib_state' not in _cache:
        import zlib_state
        _cache['zlib_state'] = zlib_state
    return _cache['zlib_state']

def xml_etree():
    if 'xml_etree' not in _cache:
        import xml.etree.ElementTree as ET
        _cache['xml_etree'] = ET
    return _cache['xml_etree']

def lxml_html():
    if 'lxml_html' not in _cache:
        import lxml.html
        _cache['lxml_html'] = lxml.html
    return _cache['lxml_html']

def ijson():
    if 'ijson' not in _cache:
        import ijson
        _cache['ijson'] = ijson
    return _cache['ijson']

def pyautocorpus():
    if 'pyautocorpus' not in _cache:
        try:
            import pyautocorpus
        except ImportError as ie:
            raise ImportError("This dataset requires pyautocorpus. Run 'pip install pyautocorpus'") from ie
        _cache['pyautocorpus'] = pyautocorpus
    return _cache['pyautocorpus']

def unlzw3():
    if 'unlzw3' not in _cache:
        import unlzw3
        _cache['unlzw3'] = unlzw3
    return _cache['unlzw3']
