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


def yaml():
    if 'yaml' not in _cache:
        import yaml
        _cache['yaml'] = yaml
    return _cache['yaml']


def trec_car():
    if 'trec_car' not in _cache:
        import trec_car.read_data
        _cache['trec_car'] = trec_car
    return _cache['trec_car']
