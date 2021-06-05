import ir_datasets
from typing import NamedTuple

class Example(NamedTuple):
	code: str = None
	code_lang: str = 'py'
	output: str = None
	message_html: str = None

def find_corpus_dataset(name):
    # adapted from https://github.com/Georgetown-IR-Lab/OpenNIR/blob/master/onir/datasets/irds.py#L47
    ds = ir_datasets.load(name)
    segments = name.split("/")
    docs_handler = ds.docs_handler()
    parent_docs_ds = name
    while len(segments) > 1:
        segments = segments[:-1]
        try:
            parent_ds = ir_datasets.load("/".join(segments))
            if parent_ds.has_docs() and parent_ds.docs_handler() == docs_handler:
                parent_docs_ds = "/".join(segments)
        except KeyError:
            pass
    return parent_docs_ds

from .python_generator import PythonExampleGenerator
from .cli_generator import CliExampleGenerator
from .pyterrier_generator import PyTerrierExampleGenerator
