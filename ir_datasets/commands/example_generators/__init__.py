import ir_datasets
from typing import NamedTuple

class Example(NamedTuple):
	code: str = None
	code_lang: str = 'py'
	output: str = None
	message_html: str = None


from .python_generator import PythonExampleGenerator
from .cli_generator import CliExampleGenerator
from .pyterrier_generator import PyTerrierExampleGenerator
