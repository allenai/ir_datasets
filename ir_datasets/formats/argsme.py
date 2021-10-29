from enum import Enum
from typing import NamedTuple, List, Any, Dict, Optional

from ir_datasets import load
from ir_datasets.formats import BaseDocs
from ir_datasets.util import Cache


class ArgsMeStance(Enum):
    """
    See the corresponding Java source files from the args.me project:
    https://git.webis.de/code-research/arguana/args/args-framework/-/blob/master/src/main/java/me/args/argument/Stance.java
    """
    PRO = 2
    CON = 2

    @staticmethod
    def from_json(json: dict) -> "ArgsMeStance":
        raise NotImplementedError()


class ArgsMePremiseAnnotation(NamedTuple):
    """
    See the corresponding Java source files from the args.me project:
    https://git.webis.de/code-research/arguana/args/args-framework/-/blob/master/src/main/java/me/args/argument/PremiseAnnotation.java
    """
    start: int
    end: int
    source: str

    @staticmethod
    def from_json(json: dict) -> "ArgsMePremiseAnnotation":
        raise NotImplementedError()


class ArgsMePremise(NamedTuple):
    """
    See the corresponding Java source files from the args.me project:
    https://git.webis.de/code-research/arguana/args/args-framework/-/blob/master/src/main/java/me/args/argument/Premise.java
    """
    text: str
    stance: ArgsMeStance
    annotations: ArgsMePremiseAnnotation

    @staticmethod
    def from_json(json: dict) -> "ArgsMePremise":
        raise NotImplementedError()


class ArgsMeArgument(NamedTuple):
    """
    See the corresponding Java source files from the args.me project:
    https://git.webis.de/code-research/arguana/args/args-framework/-/blob/master/src/main/java/me/args/Argument.java
    """
    id: str
    conclusion: str
    premises: List[ArgsMePremise]
    context: Dict[str, Any]

    @staticmethod
    def from_json(json: dict) -> "ArgsMeArgument":
        raise NotImplementedError()


class ArgsMeArguments(BaseDocs):
    _source: Cache
    _namespace: Optional[str]
    _language: Optional[str]
    _count_hint: Optional[int]

    def __init__(
            self,
            cache: Cache,
            namespace: Optional[str] = None,
            language: Optional[str] = None,
            count_hint: Optional[int] = None,
    ):
        self._source = cache
        self._namespace = namespace
        self._language = language
        self._count_hint = count_hint

    def docs_iter(self):
        with self._source.stream() as json_stream:
            with load(json_stream) as arguments_json:
                for argument_json in arguments_json:
                    yield ArgsMeArgument.from_json(argument_json)

    def docs_count(self):
        return self._count_hint

    def docs_cls(self):
        return ArgsMeArgument

    def docs_namespace(self):
        return self._namespace

    def docs_lang(self):
        return self._language
