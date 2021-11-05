from enum import Enum
from pathlib import Path
from typing import NamedTuple, List, Optional

from ijson import items

from ir_datasets.formats import BaseDocs
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.util import Cache


class ArgsMeStance(Enum):
    """
    See the corresponding Java source files from the args.me project:
    https://git.webis.de/code-research/arguana/args/args-framework/-/blob/master/src/main/java/me/args/argument/Stance.java
    """
    PRO = 2
    CON = 2

    @staticmethod
    def from_json(json: str) -> "ArgsMeStance":
        if json == "PRO":
            return ArgsMeStance.PRO
        if json == "CON":
            return ArgsMeStance.CON
        else:
            raise ValueError(f"Unknown stance {json}")


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
        return ArgsMePremiseAnnotation(
            int(json["start"]),
            int(json["end"]),
            str(json["source"]),
        )


class ArgsMePremise(NamedTuple):
    """
    See the corresponding Java source files from the args.me project:
    https://git.webis.de/code-research/arguana/args/args-framework/-/blob/master/src/main/java/me/args/argument/Premise.java
    """
    text: str
    stance: ArgsMeStance
    annotations: List[ArgsMePremiseAnnotation]

    @staticmethod
    def from_json(json: dict) -> "ArgsMePremise":
        return ArgsMePremise(
            str(json["text"]),
            ArgsMeStance.from_json(json["stance"]),
            [
                ArgsMePremiseAnnotation.from_json(annotation)
                for annotation in json["annotations"]
            ] if "annotations" in json else [],
        )


class ArgsMeDoc(NamedTuple):
    """
    See the corresponding Java source files from the args.me project:
    https://git.webis.de/code-research/arguana/args/args-framework/-/blob/master/src/main/java/me/args/Argument.java
    """
    doc_id: str
    conclusion: str
    premises: List[ArgsMePremise]

    @staticmethod
    def from_json(json: dict) -> "ArgsMeDoc":
        return ArgsMeDoc(
            str(json["id"]),
            str(json["conclusion"]),
            [
                ArgsMePremise.from_json(premise)
                for premise in json["premises"]
            ],
        )


class ArgsMeDocs(BaseDocs):
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

    def docs_path(self):
        return self._source.path()

    def docs_iter(self):
        with self._source.stream() as json_stream:
            argument_jsons = items(json_stream, "arguments.item")
            for argument_json in argument_jsons:
                argument = ArgsMeDoc.from_json(argument_json)
                yield argument

    def docs_store(self, field="doc_id"):
        return PickleLz4FullStore(
            path=f"{self.docs_path()}.pklz4",
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=["doc_id"],
            count_hint=self._count_hint,
        )

    def docs_count(self):
        return self._count_hint

    def docs_cls(self):
        return ArgsMeDoc

    def docs_namespace(self):
        return self._namespace

    def docs_lang(self):
        return self._language


class ArgsMeCombinedArguments(BaseDocs):
    _path: Path
    _sources: List[ArgsMeDocs]
    _namespace: Optional[str]
    _language: Optional[str]
    _count_hint: Optional[int]

    def __init__(
            self,
            path: Path,
            sources: List[ArgsMeDocs],
            namespace: Optional[str] = None,
            language: Optional[str] = None,
            count_hint: Optional[int] = None,
    ):
        self._path = path
        self._sources = sources
        self._namespace = namespace
        self._language = language
        self._count_hint = count_hint

    def docs_path(self):
        return self._path

    def docs_iter(self):
        for source in self._sources:
            for argument in source.docs_iter():
                yield argument

    def docs_store(self, field="doc_id"):
        return PickleLz4FullStore(
            path=f"{self.docs_path()}.pklz4",
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=["doc_id"],
            count_hint=self._count_hint,
        )

    def docs_count(self):
        assert (sum(
            source.docs_count()
            for source in self._sources
        ) == self._count_hint)
        return self._count_hint

    def docs_cls(self):
        assert (all(
            source.docs_cls() == ArgsMeDoc
            for source in self._sources
        ))
        return ArgsMeDoc

    def docs_namespace(self):
        assert (all(
            source.docs_namespace() in self._namespace
            for source in self._sources
        ))
        return self._namespace

    def docs_lang(self):
        assert (all(
            source.docs_lang() == self._language
            for source in self._sources
        ))
        return self._language
