from ast import literal_eval
from csv import DictReader, field_size_limit
from datetime import datetime
from enum import Enum
from io import TextIOWrapper
from pathlib import Path
from re import sub
from sys import maxsize
from typing import NamedTuple, List, Optional

from ir_datasets import lazy_libs
from ir_datasets.formats import BaseDocs
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.util import Cache, use_docstore


class ArgsMeStance(Enum):
    """
    See the corresponding Java source files from the args.me project:
    https://git.webis.de/code-research/arguana/args/args-framework/-/blob/master/src/main/java/me/args/argument/Stance.java
    """
    PRO = 1
    CON = 2

    @staticmethod
    def from_json(json: str) -> "ArgsMeStance":
        if json == "PRO":
            return ArgsMeStance.PRO
        elif json == "CON":
            return ArgsMeStance.CON
        else:
            raise ValueError(f"Unknown stance {json}")


class ArgsMeMode(Enum):
    person = 1
    discussion = 2

    @staticmethod
    def from_json(json: str) -> "ArgsMeMode":
        if json == "person":
            return ArgsMeMode.person
        elif json == "discussion":
            return ArgsMeMode.discussion
        else:
            raise ValueError(f"Unknown mode {json}")


class ArgsMeSourceDomain(Enum):
    debateorg = 1
    debatepedia = 2
    debatewise = 3
    idebate = 4
    canadian_parliament = 5

    @staticmethod
    def from_json(json: str) -> "ArgsMeSourceDomain":
        if json == "debate.org":
            return ArgsMeSourceDomain.debateorg
        elif json == "debatepedia":
            return ArgsMeSourceDomain.debatepedia
        elif json == "debatewise":
            return ArgsMeSourceDomain.debatewise
        elif json == "idebate":
            return ArgsMeSourceDomain.idebate
        elif json == "canadian-parliament":
            return ArgsMeSourceDomain.canadian_parliament
        else:
            raise ValueError(f"Unknown source domain {json}")


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
        start = int(json["start"])
        end = int(json["end"])
        source = str(json["source"])
        return ArgsMePremiseAnnotation(start, end, source)


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


class ArgsMeAspect(NamedTuple):
    name: str
    weight: float
    normalized_weight: float
    rank: int

    @staticmethod
    def from_json(json: dict) -> "ArgsMeAspect":
        name = str(json["name"])
        weight = float(json["weight"])
        normalized_weight = float(json["normalizedWeight"])
        rank = int(json["rank"])
        return ArgsMeAspect(name, weight, normalized_weight, rank)


class ArgsMeSentence(NamedTuple):
    id: str
    text: str

    @staticmethod
    def from_json(json: dict) -> "ArgsMeSentence":
        return ArgsMeSentence(
            str(json["sent_id"]),
            str(json["sent_text"]),
        )


class ArgsMeDoc(NamedTuple):
    """
    See the corresponding Java source files from the args.me project:
    https://git.webis.de/code-research/arguana/args/args-framework/-/blob/master/src/main/java/me/args/Argument.java
    https://git.webis.de/code-research/arguana/args/args-framework/-/blob/master/src/main/java/me/args/argument/Premise.java
    """
    doc_id: str
    conclusion: str
    premises: List[ArgsMePremise]
    premises_texts: str  # Premises texts concatenated with spaces.
    aspects: List[ArgsMeAspect]
    aspects_names: str  # Aspects namews concatenated with spaces.
    source_id: str
    source_title: str
    source_url: Optional[str]
    source_previous_argument_id: Optional[str]
    source_next_argument_id: Optional[str]
    source_domain: Optional[ArgsMeSourceDomain]
    source_text: Optional[str]
    source_text_conclusion_start: Optional[int]
    source_text_conclusion_end: Optional[int]
    source_text_premise_start: Optional[int]
    source_text_premise_end: Optional[int]
    topic: str  # Topic or discussion title.
    acquisition: datetime
    date: Optional[datetime]
    author: Optional[str]
    author_image_url: Optional[str]
    author_organization: Optional[str]
    author_role: Optional[str]
    mode: Optional[ArgsMeMode]

    def default_text(self):
        """
        premises + conclusion
        """
        return f"{self.premises_texts} {self.conclusion}"

    @staticmethod
    def from_json(json: dict) -> "ArgsMeDoc":
        context_json = json["context"]

        doc_id = str(json["id"])
        conclusion = str(json["conclusion"])

        premises = [
            ArgsMePremise.from_json(premise)
            for premise in json["premises"]
        ]
        premises_texts = " ".join(premise.text for premise in premises)

        aspects = [
            ArgsMeAspect.from_json(aspect)
            for aspect in context_json["aspects"]
        ] if "aspects" in context_json else []
        aspects_names = " ".join(aspect.name for aspect in aspects)

        source_id = str(context_json["sourceId"])
        source_title = str(context_json["sourceTitle"])
        source_url = (
            str(context_json["sourceUrl"])
            if "sourceUrl" in context_json
            else None
        )
        source_previous_argument_id = (
            str(context_json["previousArgumentInSourceId"])
            if ("previousArgumentInSourceId" in context_json and
                context_json["previousArgumentInSourceId"])
            else None
        )
        source_next_argument_id = (
            str(context_json["nextArgumentInSourceId"])
            if ("nextArgumentInSourceId" in context_json and
                context_json["nextArgumentInSourceId"])
            else None
        )
        source_domain = (
            ArgsMeSourceDomain.from_json(context_json["sourceDomain"])
            if "sourceDomain" in context_json
            else None
        )
        source_text = (
            str(context_json["sourceText"])
            if "sourceText" in context_json
            else None
        )
        source_text_conclusion_start = (
            int(context_json["sourceTextConclusionStart"])
            if "sourceTextConclusionStart" in context_json
            else None
        )
        source_text_conclusion_end = (
            int(context_json["sourceTextConclusionEnd"])
            if "sourceTextConclusionEnd" in context_json
            else None
        )
        source_text_premise_start = (
            int(context_json["sourceTextPremiseStart"])
            if "sourceTextPremiseStart" in context_json
            else None
        )
        source_text_premise_end = (
            int(context_json["sourceTextPremiseEnd"])
            if "sourceTextPremiseEnd" in context_json
            else None
        )

        topic = (
            str(context_json["topic"])
            if "topic" in context_json
            else str(context_json["discussionTitle"])
        )
        acquisition = datetime.fromisoformat(
            sub(r"Z$", "+00:00", context_json["acquisitionTime"])
        )
        date = (
            datetime.fromisoformat(
                sub(r"Z$", "+00:00", context_json["date"])
            )
            if "date" in context_json
            else None
        )

        author = (
            str(context_json["author"])
            if "author" in context_json
            else None
        )
        author_image_url = (
            str(context_json["authorImage"])
            if "authorImage" in context_json
            else None
        )
        author_organization = (
            str(context_json["authorOrganization"])
            if "authorOrganization" in context_json
            else None
        )
        author_role = (
            str(context_json["authorRole"])
            if "authorRole" in context_json
            else None
        )
        mode = (
            ArgsMeMode.from_json(context_json["mode"])
            if "mode" in context_json
            else None
        )

        return ArgsMeDoc(
            doc_id=doc_id,
            conclusion=conclusion,
            premises=premises,
            premises_texts=premises_texts,
            aspects=aspects,
            aspects_names=aspects_names,
            source_id=source_id,
            source_title=source_title,
            source_url=source_url,
            source_previous_argument_id=source_previous_argument_id,
            source_next_argument_id=source_next_argument_id,
            source_domain=source_domain,
            source_text=source_text,
            source_text_conclusion_start=source_text_conclusion_start,
            source_text_conclusion_end=source_text_conclusion_end,
            source_text_premise_start=source_text_premise_start,
            source_text_premise_end=source_text_premise_end,
            topic=topic,
            acquisition=acquisition,
            date=date,
            author=author,
            author_image_url=author_image_url,
            author_organization=author_organization,
            author_role=author_role,
            mode=mode,
        )


class ArgsMeProcessedDoc(NamedTuple):
    """
    See the corresponding Java source files from the args.me project:
    https://git.webis.de/code-research/arguana/args/args-framework/-/blob/master/src/main/java/me/args/Argument.java
    https://git.webis.de/code-research/arguana/args/args-framework/-/blob/master/src/main/java/me/args/argument/Premise.java
    """
    doc_id: str
    conclusion: str
    premises: List[ArgsMePremise]
    premises_texts: str  # Premises texts concatenated with spaces.
    aspects: List[ArgsMeAspect]
    aspects_names: str  # Aspects namews concatenated with spaces.
    source_id: str
    source_title: str
    source_url: Optional[str]
    source_previous_argument_id: Optional[str]
    source_next_argument_id: Optional[str]
    source_domain: Optional[ArgsMeSourceDomain]
    source_text: Optional[str]
    source_text_conclusion_start: Optional[int]
    source_text_conclusion_end: Optional[int]
    source_text_premise_start: Optional[int]
    source_text_premise_end: Optional[int]
    topic: str  # Topic or discussion title.
    acquisition: datetime
    date: Optional[datetime]
    author: Optional[str]
    author_image_url: Optional[str]
    author_organization: Optional[str]
    author_role: Optional[str]
    mode: Optional[ArgsMeMode]
    sentences: List[ArgsMeSentence]

    @staticmethod
    def from_csv(csv: dict) -> "ArgsMeProcessedDoc":
        csv["premises"] = literal_eval(csv["premises"])
        csv["context"] = literal_eval(csv["context"])
        doc = ArgsMeDoc.from_json(csv)
        sentences = [
            ArgsMeSentence.from_json(json)
            for json in literal_eval(csv["sentences"])
        ]
        return ArgsMeProcessedDoc(
            *doc,
            sentences=sentences,
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

    @use_docstore
    def docs_iter(self):
        ijson = lazy_libs.ijson()
        with self._source.stream() as json_stream:
            argument_jsons = ijson.items(json_stream, "arguments.item")
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


class ArgsMeProcessedDocs(BaseDocs):
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

    @use_docstore
    def docs_iter(self):
        with self._source.stream() as csv_stream:
            lines = TextIOWrapper(csv_stream)
            field_size_limit(maxsize)
            reader = DictReader(lines)
            for argument_csv in reader:
                argument = ArgsMeProcessedDoc.from_csv(argument_csv)
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
        return ArgsMeProcessedDoc

    def docs_namespace(self):
        return self._namespace

    def docs_lang(self):
        return self._language


class ArgsMeCombinedDocs(BaseDocs):
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

    @use_docstore
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
