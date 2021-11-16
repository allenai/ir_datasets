from typing import NamedTuple, Any, Optional
from xml.etree.ElementTree import parse, Element, ElementTree

from ir_datasets.formats import BaseQueries


class ToucheQuery(NamedTuple):
    query_id: str
    title: str
    description: str
    narrative: str


class ToucheSimpleQuery(NamedTuple):
    query_id: str
    title: str


class ToucheQueries(BaseQueries):
    _source: Any
    _namespace: Optional[str]
    _language: Optional[str]
    _has_description: bool

    def __init__(
            self,
            source: Any,
            namespace: Optional[str] = None,
            language: Optional[str] = None,
            has_description: bool = True,
    ):
        self._source = source
        self._namespace = namespace
        self._language = language
        self._has_description = has_description

    def queries_path(self):
        return self._source.path()

    def queries_iter(self):
        with self._source.stream() as file:
            tree: ElementTree = parse(file)
            root: Element = tree.getroot()
            assert root.tag == "topics"

            for element in root:
                element: Element
                assert element.tag == "topic"
                number = int(element.findtext("number"))
                title = element.findtext("title")

                if self._has_description:
                    description = element.findtext("description")
                    narrative = element.findtext("narrative")
                    yield ToucheQuery(
                        str(number),
                        title,
                        description,
                        narrative,
                    )
                else:
                    yield ToucheSimpleQuery(
                        str(number),
                        title,
                    )

    def queries_cls(self):
        return ToucheQuery if self._has_description else ToucheSimpleQuery

    def queries_namespace(self):
        return self._namespace

    def queries_lang(self):
        return self._language
