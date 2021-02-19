from .base import GenericDoc, GenericQuery, GenericQrel, GenericScoredDoc, GenericDocPair, DocstoreBackedDocs
from .base import BaseDocs, BaseQueries, BaseQrels, BaseScoredDocs, BaseDocPairs
from .csv_fmt import CsvDocs, CsvQueries, CsvDocPairs
from .tsv import TsvDocs, TsvQueries, TsvDocPairs
from .trec import TrecDocs, TrecQueries, TrecXmlQueries, TrecColonQueries, TrecQrels, TrecScoredDocs, TrecDoc, TrecQuery, TrecSubtopic, TrecQrel
from .webarc import WarcDocs, WarcDoc
from .ntcir import NtcirQrels
