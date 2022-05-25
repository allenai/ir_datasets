from .base import GenericDoc, GenericQuery, GenericQrel, GenericScoredDoc, GenericDocPair, DocstoreBackedDocs, DocSourceSeekableIter, DocSource, SourceDocIter, BaseQlogs
from .base import BaseDocs, BaseQueries, BaseQrels, BaseScoredDocs, BaseDocPairs
from .csv_fmt import CsvDocs, CsvQueries, CsvDocPairs
from .tsv import TsvDocs, TsvQueries, TsvDocPairs
from .trec import TrecDocs, TrecQueries, TrecXmlQueries, TrecColonQueries, TrecQrels, TrecPrels, TrecScoredDocs, TrecDoc, TitleUrlTextDoc, TrecQuery, TrecSubtopic, TrecQrel, TrecPrel, TrecParsedDoc
from .touche import (
    ToucheQueries, ToucheQuery, ToucheTitleQuery, ToucheQrels
)
from .webarc import WarcDocs, WarcDoc
from .ntcir import NtcirQrels
from .clirmatrix import CLIRMatrixQueries, CLIRMatrixQrels
from .argsme import (
    ArgsMeDocs, ArgsMeDoc, ArgsMeStance, ArgsMeMode, ArgsMeSourceDomain,
    ArgsMePremise, ArgsMePremiseAnnotation, ArgsMeAspect
)

from .extracted_cc import ExctractedCCDoc, ExctractedCCDocs, ExctractedCCQuery, ExctractedCCQueries
from .jsonl import JsonlDocs
