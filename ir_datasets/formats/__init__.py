from .base import GenericDoc, GenericQuery, GenericQrel, GenericScoredDoc, GenericDocPair, DocstoreBackedDocs, DocSourceSeekableIter, DocSource, SourceDocIter, BaseQlogs
from .base import BaseDocs, BaseQueries, BaseQrels, BaseScoredDocs, BaseDocPairs
from .csv_fmt import CsvDocs, CsvQueries, CsvDocPairs
from .tsv import TsvDocs, TsvQueries, TsvDocPairs
from .trec import TrecDocs, TrecQueries, TrecXmlQueries, TrecColonQueries, TrecQrels, TrecSubQrels, TrecPrels, TrecScoredDocs, TrecDoc, TitleUrlTextDoc, TrecQuery, TrecSubtopic, TrecQrel, TrecSubQrel, TrecPrel, TrecParsedDoc
from .touche import ToucheQuery, ToucheTitleQuery, ToucheComparativeQuery, ToucheCausalQuery, ToucheQualityQrel, ToucheQualityCoherenceQrel, ToucheComparativeStance, ToucheQualityComparativeStanceQrel, ToucheControversialStance, ToucheControversialStanceQrel, TouchePassageDoc, ToucheQueries, ToucheTitleQueries, ToucheComparativeQueries, ToucheCausalQueries, ToucheQrels, ToucheQualityQrels, ToucheQualityCoherenceQrels, ToucheQualityComparativeStanceQrels, ToucheControversialStanceQrels, TouchePassageDocs
from .touche_image import ToucheImageRanking, ToucheImageNode, ToucheImagePage, ToucheImageDoc, ToucheImageDocs
from .webarc import WarcDocs, WarcDoc
from .ntcir import NtcirQrels
from .clirmatrix import CLIRMatrixQueries, CLIRMatrixQrels
from .argsme import ArgsMeStance, ArgsMeMode, ArgsMeSourceDomain, ArgsMePremiseAnnotation, ArgsMePremise, ArgsMeAspect, ArgsMeDoc, ArgsMeProcessedDoc, ArgsMeDocs, ArgsMeProcessedDocs, ArgsMeCombinedDocs
from .extracted_cc import ExctractedCCDoc, ExctractedCCDocs, ExctractedCCQuery, ExctractedCCQueries
from .jsonl import JsonlDocs, JsonlQueries
from .clueweb22 import ClueWeb22Docs, ClueWeb22Docstore, ClueWeb22LDoc, ClueWeb22ADoc, ClueWeb22BDoc, ClueWeb22Subset, ClueWeb22Language, ClueWeb22Format, ClueWeb22Compression