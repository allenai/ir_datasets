import json
from typing import NamedTuple
import ir_datasets
from ir_datasets.util import DownloadConfig
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import TsvQueries
from ir_datasets.datasets.msmarco_passage import DUA
from ir_datasets.datasets.msmarco_passage_v2 import MsMarcoV2Passages

_logger = ir_datasets.log.easy()

NAME = 'msmarco-segment-v2.1'


class MsMarcoV21SegmentedDoc(NamedTuple):
    doc_id: str
    url: str
    title: str
    headings: str
    segment: str
    start_char: int
    end_char: int
    def default_text(self):
        """
        title + headings + segment
        This is consistent with the MsMarcoV21Document that returns the full text alternative of this: title + headings + body
        Please note that Anserini additionaly returns the url. I.e., anserini returns url + title + headings + segment
        E.g., https://github.com/castorini/anserini/blob/b8ce19f56bc4e85056ef703322f76646804ec640/src/main/java/io/anserini/collection/MsMarcoV2DocCollection.java#L169
        """
        return f'{self.title} {self.headings} {self.segment}'


def parse_msmarco_segment(line):
    data = json.loads(line)
    return MsMarcoV21SegmentedDoc(
        data['docid'],
        data['url'],
        data['title'],
        data['headings'],
        data['segment'],
        data['start_char'],
        data['end_char']
    )


def passage_bundle_pos_from_key(key):
    # key like: msmarco_v2.1_doc_00_0#4_5974
    first, second = key.split('#')
    (string1, string2, string3, bundle, doc_pos) = first.split('_')
    (segment_num, segment_pos) = first.split('_')
    assert string1 == 'msmarco' and string2 == 'v2.1' and string3 == 'doc'
    return f'msmarco_v2.1_doc_segmented_{bundle}.json', segment_pos


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    dlc = DownloadConfig.context(NAME, base_path, dua=DUA)
    collection = MsMarcoV2Passages(dlc['docs'], cls=MsMarcoV21SegmentedDoc, parse_passage=parse_msmarco_segment, name=NAME, bundle_pos_from_key=passage_bundle_pos_from_key, count=113_520_750)
    subsets = {}
    subsets['trec-rag-2024'] = Dataset(
        collection,
        TsvQueries(dlc['rag-2024-test-topics'], namespace=NAME, lang='en'),
    )

    ir_datasets.registry.register(NAME, Dataset(collection, documentation('_')))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', Dataset(subsets[s], documentation(s)))
    
    return collection, subsets

collection, subsets = _init()
