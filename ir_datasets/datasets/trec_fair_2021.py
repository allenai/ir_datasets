import json
import mwparserfromhell
import codecs
from typing import NamedTuple, Dict
import ir_datasets
from ir_datasets.util import GzipExtract, Cache, Lazy
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries
from ir_datasets.formats import BaseQueries, BaseDocs, BaseQrels, TrecQrel
from ir_datasets.indices import PickleLz4FullStore
import pyautocorpus
from itertools import chain


_logger = ir_datasets.log.easy()


NAME = 'trec_fair_2021'


class FairTrecDoc(NamedTuple):
    doc_id: str
    title: str
    text: str
    markupfreetext: str
    url: str
    quality_score: str
    geographic_locations: str
    quality_score_disk: str


class FairTrecQuery(NamedTuple):
    query_id: str
    text: str

#    metadata: Dict[str, str]


class FairTrecDocs(BaseDocs):
    def __init__(self, name, dlc,mlc):
        super().__init__()
        self._name = name
        self._dlc = dlc
        self._mlc = mlc

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        data2 = {}
        with self._mlc.stream() as stream2:
            
            for line in stream2:
                dataContx = json.loads(line) 
                data2[dataContx["page_id"]] = dataContx
                
                
                
        
        with self._dlc.stream() as stream1 :
            textifier = pyautocorpus.Textifier()
            for line1 in stream1:
                data1 = json.loads(line1)
               
                
                match = data2.get(int(data1["id"]))
              
                try:
                    if match:
                    
                        plaintext = ""
                        plaintext = textifier.textify(data1['text'])
                        yield FairTrecDoc(data1['id'], data1['title'],data1['text'],plaintext, data1['url'], str(match['quality_score']), match['geographic_locations'], str(match['quality_score_disc']))
                    else: 
                   
                        plaintext = ""
                        plaintext = textifier.textify(data1['text'])
                        yield FairTrecDoc(data1['id'], data1['title'],data1['text'],plaintext, data1['url'], "NA","NA","NA")
                except ValueError as err:
                    print("[{} - {}]: {}".format(data1['id'], data1['title'], err))


  
              

    def docs_cls(self):
        return FairTrecDoc

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME/self._name}/docs.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field, 
            index_fields=['doc_id']
        )

    def docs_count(self):
        return self.docs_store().count()

    def docs_namespace(self):
        return f'{NAME}/{self._name}'

    def docs_lang(self):
        return 'en'


class FairTrecQueries(BaseQueries):
    def __init__(self, name, dlc, keep_metadata=None):
        super().__init__()
        self._name = name
        self._dlc = dlc
        self._keep_metadata = keep_metadata

    def queries_iter(self):
        with self._dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                yield FairTrecQuery(data['id'], data['title'])

    def queries_cls(self):
        return FairTrecQuery

    def queries_namespace(self):
        return f'{NAME}/{self._name}'

    def queries_lang(self):
        return 'en'


class FairTrecQrels(BaseQrels):
    def __init__(self, qrels_dlc):
        self._qrels_dlc = qrels_dlc
       

    def qrels_path(self):
        return self._qrels_dlc.path()


    def qrels_iter(self):
        with self._qrels_dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                for rlDoc in data["rel_docs"]:
                    yield TrecQrel(data["id"], rlDoc, 1,0)



    def qrels_cls(self):
        return TrecQrel

    def qrels_defs(self):
        return self._qrels_defs


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    base = Dataset(documentation('_'))

  
    dataset = Dataset(
            FairTrecDocs('docs', GzipExtract(dlc["docs"]), GzipExtract(dlc["metadata"])),
            FairTrecQueries('queries', Cache(GzipExtract(dlc["topics"]), base_path/'trec_fair_2021'/'queries.json')),
            FairTrecQrels(GzipExtract(dlc["topics"])),
            documentation('trec_fair_2021'))

    ir_datasets.registry.register(NAME, dataset)


    return dataset

#def qid_filter(subset_qrels):
#    # NOTE: this must be in a separate function otherwise there can be weird lambda binding problems
#    return Lazy(lambda: {q.query_id for q in subset_qrels.qrels_iter()})


base = _init()
