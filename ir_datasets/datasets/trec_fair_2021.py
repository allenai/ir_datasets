import json
import codecs
from typing import NamedTuple, Dict
import ir_datasets
from ir_datasets.util import GzipExtract, Cache, Lazy
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries
from ir_datasets.formats import BaseQueries, BaseDocs, BaseQrels, TrecQrel
from ir_datasets.indices import PickleLz4FullStore
from itertools import chain


_logger = ir_datasets.log.easy()


NAME = 'trec-fair-2021'


class FairTrecDoc(NamedTuple):
    doc_id: str
    title: str
    text: str
    markupfreetext: str
    url: str
    quality_score: Optional[float]
    geographic_locations: Optional[List(str)]
    quality_score_disk: Optional[str]


class FairTrecQuery(NamedTuple):
    query_id: str
    text: str
    keywords: list(str)
    scope: str
    homepage: str

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
                if match:
                        try:
                            plaintext = textifier.textify(data1['text'])
                            yield FairTrecDoc(data1['id'], data1['title'],data1['text'],plaintext, data1['url'], str(match['quality_score']), match['geographic_locations'], str(match['quality_score_disc']))

                        except ValueError as err:
                            message, position = err.args
                            if message == "Expected markup type 'comment'":
                                # unmatched <!-- comment tag
                                # The way Wikipedia renders this is it cuts the article off at this point.
                                # We'll follow that here, given it's only 22 articles of the 6M.
                                # (Note: the position is a byte offset, so that's why it encodes/decodes.)
                                plaintext = textifier.textify(data['text'].encode()[:position].decode())
                                yield FairTrecDoc(data1['id'], data1['title'],data1['text'],plaintext, data1['url'], str(match['quality_score']), match['geographic_locations'], str(match['quality_score_disc']))
                            else:
                                raise 
                   
                else: 
                    try:
                            plaintext = textifier.textify(data1['text'])
                            yield FairTrecDoc(data1['id'], data1['title'],data1['text'],plaintext, data1['url'], "None", "None", "None")

                    except ValueError as err:
                        message, position = err.args
                        if message == "Expected markup type 'comment'":
                                # unmatched <!-- comment tag
                                # The way Wikipedia renders this is it cuts the article off at this point.
                                # We'll follow that here, given it's only 22 articles of the 6M.
                                # (Note: the position is a byte offset, so that's why it encodes/decodes.)
                            plaintext = textifier.textify(data['text'].encode()[:position].decode())
                            yield FairTrecDoc(data1['id'], data1['title'],data1['text'],plaintext, data1['url'], "None", "None", "None")
                        else:
                            raise 

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
                yield FairTrecQuery(data['id'], data['title'], data["keywords"], data["scope"], data["homepage"])


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
    datasets = {"trec-fair-2021" :"dev" } #here you can add the eval queries later on 
    subsets = {}
    
    for ds, qrels in datasets.items():
        subsets[ds] = Dataset(
                FairTrecDocs('docs', GzipExtract(dlc["docs"]), GzipExtract(dlc["metadata"])),
                FairTrecQueries('queries', Cache(GzipExtract(dlc[str(qrels) + "/topics"]), base_path/'trec-fair-2021'+ str(qrels)/'queries.json')),
                FairTrecQrels(GzipExtract(dlc[str(qrels)+"topics"])),
                documentation('trec-fair-2021'))

        for s in sorted(subsets):
            
            ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])


    return dataset


base = _init()
