from collections import namedtuple
import ir_datasets
from ir_datasets.util import DownloadConfig, TarExtract, ReTar
from ir_datasets.formats import TrecQrels, BaseDocs, BaseQueries, GenericDoc
from ir_datasets.datasets.base import Dataset, YamlDocumentation


NAME = 'car-v1.5'


AUTO_QRELS = {
    1: 'Paragraph appears under heading'
}


MANUAL_QRELS = {
    3: 'MUST be mentioned',
    2: 'SHOULD be mentioned',
    1: 'CAN be mentioned',
    0: 'Non-relevant, but roughly on TOPIC',
    -1: 'NO, non-relevant',
    -2: 'Trash',
}


CarQuery = namedtuple('CarQuery', ['query_id', 'text', 'title', 'headings'])



class CarDocs(BaseDocs):
    def __init__(self, streamer):
        super().__init__()
        self._streamer = streamer

    def docs_iter(self):
        trec_car = ir_datasets.lazy_libs.trec_car()
        with self._streamer.stream() as stream:
            paras = trec_car.read_data.iter_paragraphs(stream)
            for p in paras:
                yield GenericDoc(p.para_id, p.get_text())


class CarQueries(BaseQueries):
    def __init__(self, streamer):
        super().__init__()
        self._streamer = streamer

    def queries_iter(self):
        trec_car = ir_datasets.lazy_libs.trec_car()
        with self._streamer.stream() as stream:
            for page in trec_car.read_data.iter_outlines(stream):
                for heads in page.flat_headings_list():
                    qid = '/'.join([page.page_id] + [h.headingId for h in heads])
                    title = page.page_name
                    headings = tuple(h.heading for h in heads)
                    text = ' '.join((title,) + headings)
                    yield CarQuery(qid, text, title, headings)


def _init():
    subsets = {}
    base_path = ir_datasets.util.cache_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    docs_v15 = CarDocs(TarExtract(dlc['docs'], 'paragraphcorpus/paragraphcorpus.cbor', compression='xz'))
    base = Dataset(docs_v15, documentation('_'))

    subsets['trec-y1'] = Dataset(
        docs_v15,
        CarQueries(TarExtract(dlc['trec-y1/queries'], 'benchmarkY1test.public/test.benchmarkY1test.cbor.outlines', compression='xz')),)
    subsets['trec-y1/manual'] = Dataset(
        subsets['trec-y1'],
        TrecQrels(TarExtract(dlc['trec-y1/qrels'], 'TREC_CAR_2017_qrels/manual.benchmarkY1test.cbor.hierarchical.qrels'), MANUAL_QRELS))
    subsets['trec-y1/auto'] = Dataset(
        subsets['trec-y1'],
        TrecQrels(TarExtract(dlc['trec-y1/qrels'], 'TREC_CAR_2017_qrels/automatic.benchmarkY1test.cbor.hierarchical.qrels'), AUTO_QRELS))

    subsets['test200'] = Dataset(
        docs_v15,
        CarQueries(TarExtract(dlc['test200'], 'test200/train.test200.cbor.outlines', compression='xz')),
        TrecQrels(TarExtract(dlc['test200'], 'test200/train.test200.cbor.hierarchical.qrels', compression='xz'), AUTO_QRELS))

    train_data = ReTar(dlc['train'], base_path/'train.smaller.tar.xz', ['train/train.fold?.cbor.outlines', 'train/train.fold?.cbor.hierarchical.qrels'], compression='xz')
    subsets['train/fold0'] = Dataset(
        docs_v15,
        CarQueries(TarExtract(train_data, 'train/train.fold0.cbor.outlines', compression='xz')),
        TrecQrels(TarExtract(train_data, 'train/train.fold0.cbor.hierarchical.qrels', compression='xz'), AUTO_QRELS))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', Dataset(subsets[s], documentation(s)))

    return base, subsets


base, subsets = _init()
