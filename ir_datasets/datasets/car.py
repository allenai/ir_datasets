from typing import NamedTuple, Tuple
import ir_datasets
from ir_datasets.util import DownloadConfig, TarExtract, ReTar
from ir_datasets.formats import TrecQrels, BaseDocs, BaseQueries, GenericDoc
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.indices import PickleLz4FullStore


NAME = 'car'


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


class CarQuery(NamedTuple):
    query_id: str
    text: str
    title: str
    headings: Tuple[str, ...]


class CarDocs(BaseDocs):
    def __init__(self, streamer):
        super().__init__()
        self._streamer = streamer

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        trec_car = ir_datasets.lazy_libs.trec_car()
        with self._streamer.stream() as stream:
            paras = trec_car.read_data.iter_paragraphs(stream)
            for p in paras:
                yield GenericDoc(p.para_id, p.get_text())

    def docs_cls(self):
        return GenericDoc

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME}/docs.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
        )

    def docs_count(self):
        return self.docs_store().count()

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'

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

    def queries_namespace(self):
        return NAME

    def queries_cls(self):
        return CarQuery

    def queries_lang(self):
        return 'en'

def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    docs_v15 = CarDocs(TarExtract(dlc['docs'], 'paragraphcorpus/paragraphcorpus.cbor', compression='xz'))
    base = Dataset(documentation('_'))

    subsets['v1.5'] = Dataset(docs_v15, documentation('v1.5'))

    subsets['v1.5/trec-y1'] = Dataset(
        docs_v15,
        CarQueries(TarExtract(dlc['trec-y1/queries'], 'benchmarkY1test.public/test.benchmarkY1test.cbor.outlines', compression='xz')),)
    subsets['v1.5/trec-y1/manual'] = Dataset(
        subsets['v1.5/trec-y1'],
        TrecQrels(TarExtract(dlc['trec-y1/qrels'], 'TREC_CAR_2017_qrels/manual.benchmarkY1test.cbor.hierarchical.qrels'), MANUAL_QRELS))
    subsets['v1.5/trec-y1/auto'] = Dataset(
        subsets['v1.5/trec-y1'],
        TrecQrels(TarExtract(dlc['trec-y1/qrels'], 'TREC_CAR_2017_qrels/automatic.benchmarkY1test.cbor.hierarchical.qrels'), AUTO_QRELS))

    subsets['v1.5/test200'] = Dataset(
        docs_v15,
        CarQueries(TarExtract(dlc['test200'], 'test200/train.test200.cbor.outlines', compression='xz')),
        TrecQrels(TarExtract(dlc['test200'], 'test200/train.test200.cbor.hierarchical.qrels', compression='xz'), AUTO_QRELS))

    train_data = ReTar(dlc['train'], base_path/'train.smaller.tar.xz', ['train/train.fold?.cbor.outlines', 'train/train.fold?.cbor.hierarchical.qrels'], compression='xz')
    subsets['v1.5/train/fold0'] = Dataset(
        docs_v15,
        CarQueries(TarExtract(train_data, 'train/train.fold0.cbor.outlines', compression='xz')),
        TrecQrels(TarExtract(train_data, 'train/train.fold0.cbor.hierarchical.qrels', compression='xz'), AUTO_QRELS))
    subsets['v1.5/train/fold1'] = Dataset(
        docs_v15,
        CarQueries(TarExtract(train_data, 'train/train.fold1.cbor.outlines', compression='xz')),
        TrecQrels(TarExtract(train_data, 'train/train.fold1.cbor.hierarchical.qrels', compression='xz'), AUTO_QRELS))
    subsets['v1.5/train/fold2'] = Dataset(
        docs_v15,
        CarQueries(TarExtract(train_data, 'train/train.fold2.cbor.outlines', compression='xz')),
        TrecQrels(TarExtract(train_data, 'train/train.fold2.cbor.hierarchical.qrels', compression='xz'), AUTO_QRELS))
    subsets['v1.5/train/fold3'] = Dataset(
        docs_v15,
        CarQueries(TarExtract(train_data, 'train/train.fold3.cbor.outlines', compression='xz')),
        TrecQrels(TarExtract(train_data, 'train/train.fold3.cbor.hierarchical.qrels', compression='xz'), AUTO_QRELS))
    subsets['v1.5/train/fold4'] = Dataset(
        docs_v15,
        CarQueries(TarExtract(train_data, 'train/train.fold4.cbor.outlines', compression='xz')),
        TrecQrels(TarExtract(train_data, 'train/train.fold4.cbor.hierarchical.qrels', compression='xz'), AUTO_QRELS))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', Dataset(subsets[s], documentation(s)))

    return base, subsets


base, subsets = _init()
