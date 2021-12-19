from .base import Dataset
import itertools
import pickle
import json
import shutil
import uuid
from typing import NamedTuple
import ir_datasets
from ir_datasets import EntityType
from ir_datasets.indices import Lz4PickleLookup, PickleLz4FullStore
from ir_datasets.formats import BaseDocs, BaseQueries, BaseQrels, BaseScoredDocs, BaseDocPairs, BaseQlogs

logger = ir_datasets.log.easy()

NAME = 'local'

BASE_PATH = ir_datasets.util.home_path()/NAME


class BaseLocal:
    def __init__(self, path, etype):
        self._path = path
        self._etype = etype
        self._cls_ = None

    def __getattr__(self, attr):
        if attr.startswith(f'{self._etype.value}_'):
            return getattr(self, attr[len(self._etype.value):])
        raise AttributeError(attr)

    def _iter(self):
        cls = self._cls()
        lz4_frame = ir_datasets.lazy_libs.lz4_frame().frame
        with lz4_frame.LZ4FrameFile(self._path/'data', 'rb') as fin:
            while fin.peek(1):
                yield cls(*pickle.load(fin))

    def _count(self):
        with (self._path/'count.pkl').open('rb') as fin:
            return pickle.load(fin)

    def _cls(self):
        if self._cls_ is None:
            with (self._path/'type.pkl').open('rb') as fin:
                name, attrs = pickle.load(fin)
            self._cls_ = NamedTuple(name, list(attrs.items()))
        return self._cls_

    @classmethod
    def create(cls, path, records):
        path.mkdir(exist_ok=True, parents=True)
        records = iter(records)
        first = next(records)
        if isinstance(first, dict):
            EntityCls = NamedTuple('LocalEntity', [(k, type(v)) for k, v in first.items()])
            records = itertools.chain([EntityCls(**first)], (EntityCls(**r) for r in records))
        else:
            EntityCls = type(first)
            records = itertools.chain([first], records)
        with ir_datasets.util.finialized_file(path/'type.pkl', 'wb') as fout:
            pickle.dump((EntityCls.__name__, EntityCls.__annotations__), fout)
        count = cls._build_datafile(path/'data', records, EntityCls)
        with ir_datasets.util.finialized_file(path/'count.pkl', 'wb') as fout:
            pickle.dump(count, fout)

    @classmethod
    def _build_datafile(cls, path, records, ecls):
        count = 0
        lz4_frame = ir_datasets.lazy_libs.lz4_frame().frame
        with ir_datasets.util.finialized_file(path, 'wb') as raw_fout, \
             lz4_frame.LZ4FrameFile(raw_fout, 'wb') as fout:
            for record in records:
                pickle.dump(tuple(record), fout)
                count += 1
        return count


class LocalDocs(BaseLocal, BaseDocs):
    def __init__(self, path):
        super().__init__(path, EntityType.docs)

    def docs_iter(self):
        return iter(self.docs_store())

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{self._path}/data',
            init_iter_fn=None,
            data_cls=self._cls(),
            lookup_field='doc_id',
            index_fields=['doc_id'],
            count_hint=self._count,
        )

    def docs_count(self):
        return len(self.docs_store())

    def docs_cls(self):
        return self._cls()

    @classmethod
    def _build_datafile(cls, path, records, ecls):
        lookup = Lz4PickleLookup(path, ecls, 'doc_id', ['doc_id'])
        with lookup.transaction() as trans:
            for doc in records:
                trans.add(doc)


class LocalQueries(BaseLocal, BaseQueries):
    def __init__(self, path):
        super().__init__(path, EntityType.queries)


class LocalQrels(BaseLocal, BaseQrels):
    def __init__(self, path):
        super().__init__(path, EntityType.qrels)

    def qrels_defs(self):
        return {}


class LocalScoredDocs(BaseLocal, BaseScoredDocs):
    def __init__(self, path):
        super().__init__(path, EntityType.scoreddocs)


class LocalDocpairs(BaseLocal, BaseDocPairs):
    def __init__(self, path):
        super().__init__(path, EntityType.docpairs)


class LocalQlogs(BaseLocal, BaseQlogs):
    def __init__(self, path):
        super().__init__(path, EntityType.qlogs)


PROVIDERS = {
    EntityType.docs: LocalDocs,
    EntityType.queries: LocalQueries,
    EntityType.qrels: LocalQrels,
    EntityType.scoreddocs: LocalScoredDocs,
    EntityType.docpairs: LocalDocpairs,
    EntityType.qlogs: LocalQlogs,
}


def create_local_dataset(dataset_id, **sources):
    if dataset_id in ir_datasets.registry:
        raise KeyError(f'{dataset_id} already in registry; choose another name')
    path = str(uuid.uuid4())
    ds_path = BASE_PATH/path
    components = []
    dataset_record = {
        'id': dataset_id,
        'path': path,
        'provides': {},
    }
    with logger.duration(f'provisioning {dataset_id} to {ds_path}'):
        ds_path.mkdir(exist_ok=True, parents=True)
        for etype in EntityType:
            if etype.value not in sources:
                continue
            source = sources[etype.value]
            if isinstance(source, str):
                components.append(ir_datasets.load(source).handler(etype))
                dataset_record['provides'][etype.value] = source
            else:
                e_path = ds_path/etype.value
                provider_cls = PROVIDERS[EntityType(etype)]
                with logger.duration(f'creating {str(e_path)}'):
                    provider_cls.create(e_path, source)
                components.append(provider_cls(e_path))
                dataset_record['provides'][etype.value] = None
            del sources[etype.value]
    if sources:
        raise RuntimeError(f'Unexpected argument(s): {sources.keys()}')
    dataset = Dataset(*components)
    ir_datasets.registry.register(dataset_id, dataset)
    registry_path = (BASE_PATH/'registry.json')
    if registry_path.exists():
        with registry_path.open('rt') as fin:
            registry = json.load(fin)
    else:
        registry = []
    registry.append(dataset_record)
    with ir_datasets.util.finialized_file(registry_path, 'wt') as fout:
        json.dump(registry, fout)
    return dataset


def delete_local_dataset(dataset_id, remove_files=True):
    registry_paths = list(BASE_PATH.glob('registry*.json'))
    for registry_path in sorted(registry_paths):
        with registry_path.open('rt') as fin:
            registry = json.load(fin)
        changed = False
        for dataset in list(registry):
            if dataset['id'] == dataset_id:
                registry.remove(dataset)
                changed = True
                try:
                    del ir_datasets.registry[dataset_id]
                except KeyError:
                    pass
            if remove_files:
                with logger.duration(f'Removing {dataset["id"]} at {str(BASE_PATH/dataset["path"])}'):
                    shutil.rmtree(BASE_PATH/dataset['path'])
            if changed:
                with ir_datasets.util.finialized_file(registry_path, 'wt') as fout:
                    json.dump(registry, fout)


def iter_local_datasets():
    registry_paths = list(BASE_PATH.glob('registry*.json'))
    for registry_path in sorted(registry_paths):
        with registry_path.open('rt') as fin:
            registry = json.load(fin)
        changed = False
        for dataset in list(registry):
            dataset['registry_path'] = str(registry_path)
            yield dataset


def _init():
    for dataset in iter_local_datasets():
        if dataset['id'] in ir_datasets.registry:
            new_id = f'local/{dataset["path"]}'
            logger.warn(f'Local dataset {repr(dataset["id"])} from {dataset["registry_path"]} already in registry. '
                        f'Renaming it to {new_id}.')
            dataset['id'] = new_id
        ds_path = BASE_PATH/dataset['path']
        components = []
        for etype, val in dataset['provides'].items():
            if val is None:
                provider_cls = PROVIDERS[EntityType(etype)]
                components.append(provider_cls(ds_path/etype))
            else:
                components.append(ir_datasets.load(val).handler(etype))
        ir_datasets.registry.register(dataset['id'], Dataset(*components))

_init()
