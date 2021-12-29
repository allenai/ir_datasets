import io
import codecs
import re
import ir_datasets
from typing import NamedTuple
from ir_datasets.util import DownloadConfig, ZipExtractCache, RelativePath
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries
from ir_datasets.datasets import msmarco_passage
from ir_datasets.formats import BaseActions

NAME = 'movielens'


class MovieLensAction(NamedTuple):
    user_id: str
    item_id: str
    timestamp: int
    rating: int


class MovieLensUser(NamedTuple):
    user_id: str
    age: int
    gender: str
    occupation: str
    zipcode: str


class MovieLensActions(BaseActions):
    def __init__(self, dlc):
        super().__init__()
        self._dlc = dlc

    def actions_iter(self):
        with self._dlc.stream() as stream:
            for line in stream:
                user_id, item_id, rating, timestamp = line.decode().strip().split('\t')
                yield MovieLensAction(user_id, item_id, int(rating), int(timestamp))

    def actions_cls(self):
        return MovieLensAction


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)

    subsets = {}

    main_dlc = ZipExtractCache(dlc['100k'], base_path/'100k')

    users100k = None
    items100k = None

    subsets['100k'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/u.data')),
    )

    subsets['100k/fold1/train'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/u1.base')),
    )
    subsets['100k/fold1/test'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/u1.test')),
    )

    subsets['100k/fold2/train'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/u2.base')),
    )
    subsets['100k/fold2/test'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/u2.test')),
    )

    subsets['100k/fold3/train'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/u3.base')),
    )
    subsets['100k/fold3/test'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/u3.test')),
    )

    subsets['100k/fold4/train'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/u4.base')),
    )
    subsets['100k/fold4/test'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/u4.test')),
    )

    subsets['100k/fold5/train'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/u5.base')),
    )
    subsets['100k/fold5/test'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/u5.test')),
    )

    subsets['100k/split-a/train'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/ua.base')),
    )
    subsets['100k/split-a/test'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/ua.test')),
    )

    subsets['100k/split-b/train'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/ub.base')),
    )
    subsets['100k/split-b/test'] = Dataset(
        users100k,
        items100k,
        MovieLensActions(RelativePath(main_dlc, 'ml-100k/ub.test')),
    )

    ir_datasets.registry.register(NAME, Dataset(documentation('_')))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', Dataset(subsets[s], documentation(s)))

    return subsets


subsets = _init()
