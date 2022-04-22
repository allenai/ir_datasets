import io
import itertools
import ir_datasets
from typing import NamedTuple
from ir_datasets.util import DownloadConfig, TarExtract, Cache
from ir_datasets.formats import BaseDocs, BaseQueries, BaseQrels
from ir_datasets.formats import TsvDocs
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.datasets.base import Dataset, YamlDocumentation


NAME = 'wands'


QREL_DEFS = {
    2: 'Exact match: The surfaced product fully matches the search query.',
    1: 'Partial match: The surfaced product does not fully match the search query. It only matches the target entity of the query, but does not satisfy the modifiers for the query.',
    0: 'Irrelevant: The product is not relevant to the query.',
}


class WandsDoc(NamedTuple):
    doc_id: str
    name: str
    product_class: str
    category_hierarchy: str
    product_description: str
    product_features: str
    rating_count: int
    average_rating: float
    review_count: int


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    base = Dataset(
        TsvDocs(dlc['docs'], doc_cls=WandsDoc, namespace=NAME, lang='en', count_hint=ir_datasets.util.count_hint(NAME), skip_first_line=True, cast_types=False),
        documentation('_'),
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
