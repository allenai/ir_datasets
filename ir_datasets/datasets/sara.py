import ir_datasets
from ir_datasets.formats import TsvDocs, TsvQueries, TrecQrels
from ir_datasets.util import DownloadConfig, TarExtract, Cache
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.datasets.base import Dataset, FilteredQueries, FilteredQrels, YamlDocumentation
from ir_datasets.formats import BaseDocs, TrecXmlQueries, TrecQrels, GenericQuery, GenericQrel
from ir_datasets.indices import PickleLz4FullStore
from typing import NamedTuple, Tuple
import itertools
import io
from ir_datasets.util import GzipExtract, Cache, Lazy, TarExtractAll, RelativePath
import os
import re
import logging
import glob
import tarfile

# Utility functions

def filenames_from_cat(top,second, directory):
    cats_globs = sorted(glob.glob(directory + '/*/*/*.cats'))
    matches = []
    for g in cats_globs:
        with open(g) as file:
            for line in file:
                line = line.split()[0]
                line = line.split(",")
                if int(line[0]) == top and int(line[1]) == second:
                    dir = os.path.basename(g)
                    filename = os.path.splitext(dir)[0]
                    matches.append(filename)
    return matches


def save_email_from_filename(filename, directory):
    full_filename = sorted(glob.glob(directory +'/*/*/' + str(filename) + '.txt'))
    email_contents = ""
    with open(full_filename[0]) as file:
        for line in file:
            email_contents = email_contents + line
    return email_contents


def list_all_filenames(directory):
    globs = sorted(glob.glob(directory +'/*/*/*.cats'))
    filenames = []
    for file in globs:
        filenames.append(os.path.splitext(os.path.basename(file))[0])
    return filenames


# A unique identifier for this dataset. This should match the file name (with "-" instead of "_")
NAME = "sara"

# What do the relevance levels in qrels mean?
QREL_DEFS = {
    2: 'highly relevant',
    1: 'partially relevant',
    0: 'not relevant',
}

class SaraDoc(NamedTuple):
    doc_id: str
    text: str
    sensitivity: int

class SaraDocs(BaseDocs):
    def __init__(self,dlc):
        super().__init__()
        self._dlc = dlc

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        directory = str(self._dlc.path())
        # Partition emails into sensitive and non-sensitive
        sensitive_filenames = []
        # Assume 'Purely personal' and 'Personal but in a professional context' are the sensitive categories
        sensitive_filenames.append(filenames_from_cat(1,2, directory))
        sensitive_filenames.append(filenames_from_cat(1,3, directory))

        # Flatten the list
        sensitive_filenames = [j for sub in sensitive_filenames for j in sub]
        # Remove duplicates - 3 emails are counted in both categories
        sensitive_filenames = list(dict.fromkeys(sensitive_filenames))

        non_sensitive_filenames = []
        for name in list_all_filenames(directory):
            if name not in sensitive_filenames:
                non_sensitive_filenames.append(name)

        for filename in sensitive_filenames:
            email = save_email_from_filename(filename, directory)
            yield SaraDoc(filename,email,1)

        for filename in non_sensitive_filenames:
            email = save_email_from_filename(filename, directory)
            yield SaraDoc(filename,email,0)

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME}/enron_with_categories/enron_with_categories/docs.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=ir_datasets.util.count_hint(NAME),
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'

    def docs_cls(self):
        return SaraDoc

# An initialization function is used to keep the namespace clean
def _init():
    base_path = ir_datasets.util.home_path()/NAME

    # # Load an object that is used for providing the documentation
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    dlc = DownloadConfig.context(NAME, base_path)

    docs = SaraDocs(TarExtractAll(dlc["docs"], base_path/"enron_with_categories/"))

    queries = TsvQueries(dlc['queries'], namespace=NAME, lang='en')

    qrels = TrecQrels(dlc['qrels'], QREL_DEFS)

    # Package the docs, queries, qrels, and documentation into a Dataset object
    dataset = Dataset(docs, queries, qrels, documentation('_'))

    # Register the dataset in ir_datasets
    ir_datasets.registry.register(NAME, dataset)

    return dataset # used for exposing dataset to the namespace


_init()
