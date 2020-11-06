# ir_datasets

`ir_datasets` is a python package that provides a common interface to many IR ad-hoc ranking
benchmarks, training datasets, etc. It was built as a fork of [OpenNIR](https://opennir.net) to
allow easier integration with other systems.

The package takes care of downloading datasets (including documents, queries, relevance judgments,
etc.) when available from public sources. Instructions on how to obtain datasets are provided when
they are not publicly available.

`ir_datasets` provides a common iterator format to allow them to be easily used in python. It
attempts to provide the data in an unaltered form (i.e., keeping all fields and markup), while
handling differences in file formats, encoding, etc. Adapters provide extra functionality, e.g., to
allow quick lookups of documents by ID.

A command line interface is also available.

Want a new dataset, added functionality, or a bug fixed? Feel free to post an issue or make a pull
request! 

## Getting Started

Install via pip using:

```
pip install ir_datasets
```

or locally with:

```
python setup.py bdist_wheel
pip install dist/ir_datasets-*.whl
```

## Python Interface

Load a dataset using:
```
import ir_datasets
dataset = ir_datasets.load('dataset-id')
```

A dataset object lets you iterate through supported properties like docs (`dataset.docs_iter()`),
queries (`dataset.queries_iter()`), and relevance judgments (`dataset.qrels_iter()`). Each iterator
yields namedtuples, with fields based on the available data.

If you want to use your own dataset, you can construct an object with the same interface as the
standard benchmarks by:
```
import ir_datasets
dataset = ir_datasets.create_dataset(
  docs_tsv="path/to/docs.tsv",
  queries_tsv="path/to/queries.tsv",
  qrels_trec="path/to/qrels.trec"
)
```

Here, documents and queries are represented in TSV format with format `[id]\t[text]`. Query
relevance judgments are provided in the standard TREC format:
`[query_id] [iteration] [doc_id] [rel]`. 


## Command Line Interface

Export data in various formats:
```
ir_datasets export [dataset-id] [docs/queries/qrels/scoreddocs]
```

`--format` specifies the output format (e.g., tsv or jsonl). `--fields` specifies which fields to
include in the output. This depends on what fields are available in the dataset, but most try to
include common fields, e.g., `text` in documents returns the text of the document, without any
markup.

Look up documents and queries by ID:

```
ir_datasets lookup [dataset-id] [--qid] [ids...]
```

`--format` and `--fields` also work here. `--qid` indicates that queries should be looked up instead
of documents (default).

This is much faster than using `ir_datasets export ... | grep` (or similar) because it indexes the
documents/queries by ID.

## Datasets

Available datasets include (each of which containing subsets):
 - `antique`
 - `car-v1.5`
 - `cord19`
 - `msmarco-document`
 - `msmarco-passage`
 - `nyt`
 - `trec-arabic`
 - `trec-mandarin`
 - `trec-robust04`
 - `trec-spanish`

See the [datasets documentation page](ir_datasets/docs/datasets.html) for details about each
dataset, its available subsets, and what data they provide.

## Citing

When using datasets provided by this package, be sure to properly cite them. Bibtex for each dataset
can be found on the [datasets documentation page](ir_datasets/docs/datasets.html), or in the python
interface via `dataset.bibtex()` (when available).

The `ir_datasets` package was released as part of [ABNIRML](https://arxiv.org/abs/2011.00696), so
please cite the following if you use this package:

```
@article{macavaney:arxiv2020-abnirml,
  author = {MacAvaney, Sean and Feldman, Sergey and Goharian, Nazli and Downey, Doug and Cohan, Arman},
  title = {ABNIRML: Analyzing the Behavior of Neural IR Models},
  year = {2020},
  url = {https://arxiv.org/abs/2011.00696},
  journal = {arXiv},
  volume = {abs/2011.00696}
}
```
