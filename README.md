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

You can find a list of datasets and their features [here](https://allenai.github.io/ir_datasets/datasets.html).
Want a new dataset, added functionality, or a bug fixed? Feel free to post an issue or make a pull request! 

## Getting Started

Install via pip:

```
pip install ir_datasets
```

If you want to build from source, use:

```
$ git clone https://github.com/allenai/ir_datasets
$ cd ir_datasets
$ python setup.py bdist_wheel
$ pip install dist/ir_datasets-*.whl
```

Tested with python versions 3.6 and 3.7

## Python Interface

Load a dataset, such as the [MS-MARCO passage ranking datset](https://microsoft.github.io/msmarco/), using:
```python
import ir_datasets
dataset = ir_datasets.load('msmarco-passage/train')
```

A dataset object lets you iterate through supported properties like docs (`dataset.docs_iter()`),
queries (`dataset.queries_iter()`), and relevance judgments (`dataset.qrels_iter()`). Each iterator
yields namedtuples, with fields based on the available data.

```python
# Documents
for doc in dataset.docs_iter():
    print(doc)
# GenericDoc(doc_id='0', text='The presence of communication amid scientific minds was equa...
# GenericDoc(doc_id='1', text='The Manhattan Project and its atomic bomb helped bring an en...
# ...

# Queries
for query in dataset.queries_iter():
    print(query)
# GenericQuery(query_id='121352', text='define extreme')                                          
# GenericQuery(query_id='634306', text='what does chattel mean on credit history')
# ...

# Query relevance judgments (qrels)
for qrel in dataset.qrels_iter():
    print(qrels)
# TrecQrel(query_id='1185869', doc_id='0', relevance=1, iteration='0')
# TrecQrel(query_id='1185868', doc_id='16', relevance=1, iteration='0')
# ...

# Look up queries and documents by ID
queries_store = dataset.queries_store()
queries_store.get("1185868")
# GenericQuery(query_id='1185868', text='_________ justice is designed to repair the harm to victim, the comm...

dataset = ir_datasets.wrappers.DocstoreWrapper(dataset)
doc_store = dataset.docs_store()
doc_store.get("16")
# GenericDoc(doc_id='16', text='The approach is based on a theory of justice that considers crime and wrongdoi...
```

If you want to use your own dataset, you can construct an object with the same interface as the
standard benchmarks by:
```python
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
$ ir_datasets export [dataset-id] [docs/queries/qrels/scoreddocs/docpairs]

$ ir_datasets export msmarco-passage/train docs | head -n2
0	The presence of communication amid scientific minds was equally important to the success of the Manh...
1	The Manhattan Project and its atomic bomb helped bring an end to World War II. Its legacy of peacefu...

$ ir_datasets export msmarco-passage/train docs --format jsonl | head -n2
{"doc_id": "0", "text": "The presence of communication amid scientific minds was equally important to the su...
{"doc_id": "1", "text": "The Manhattan Project and its atomic bomb helped bring an end to World War II. Its ...
```

`--format` specifies the output format (e.g., tsv or jsonl). `--fields` specifies which fields to
include in the output. This depends on what fields are available in the dataset, but most try to
include common fields, e.g., `text` in documents returns the text of the document, without any
markup.

Look up documents and queries by ID:

```
$ ir_datasets lookup [dataset-id] [--qid] [ids...]
```

`--format` and `--fields` also work here. `--qid` indicates that queries should be looked up instead
of documents (default).

This is much faster than using `ir_datasets export ... | grep` (or similar) because it indexes the
documents/queries by ID.

## Datasets

Available datasets include (each of which containing subsets):
 - [ANTIQUE](https://allenai.github.io/ir_datasets/antique.html)
 - [TREC CAR](https://allenai.github.io/ir_datasets/car-v1.5.html)
 - [ClueWeb09](https://allenai.github.io/ir_datasets/clueweb09.html)
 - [ClueWeb12](https://allenai.github.io/ir_datasets/clueweb12.html)
 - [CORD-19](https://allenai.github.io/ir_datasets/cord19.html)
 - [MSMARCO (document)](https://allenai.github.io/ir_datasets/msmarco-document.html)
 - [MSMARCO (passage)](https://allenai.github.io/ir_datasets/msmarco-passage.html)
 - [NYT](https://allenai.github.io/ir_datasets/nyt.html)
 - [TREC Arabic](https://allenai.github.io/ir_datasets/trec-arabic.html)
 - [TREC Mandarin](https://allenai.github.io/ir_datasets/trec-mandarin.html)
 - [TREC Robust 2004](https://allenai.github.io/ir_datasets/trec-robust04.html)
 - [TREC Spanish](https://allenai.github.io/ir_datasets/trec-spanish.html)

See the [datasets documentation page](https://allenai.github.io/ir_datasets/datasets.html) for details about each
dataset, its available subsets, and what data they provide.

## Citing

When using datasets provided by this package, be sure to properly cite them. Bibtex for each dataset
can be found on the [datasets documentation page](https://allenai.github.io/ir_datasets/datasets.html), or in the python
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
