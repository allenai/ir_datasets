# ir_datasets

`ir_datasets` is a python package that provides a common interface to many IR ad-hoc ranking
benchmarks, training datasets, etc.

The package takes care of downloading datasets (including documents, queries, relevance judgments,
etc.) when available from public sources. Instructions on how to obtain datasets are provided when
they are not publicly available.

`ir_datasets` provides a common iterator format to allow them to be easily used in python. It
attempts to provide the data in an unaltered form (i.e., keeping all fields and markup), while
handling differences in file formats, encoding, etc. Adapters provide extra functionality, e.g., to
allow quick lookups of documents by ID.

A command line interface is also available.

You can find a list of datasets and their features [here](https://ir-datasets.com/).
Want a new dataset, added functionality, or a bug fixed? Feel free to post an issue or make a pull request! 

## Getting Started

For a quick start with the Python API, check out our Colab tutorials:
[Python](https://colab.research.google.com/github/allenai/ir_datasets/blob/master/examples/ir_datasets.ipynb)
[Command Line](https://colab.research.google.com/github/allenai/ir_datasets/blob/master/examples/ir_datasets_cli.ipynb)

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

## Features

**Python and Command Line Interfaces**. Access datasts both through a simple Python API and
via the command line.

```python
import ir_datasets
dataset = ir_datasets.load('msmarco-passage/train')
# Documents
for doc in dataset.docs_iter():
    print(doc)
# GenericDoc(doc_id='0', text='The presence of communication amid scientific minds was equa...
# GenericDoc(doc_id='1', text='The Manhattan Project and its atomic bomb helped bring an en...
# ...
```

```bash
ir_datasets export msmarco-passage/train docs | head -n2
0 The presence of communication amid scientific minds was equally important to the success of the Manh...
1 The Manhattan Project and its atomic bomb helped bring an end to World War II. Its legacy of peacefu...
```

**Automatically downloads source files** (when available). Will download and verify the source
files for queries, documents, qrels, etc. when they are publicly available, as they are needed.
A CI build checks weekly to ensure that all the downloadable content is available and correct:
![Downloadable Content](https://github.com/allenai/ir_datasets/workflows/Downloadable%20Content/badge.svg)

```python
import ir_datasets
dataset = ir_datasets.load('msmarco-passage/train')
for doc in dataset.docs_iter(): # Will download and extract MS-MARCO's collection.tar.gz the first time
    ...
for query in dataset.queries_iter(): # Will download and extract MS-MARCO's queries.tar.gz the first time
    ...
```

**Instructions for dataset access** (when not publicly available). Provides instructions on how
to get a copy of the data when it is not publicly available online (e.g., when it requires a
data usage agreement).

```python
import ir_datasets
dataset = ir_datasets.load('trec-arabic')
for doc in dataset.docs_iter():
    ...
# Provides the following instructions:
# The dataset is based on the Arabic Newswire corpus. It is available from the LDC via: <https://catalog.ldc.upenn.edu/LDC2001T55>
# To proceed, symlink the source file here: [gives path]
```

**Support for datasets big and small**. By using iterators, supports large datasets that may
not fit into system memory, such as ClueWeb.

```python
import ir_datasets
dataset = ir_datasets.load('clueweb09')
for doc in dataset.docs_iter():
    ... # will iterate through all ~1B documents
```

**Fixes known dataset issues**. For instance, automatically corrects the document UTF-8 encoding
problem in the MS-MARCO passage collection.

```python
import ir_datasets
dataset = ir_datasets.load('msmarco-passage')
docstore = dataset.docs_store()
docstore.get('243').text
# "John Maynard Keynes, 1st Baron Keynes, CB, FBA (/ˈkeɪnz/ KAYNZ; 5 June 1883 – 21 April [SNIP]"
# Naïve UTF-8 decoding yields double-encoding artifacts like:
# "John Maynard Keynes, 1st Baron Keynes, CB, FBA (/Ë\x88keÉªnz/ KAYNZ; 5 June 1883 â\x80\x93 21 April [SNIP]"
#                                                  ~~~~~~  ~~                       ~~~~~~~~~
```

**Fast Random Document Access.** Builds data structures that allow fast and efficient lookup of
document content. For large datasets, such as ClueWeb, uses
[checkpoint files](https://ir-datasets.com/clueweb_warc_checkpoints.md) to load documents from
source 40x faster than normal. Results are cached for even faster subsequent accesses.

```python
import ir_datasets
dataset = ir_datasets.load('clueweb12')
docstore = dataset.docs_store()
docstore.get_many(['clueweb12-0000tw-05-00014', 'clueweb12-0000tw-05-12119', 'clueweb12-0106wb-18-19516'])
# {'clueweb12-0000tw-05-00014': ..., 'clueweb12-0000tw-05-12119': ..., 'clueweb12-0106wb-18-19516': ...}
```

**Fancy Iter Slicing.** Sometimes it's helpful to be able to select ranges of data (e.g., for processing
document collections in parallel on multiple devices). Efficient implementations of slicing operations
allow for much faster dataset partitioning than using `itertools.slice`.

```python
import ir_datasets
dataset = ir_datasets.load('clueweb12')
dataset.docs_iter()[500:1000] # normal slicing behavior
# WarcDoc(doc_id='clueweb12-0000tw-00-00502', ...), WarcDoc(doc_id='clueweb12-0000tw-00-00503', ...), ...
dataset.docs_iter()[-10:-8] # includes negative indexing
# WarcDoc(doc_id='clueweb12-1914wb-28-24245', ...), WarcDoc(doc_id='clueweb12-1914wb-28-24246', ...)
dataset.docs_iter()[::100] # includes support for skip (only positive values)
# WarcDoc(doc_id='clueweb12-0000tw-00-00000', ...), WarcDoc(doc_id='clueweb12-0000tw-00-00100', ...), ...
dataset.docs_iter()[1/3:2/3] # supports proportional slicing (this takes the middle third of the collection)
# WarcDoc(doc_id='clueweb12-0605wb-28-12714', ...), WarcDoc(doc_id='clueweb12-0605wb-28-12715', ...), ...
```

## Datasets

Available datasets include:
 - [ANTIQUE](https://ir-datasets.com/antique.html)
 - [AQUAINT](https://ir-datasets.com/aquaint.html)
 - [TREC CAR](https://ir-datasets.com/car.html)
 - [ClueWeb09](https://ir-datasets.com/clueweb09.html)
 - [ClueWeb12](https://ir-datasets.com/clueweb12.html)
 - [CodeSearchNet](https://ir-datasets.com/codesearchnet.html)
 - [CORD-19](https://ir-datasets.com/cord19.html)
 - [GOV](https://ir-datasets.com/gov.html)
 - [GOV2](https://ir-datasets.com/gov2.html)
 - [Highwire (TREC Genomics 2006-07)](https://ir-datasets.com/highwire.html)
 - [Medline](https://ir-datasets.com/medline.html)
 - [MSMARCO (document)](https://ir-datasets.com/msmarco-document.html)
 - [MSMARCO (passage)](https://ir-datasets.com/msmarco-passage.html)
 - [MSMARCO (QnA)](https://ir-datasets.com/msmarco-qna.html)
 - [NFCorpus (NutritionFacts)](https://ir-datasets.com/nfcorpus.html)
 - [NYT](https://ir-datasets.com/nyt.html)
 - [PubMed Central (TREC CDS)](https://ir-datasets.com/pmc.html)
 - [TREC Arabic](https://ir-datasets.com/trec-arabic.html)
 - [TREC Mandarin](https://ir-datasets.com/trec-mandarin.html)
 - [TREC Robust 2004](https://ir-datasets.com/trec-robust04.html)
 - [TREC Spanish](https://ir-datasets.com/trec-spanish.html)
 - [Tweets 2013 (Internet Archive)](https://ir-datasets.com/tweets2013-ia.html)
 - [Vaswani](https://ir-datasets.com/vaswani.html)
 - [WikIR](https://ir-datasets.com/wikir.html)

There are "subsets" under each dataset. For instance, `clueweb12/b13/trec-misinfo-2019` provides the
queries and judgments from the [2019 TREC misinformation track](https://trec.nist.gov/data/misinfo2019.html),
and `msmarco-document/orcas` provides the [ORCAS dataset](https://microsoft.github.io/msmarco/ORCAS). They
tend to be organized with the document collection at the top level.

See the ir_dataets docs ([ir_datasets.com](https://ir-datasets.com/)) for details about each
dataset, its available subsets, and what data they provide.

## Environment variables

 - `IR_DATASETS_HOME`: Home directory for ir_datasets data (default `~/.ir_datasets/`). Contains directories
   for each top-level dataset.
 - `IR_DATASETS_TMP`: Temporary working directory (default `/tmp/ir_datasets/`).
 - `IR_DATASETS_DL_TIMEOUT`: Download stream read timeout, in seconds (default `15`). If no data is received
   within this duration, the connection will be assumed to be dead, and another download may be attempted.
 - `IR_DATASETS_DL_TRIES`: Default number of download attempts before exception is thrown (default `3`).
   When the server accepts Range requests, uses them. Otherwise, will download the entire file again
 - `IR_DATASETS_DL_DISABLE_PBAR`: Set to `true` to disable the progress bar for downloads. Useful in settings
   where an interactive console is not available.

## Citing

When using datasets provided by this package, be sure to properly cite them. Bibtex for each dataset
can be found on the [datasets documentation page](https://allenai.github.io/ir_datasets/datasets.html),
or in the python interface via `dataset.documentation()['bibtex']` (when available).

Please cite the following if you use this package:

```
@article{macavaney:arxiv2021-irds,
  author = {MacAvaney, Sean and Yates, Andrew and Feldman, Sergey and Downey, Doug and Cohan, Arman and Goharian, Nazli},
  title = {Simplified Data Wrangling with ir_datasets},
  year = {2021},
  url = {https://arxiv.org/abs/2103.02280},
  journal = {arXiv},
  volume = {abs/2103.02280}
}
```
