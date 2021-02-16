import io
import sys
import typing
import argparse
import ir_datasets


COMMON_HEAD = '''
<link rel="stylesheet" href="main.css" />
<script src="https://code.jquery.com/jquery-1.12.4.min.js" integrity="sha256-ZosEbRLbNQzLpnKIkEdrPv7lOy9C27hHQ+Xp8a4MxAQ=" crossorigin="anonymous"></script>
<script src="https://code.jquery.com/ui/1.12.1/jquery-ui.min.js" integrity="sha256-VazP97ZCwtekAsvgPBSUwPFKdrwD3unUfSGVYrahUqU=" crossorigin="anonymous"></script>
<script src="main.js"></script>
<meta name="viewport" content="width=device-width, initial-scale=1" />
'''


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets documentation', description='Generates documentation files.')
    parser.add_argument('--out_dir', default='./docs')
    args = parser.parse_args(args)
    out_dir = args.out_dir
    with open(f'{out_dir}/datasets.html', 'wt') as out:
        out.write('''
<!DOCTYPE html>
<html>
<head>
  <meta http-equiv="refresh" content="0; URL=index.html" />
  <title>ir_datasets</title>
</head>
<body>
  <p>Redirecting <a href="index.html">here</a></p>
</body>
</html>
''')
    out = open(f'{out_dir}/index.html', 'wt')
    out.write(f'''
<!DOCTYPE html>
<html>
<head>
{COMMON_HEAD}
<title>ir_datasets</title>
<body>
<div id="Backdrop"></div>
<div id="Popup">
<div id="ClosePopup">✖</div>
<div id="CodeSample"></div>
</div>

<div class="page">
<div style="position: absolute; top: 4px; right: 4px;">Github: <a href="https://github.com/allenai/ir_datasets/">allenai/ir_datasets</a></div>
<h1><code>ir_datasets</code></h1>
<p>
View on GitHub: <a href="https://github.com/allenai/ir_datasets/">allenai/ir_datasets</a>
</p>
<p>
<code>ir_datasets</code> is a python package that provides a common interface to many IR ad-hoc
ranking benchmarks, training datasets, etc. It was built as a fork of
<a href="https://OpenNIR.net/">OpenNIR</a> to allow easier integration with other systems.
</p>
<p>
Install using <code class="select">pip install ir_datasets</code>.
</p>
<p>
The package takes care of downloading datasets (including documents, queries, relevance judgments,
etc.) when available from public sources. Instructions on how to obtain datasets are provided when
they are not publicly available.
</p>
<p>
<code>ir_datasets</code> processes datasets into a common iterator format to allow them to be easily
used in python. It attempts to provide the data in an unaltered form (i.e., keeping all fields and
markup), while handling differences in file formats, encoding, etc. <code>adapter</code>s provide
extra functionality, e.g., to strip markup, index documents, etc. A command line interface is also
available.
</p>
<p>
Want a new dataset, added functionality, or a bug fixed? Feel free to
<a href="https://github.com/allenai/ir_datasets/issues">post an issue</a> or
<a href="https://github.com/allenai/ir_datasets/pulls">make a pull request</a>!
</p>
<hr/>

<h2>Python Interface</h2>

<p>
Load a dataset using:
</p>
<code class="select">
<div><span class="kwd">import</span> ir_datasets</div>
<div>dataset = ir_datasets.load(<span class="str">'dataset-id'</span>)</div>
</code>

<p>
A dataset object lets you iterate through supported properties like docs
(<code>dataset.docs_iter()</code>), queries (<code>dataset.queries_iter()</code>), and relevance
judgments (<code>dataset.qrels_iter()</code>). Each iterator yields <code>namedtuple</code>s, with
fields based on the available data.
</p>

<p>
If you want to use your own dataset, you can construct an object with the same interface as the
standard benchmarks by:
</p>

<code class="select">
<div><span class="kwd">import</span> ir_datasets</div>
<div>dataset = ir_datasets.create_dataset(</div>
<div>&nbsp;&nbsp;docs_tsv=<span class="str">"path/to/docs.tsv"</span>,</div>
<div>&nbsp;&nbsp;queries_tsv=<span class="str">"path/to/queries.tsv"</span>,</div>
<div>&nbsp;&nbsp;qrels_trec=<span class="str">"path/to/qrels.trec"</span></div>
<div>)</div>
</code>

<p>
Here, documents and queries are represented in TSV format with format <code>[id]\\t[text]</code>.
Query relevance judgments are provided in the standard TREC format: <code>[query_id] [iteration] [doc_id] [rel]</code>.
</p>

<hr/>

<h2>Command Line Interface</h2>

<p>
<b>Export data in various formats:</b>
</p>
<code class="select">
<div>ir_datasets export [dataset-id] [docs/queries/qrels/scoreddocs]</div>
</code>
<p>
<code>--format</code> specifies the output format (e.g., <code>tsv</code> or <code>jsonl</code>).
<code>--fields</code> specifies which fields to include (dataset-dependent).
</p>

<p>
<b>Look up documents and queries by ID:</b>
</p>
<code class="select">
<div>ir_datasets lookup [dataset-id] [--qid] [ids...]</div>
</code>
<p>
<code>--format</code> and <code>--fields</code> also work here. <code>--qid</code> indicates that
queries should be looked up instead of documents (default).
</p>
<p>
This is much faster than using <code>ir_datasets export ... | grep</code> (or similar) because it
indexes the documents/queries by ID.
</p>

<hr/>
''')
    top_level = [name for name in sorted(ir_datasets.registry) if '/' not in name]
    jump_list = ' '.join(f'<code class="jumpto"><a href="#{n}" class="str">{repr(n)}</a></code>' for n in top_level)
    out.write(f'''
<h2>Datasets</h2>
<ul>
''')
    with open('.github/workflows/verify_downloads.yml', 'wt') as f_ghdl:
        f_ghdl.write(f'''
name: Downloadable Content

on:
  schedule:
    - cron: '0 8 * * 0' # run every sunday at (around) 8:00am UTC
  workflow_dispatch:
    inputs:
      reason:
        description: 'reason'
        required: true
        default: ''

jobs:
''')
        for top in top_level:
            f_ghdl.write(f'''
  {top}:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.x'
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    - name: Test
      env:
        IR_DATASETS_DL_DISABLE_PBAR: 'true'
      run: |
        python -m test.downloads --filter "^{top}/"
''')

    top_level_map = {t: [] for t in top_level}
    for name in sorted(ir_datasets.registry):
        dataset = ir_datasets.registry[name]
        parent = name.split('/')[0]
        if parent != name:
            top_level_map[parent].append((name, dataset))
        else:
            documentation = dataset.documentation() if hasattr(dataset, 'documentation') else {}
            out.write(f'''
<li>
<a href="{name}.html">{documentation.get('pretty_name', name)}</a>
</li>
''')
    out.write(f'''
</ul>
<p>
See <a href="all.html">here</a> for a complete list of datasets and their subsets.
</p>
</body>
</html>
''')
    out.flush()
    out.close()

    with open(f'{out_dir}/all.html', 'wt') as out:
        index = []
        for name in sorted(ir_datasets.registry):
            dataset = ir_datasets.registry[name]
            parent = name.split('/')[0]
            if parent != name:
                ds_name = f'<a href="{parent}.html#{name}"><kbd><span class="prefix">{parent}</span>{name[len(parent):]}</kbd></a>'
                tbody = ''
            else:
                ds_name = f'<a style="font-weight: bold;" href="{parent}.html"><kbd>{parent}</kbd></a></li>'
                tbody = '</tbody><tbody>'
            index.append(f'{tbody}<tr><td>{ds_name}</td><td class="center">{emoji(dataset, "docs")}</td><td class="center">{emoji(dataset, "queries")}</td><td class="center">{emoji(dataset, "qrels")}</td><td class="center screen-small-hide">{emoji(dataset, "scoreddocs")}</td><td class="center screen-small-hide">{emoji(dataset, "docpairs")}</td></tr>')
        index = '\n'.join(index)
        out.write(f'''
<!DOCTYPE html>
<html>
<head>
{COMMON_HEAD}
<title>Datasets and Subsets - ir_datasets</title>
</head>
<body>
<div class="page">
<div style="position: absolute; top: 4px; left: 4px;"><a href="index.html">&larr; ir_datasets home</a></div>
<div style="position: absolute; top: 4px; right: 4px;"><span class="screen-small-hide">Github: </span><a href="https://github.com/allenai/ir_datasets/">allenai/ir_datasets</a></div>
<h1><kbd>ir_datasets</kbd>: Datasets and Subsets</h1>
<div>
<div style="font-weight: bold; font-size: 1.1em;">Index</div>
<table>
<tbody>
<tr>
<th>Dataset</th>
<th>docs</th>
<th>queries</th>
<th>qrels</th>
<th class="screen-small-hide">scoreddocs</th>
<th class="screen-small-hide">docpairs</th>
</tr>
{index}
</tbody>
</table>
</div>
</div>
</body>
</html>
''')

    for top_level in sorted(top_level_map):
        dataset = ir_datasets.registry[top_level]
        with open(f'{out_dir}/{top_level}.html', 'wt') as out:
            documentation = dataset.documentation() if hasattr(dataset, 'documentation') else {}
            index = '\n'.join(f'<li><a href="#{name}"><kbd><span class="prefix">{top_level}</span>{name[len(top_level):]}</kbd></a></li>' for name, ds in top_level_map[top_level])
            out.write(f'''
<!DOCTYPE html>
<html>
<head>
{COMMON_HEAD}
<title>{documentation.get('pretty_name', top_level)} - ir_datasets</title>
</head>
<body>
<div class="page">
<div style="position: absolute; top: 4px; left: 4px;"><a href="index.html">&larr; ir_datasets<span class="screen-small-hide"> home</span></a></div>
<div style="position: absolute; top: 4px; right: 4px;"><span class="screen-small-hide">Github: </span><a href="https://github.com/allenai/ir_datasets/blob/master/ir_datasets/datasets/{top_level.replace('-', '_')}.py">ir_datasets/{top_level.replace('-', '_')}.py</a></div>
<h1><span class="screen-small-hide"><kbd>ir_datasets</kbd>: </span>{documentation.get('pretty_name', top_level)}</h1>
<div>
<div style="font-weight: bold; font-size: 1.1em;">Index</div>
<ol class="index">
<li><a href="#{top_level}"><kbd>{top_level}</kbd></a></li>
{index}
</ol>
</div>
<hr />
<div class="dataset" id="{top_level}">
<h3><kbd class="select"><span class="str">"{top_level}"</kdb></h3>
{generate_dataset(dataset, top_level)}
</div>
''')
            for name, dataset in top_level_map[top_level]:
                out.write(f'''
<hr />
<div class="dataset" id="{name}" data-parent="{top_level}">
<h3><kbd class="ds-name select"><span class="str">"{name}"</kdb></h3>
{generate_dataset(dataset, name)}
</div>
''')
            out.write('''
</div>
</body>
</html>
''')

def generate_dataset(dataset, dataset_id):
    with io.StringIO() as out:
        if hasattr(dataset, 'documentation'):
            documentation = dataset.documentation()
        else:
            documentation = {}
        desc = documentation.get('desc', '<p><i>(no description provided)</i></p>')
        tags = []
        tags = ' '.join(f'<span class="tag tag-{t}" data-fields="{", ".join(c._fields)}">{t}</span>' for t, c in tags)
        out.write(f'''
<div class="desc">
{desc}
</div>
''')
        has_any = dataset.has_docs() or dataset.has_queries() or dataset.has_qrels() or dataset.has_docpairs() or dataset.has_scoreddocs() or 'bibtex' in documentation
        if has_any:
            out.write('<div class="tabs">')
        if dataset.has_queries():
            fields = ", ".join(dataset.queries_cls()._fields)
            out.write(f'''
<a class="tab" target="{dataset_id}__queries">queries</a>
<div id="{dataset_id}__queries" class="tab-content">
<p>Language: {_lang(dataset.queries_lang())}</p>
<div>Query type:</div>
{generate_data_format(dataset.queries_cls())}
<p>Example</p>
<code class="example">
<div><span class="kwd">import</span> ir_datasets</div>
<div>dataset = ir_datasets.load(<span class="str">'{dataset_id}')</div>
<div><span class="kwd">for</span> query <span class="kwd">in</span> dataset.queries_iter():</div>
<div>&nbsp;&nbsp;&nbsp;&nbsp;query <span class="comment"># namedtuple&lt;{fields}&gt;</span></div>
</code>
</div>
''')
        if dataset.has_docs():
            fields = ", ".join(dataset.docs_cls()._fields)
            out.write(f'''
<a class="tab" target="{dataset_id}__docs">docs</a>
<div id="{dataset_id}__docs" class="tab-content">
<p>Language: {_lang(dataset.docs_lang())}</p>
<div>Document type:</div>
{generate_data_format(dataset.docs_cls())}
<p>Example</p>
<code class="example">
<div><span class="kwd">import</span> ir_datasets</div>
<div>dataset = ir_datasets.load(<span class="str">'{dataset_id}')</div>
<div><span class="kwd">for</span> doc <span class="kwd">in</span> dataset.docs_iter():</div>
<div>&nbsp;&nbsp;&nbsp;&nbsp;doc <span class="comment"># namedtuple&lt;{fields}&gt;</span></div>
</code>
</div>
''')
        if dataset.has_qrels():
            fields = ", ".join(dataset.qrels_cls()._fields)
            out.write(f'''
<a class="tab" target="{dataset_id}__qrels">qrels</a>
<div id="{dataset_id}__qrels" class="tab-content">
<div>Query relevance judgment type:</div>
{generate_data_format(dataset.qrels_cls())}
<p>Relevance levels</p>
{generate_qrel_defs_table(dataset.qrels_defs())}
<p>Example</p>
<code class="example">
<div><span class="kwd">import</span> ir_datasets</div>
<div>dataset = ir_datasets.load(<span class="str">'{dataset_id}')</div>
<div><span class="kwd">for</span> qrel <span class="kwd">in</span> dataset.qrels_iter():</div>
<div>&nbsp;&nbsp;&nbsp;&nbsp;qrel <span class="comment"># namedtuple&lt;{fields}&gt;</span></div>
</code>
</div>
''')
        if dataset.has_scoreddocs():
            fields = ", ".join(dataset.scoreddocs_cls()._fields)
            out.write(f'''
<a class="tab" target="{dataset_id}__scoreddocs">scoreddocs</a>
<div id="{dataset_id}__scoreddocs" class="tab-content">
<div>Scored Document type:</div>
{generate_data_format(dataset.scoreddocs_cls())}
<p>Example</p>
<code class="example">
<div><span class="kwd">import</span> ir_datasets</div>
<div>dataset = ir_datasets.load(<span class="str">'{dataset_id}')</div>
<div><span class="kwd">for</span> scoreddoc <span class="kwd">in</span> dataset.scoreddocs_iter():</div>
<div>&nbsp;&nbsp;&nbsp;&nbsp;scoreddoc <span class="comment"># namedtuple&lt;{fields}&gt;</span></div>
</code>
</div>
''')
        if dataset.has_docpairs():
            fields = ", ".join(dataset.docpairs_cls()._fields)
            out.write(f'''
<a class="tab" target="{dataset_id}__docpairs">docpairs</a>
<div id="{dataset_id}__docpairs" class="tab-content">
<div>Document Pair type:</div>
{generate_data_format(dataset.docpairs_cls())}
<p>Example</p>
<code class="example">
<div><span class="kwd">import</span> ir_datasets</div>
<div>dataset = ir_datasets.load(<span class="str">'{dataset_id}')</div>
<div><span class="kwd">for</span> docpair <span class="kwd">in</span> dataset.docpairs_iter():</div>
<div>&nbsp;&nbsp;&nbsp;&nbsp;docpair <span class="comment"># namedtuple&lt;{fields}&gt;</span></div>
</code>
</div>
''')
        if 'bibtex' in documentation:
            out.write(f'''
<a class="tab" target="{dataset_id}__citation">Citation</a>
<div id="{dataset_id}__citation" class="tab-content">
bibtex:
<cite class="select">{documentation["bibtex"]}</cite>
</div>
''')
        if has_any:
            out.write('</div>')
        out.seek(0)
        return out.read()

def generate_data_format(cls):
    if cls in (str, int, float, bytes):
        return f'<span class="kwd">{cls.__name__}</span>'
    elif isinstance(cls, typing._GenericAlias):
        args = []
        for arg in cls.__args__:
            if arg is Ellipsis:
                args.append(' ...')
            else:
                args.append(generate_data_format(arg))
        if cls._name in ('Tuple', 'List'):
            return f'<span class="kwd">{cls._name}</span>[{",".join(args)}]'
    elif tuple in cls.__bases__ and hasattr(cls, '_fields'):
        fields = []
        for i, field in enumerate(cls._fields):
            f_type = 'UNKNOWN'
            if hasattr(cls, '__annotations__'):
                f_type = generate_data_format(cls.__annotations__[field])
            fields.append(f'<li data-tuple-idx="{i}"><span class="">{field}</span>: {f_type}</li>')
        return f"""
<div class="type">
<div class="type-name">{cls.__name__}: (<span class="kwd">namedtuple</span>)</div>
<ol class="type-fields">
{"".join(fields)}
</ol>
</div>""".strip()
    raise RuntimeError(f"uknown class {cls}")


def generate_qrel_defs_table(defs):
    rows = []
    for score, desc in sorted(defs.items()):
        rows.append(f'<tr><td class="relScore">{score}</td><td>{desc}</td></tr>')
    rows = "\n".join(rows)
    return f'''
<table>
<tr><th>Rel.</th><th>Definition</th></tr>
{rows}
</table>
'''

def emoji(ds, arg):
    has = getattr(ds, f'has_{arg}')()
    if has:
        instructions = hasattr(ds, f'documentation') and ds.documentation().get(f'{arg}_instructions')
        if instructions:
            return f'<span style="cursor: help;" title="{instructions}">⚠️</span>'
        return f'<span style="cursor: help;" title="{arg} available as automatic download">✅</span>'
    return ''


def _lang(lang_code):
    if lang_code is None:
        return '<em>multiple/other/unknown</em>'
    return f'<span class="lang-code">{lang_code}</span>'


if __name__ == '__main__':
    main(sys.argv[1:])
