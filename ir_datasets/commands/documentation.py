import io
import os
import sys
import typing
import argparse
from contextlib import contextmanager
from pathlib import Path
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
    parser.add_argument('--release', action='store_true')

    args = parser.parse_args(args)
    out_dir = args.out_dir

    versions = [f'v{ir_datasets.__version__}', ''] if args.release else ['master']

    for version in versions:
        if version:
            os.makedirs(f'{args.out_dir}/{version}', exist_ok=True)

        generate_index(args.out_dir, version)
        generate_python_docs(args.out_dir, version)
        generate_cli_docs(args.out_dir, version)
        generate_redirect(args.out_dir, version, 'datasets.html', 'index.html')
        generate_redirect(args.out_dir, version, 'all.html', 'index.html')
        generate_downloads(args.out_dir, version)
        generate_css(args.out_dir, version)
        generate_js(args.out_dir, version)

        top_level = [name for name in sorted(ir_datasets.registry) if '/' not in name]
        top_level_map = {t: [] for t in top_level}
        for name in sorted(ir_datasets.registry):
            dataset = ir_datasets.registry[name]
            parent = name.split('/')[0]
            if parent != name:
                top_level_map[parent].append((name, dataset))

        generate_ghdl(top_level)
        for top_level in sorted(top_level_map):
            generate_dataset_page(args.out_dir, version, top_level, top_level_map[top_level])


def generate_dataset_page(out_dir, version, top_level, sub_datasets):
    dataset = ir_datasets.registry[top_level]
    documentation = dataset.documentation() if hasattr(dataset, 'documentation') else {}
    with page_template(f'{top_level}.html', out_dir, version, title=documentation.get('pretty_name', top_level), source=f'datasets/{top_level.replace("-", "_")}.py') as out:
        data_access_section = generate_data_access_section(documentation)
        index = '\n'.join(f'<li><a href="#{name}"><kbd><span class="prefix">{top_level}</span>{name[len(top_level):]}</kbd></a></li>' for name, ds in sub_datasets)
        out.write(f'''
<div style="font-weight: bold; font-size: 1.1em;">Index</div>
<ol class="index">
<li><a href="#{top_level}"><kbd>{top_level}</kbd></a></li>
{index}
</ol>
<div id="Downloads">
</div>
{data_access_section}<hr />
<div class="dataset" id="{top_level}">
<h3><kbd class="select"><span class="str">"{top_level}"</kdb></h3>
{generate_dataset(dataset, top_level)}
</div>
''')
        for name, dataset in sub_datasets:
            out.write(f'''
<hr />
<div class="dataset" id="{name}" data-parent="{top_level}">
<h3><kbd class="ds-name select"><span class="str">"{name}"</kdb></h3>
{generate_dataset(dataset, name)}
</div>
''')
        out.write('''
<script type="text/javascript">
$(function () {
    $.ajax({
        'url': 'https://smac.pub/irdsdlc?ds=''' + top_level + ''''
    }).done(function (data) {
        $('#Downloads').append(generateDownloads('Downloadable content', data));
    });
});
</script>
''')



def generate_dataset(dataset, dataset_id):
    with io.StringIO() as out:
        if hasattr(dataset, 'documentation'):
            documentation = dataset.documentation()
        else:
            documentation = {}
        if 'desc' not in documentation:
            print(f'no description for {dataset_id}')
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


def generate_data_access_section(documentation):
    if 'data_access' not in documentation:
        return ''
    return f'''
<div id="DataAccess">
<h3>Data Access Information</h3>
{documentation["data_access"]}
</div>
'''


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
        if cls._name in ('Tuple', 'List', 'Dict'):
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





def generate_index(out_dir, version):
    with page_template('index.html', out_dir, version, title='Catalog') as out:
        if version == 'master':
            install = '--upgrade git+https://github.com/allenai/ir_datasets.git'
        elif version.startswith('v'):
            install = f'ir_datasets=={version[1:]}'
        elif not version:
            install = '--upgrade ir_datasets'
        else:
            raise RuntimeError(f'unknown version {version}')
        index = []
        jump = []
        for name in sorted(ir_datasets.registry):
            dataset = ir_datasets.registry[name]
            parent = name.split('/')[0]
            if parent != name:
                ds_name = f'<a href="{parent}.html#{name}"><kbd><span class="prefix"><span class="screen-small-hide">{parent}</span><span class="screen-small-show">&hellip;</span></span>{name[len(parent):]}</kbd></a>'
                tbody = ''
                row_id = ''
            else:
                ds_name = f'<a style="font-weight: bold;" href="{parent}.html"><kbd>{parent}</kbd></a></li>'
                tbody = '</tbody><tbody>'
                row_id = f' id="{parent}"'
                jump.append(f'<option value="{parent}">{parent}</option>')
            index.append(f'{tbody}<tr{row_id}><td>{ds_name}</td><td class="center">{emoji(dataset, "docs", parent)}</td><td class="center">{emoji(dataset, "queries", parent)}</td><td class="center">{emoji(dataset, "qrels", parent)}</td><td class="center screen-small-hide">{emoji(dataset, "scoreddocs", parent)}</td><td class="center screen-small-hide">{emoji(dataset, "docpairs", parent)}</td></tr>')
        index = '\n'.join(index)
        jump = '\n'.join(jump)
        out.write(f'''
<p>
<code>ir_datasets</code> provides a common interface to many IR ranking datasets.
</p>

<h2>Getting Started</h2>

<p>
Install with pip:
</p>

<code class="example">pip install {install}</code>

<p>Guides:</p>

<ul>
<li>Colab Tutorials: <a href="https://colab.research.google.com/github/allenai/ir_datasets/blob/master/examples/ir_datasets.ipynb">python</a>, <a href="https://colab.research.google.com/github/allenai/ir_datasets/blob/master/examples/ir_datasets_cli.ipynb">CLI</a></li>
<li><a href="python.html">Python API Documentation</a> (<a href="python-beta.html">beta version</a>)</li>
<li><a href="cli.html">CLI Documentation</a></li>
<li><a href="downloads.html">Download Dashboard</a></li>
<li><a href="https://github.com/allenai/ir_datasets/blob/master/examples/adding_datasets.ipynb">Adding new datasets</a></li>
<li><a href="https://arxiv.org/pdf/2103.02280.pdf">ir_datasets SIGIR resource paper</a></li>
</ul>

<h2 style="margin-bottom: 4px;">Dataset Index</h2>
<select id="DatasetJump">
<option value="">Jump to Dataset...</option>
{jump}
</select>
<p>✅: Data available as automatic download</p>
<p>⚠️: Data available from a third party</p>
<table>
<tbody>
<tr>
<th class="stick-top">Dataset</th>
<th class="stick-top">docs</th>
<th class="stick-top">queries</th>
<th class="stick-top">qrels</th>
<th class="stick-top screen-small-hide">scoreddocs</th>
<th class="stick-top screen-small-hide">docpairs</th>
</tr>
{index}
</tbody>
</table>
''')
        v_prefix = '../' if version else ''
        versions = [str(v).split('/')[-2] for v in sorted(Path(out_dir).glob('*/index.html'))]
        versions = [v for v in versions if v != version]
        versions = [f'<li><a href="{v_prefix}{v}/index.html">{v}</a></li>' for v in versions]
        if version:
            versions = [f'<li><a href="../index.html">Latest Release</a></li>'] + versions
        versions = '\n'.join(versions)
        out.write(f'''
<h2>Other Versions</h2>
<ul>
{versions}
</ul>
<h2>Citation</h2>
<p>
When using datasets provided by this package, be sure to properly cite them. Bibtex for each dataset
can be found on each dataset's documenation page, or in the python interface via
<kbd>dataset.documentation()['bibtex']</kbd> (when available).
</p>
<p>If you use this tool, please cite our <a href="https://arxiv.org/pdf/2103.02280.pdf">SIGIR resource paper</a>:</p>
<cite class="select">@inproceedings{{macavaney:sigir2021-irds,
  author = {{MacAvaney, Sean and Yates, Andrew and Feldman, Sergey and Downey, Doug and Cohan, Arman and Goharian, Nazli}},
  title = {{Simplified Data Wrangling with ir_datasets}},
  year = {{2021}},
  booktitle = {{SIGIR}}
}}
</cite>
''')





def generate_redirect(out_dir, version, page_from, page_to):
    with open(get_file_path(out_dir, version, page_from), 'wt') as out:
        out.write(f'''
<!DOCTYPE html>
<html>
<head>
  <meta http-equiv="refresh" content="0; URL={page_to}" />
  <title>ir_datasets</title>
</head>
<body>
  <p>Redirecting <a href="{page_to}">here</a></p>
</body>
</html>
''')




def generate_python_docs(out_dir, version):
    with page_template('python.html', out_dir, version, title='Python API') as out:
        out.write(f'''
<h2 id="dataset">Dataset objects</h2>

<p>
Datasets can be obtained through <code>ir_datasts.load(<span class="str">"dataset-id"</span>)</code>
or constructed with <code>ir_datasets.create_dataset(...)</code>. Dataset objects provide the
following methods:
</p>

<h4><code>dataset.has_docs() -> bool</code></h4>

<div class="methodinfo">
<p>Returns <code class="kwd">True</code> if this dataset supports <code>dataset.docs_*</code> methods.</p>
</div>

<h4><code>dataset.has_queries() -> bool</code></h4>

<div class="methodinfo">
<p>Returns <code class="kwd">True</code> if this dataset supports <code>dataset.queries_*</code> methods.</p>
</div>

<h4><code>dataset.has_qrels() -> bool</code></h4>

<div class="methodinfo">
<p>Returns <code class="kwd">True</code> if this dataset supports <code>dataset.qrels_*</code> methods.</p>
</div>
<h4><code>dataset.has_scoreddocs() -> bool</code></h4>

<div class="methodinfo">
<p>Returns <code class="kwd">True</code> if this dataset supports <code>dataset.scoreddocs_*</code> methods.</p>
</div>
<h4><code>dataset.has_docpairs() -> bool</code></h4>

<div class="methodinfo">
<p>Returns <code class="kwd">True</code> if this dataset supports <code>dataset.docpairs_*</code> methods.</p>
</div>


<h4><code>dataset.docs_count() -> int</code></h4>

<div class="methodinfo">
<p>Returns the number of documents in the collection.</p>
</div>


<h4><code>dataset.docs_iter() -> iter[namedtuple]</code></h4>

<div class="methodinfo">
<p>Returns an iterator of <code>namedtuple</code>s, where each item is a document in the collection.</p>
<p>This iterator supports fancy slicing (with some limitations):</p>

<code class="example">
<div class="comment"># First 10 documents</div>
<div>dataset.docs_iter()[:10]</div>

<div class="comment"># Last 10 documents</div>
<div>dataset.docs_iter()[-10:]</div>

<div class="comment"># Every 2 documents</div>
<div>dataset.docs_iter()[::2]</div>

<div class="comment"># Every 2 documents, starting with the first document</div>
<div>dataset.docs_iter()[1::2]</div>

<div class="comment"># The first half of the collection</div>
<div>dataset.docs_iter()[:1/2]</div>

<div class="comment"># The middle third of collection</div>
<div>dataset.docs_iter()[1/3:2/3]</div>
</code>

<p>
Note that the fancy slicing mechanics are faster and more sophisticated than
<code>itertools.islice</code>; documents are not processed if they are skipped.
</p>
</div>

<h4><code>dataset.docs_cls() -> type</code></h4>

<div class="methodinfo">
<p>
Returns the <code class="kwd">NamedTuple</code> type that the <code>docs_iter</code> returns.
The available fields and type information can be found with <code>_fields</code> and <code>__annotations__</code>:
</p>

<code class="example">
<div>dataset.docs_cls()._fields</div>
</code>
<code class="output">
<div>(<span class="str">'doc_id'</span>, <span class="str">'title'</span>, <span class="str">'doi'</span>, <span class="str">'date'</span>, <span class="str">'abstract'</span>)</div>
</code>
<code class="example">
<div>dataset.docs_cls().__annotations__</div>
</code>
<code class="output">
<div>{{</div>
<div>&nbsp;&nbsp;<span class="str">'doc_id'</span>: <span class="kwd">str</span>,</div>
<div>&nbsp;&nbsp;<span class="str">'title'</span>: <span class="kwd">str</span>,</div>
<div>&nbsp;&nbsp;<span class="str">'doi'</span>: <span class="kwd">str</span>,</div>
<div>&nbsp;&nbsp;<span class="str">'date'</span>: <span class="kwd">str</span>,</div>
<div>&nbsp;&nbsp;<span class="str">'abstract'</span>: <span class="kwd">str</span></div>
<div>}}</div>
</code>
</div>


<h4><code>dataset.docs_store() -> docstore</code></h4>

<div class="methodinfo">
<p>
Returns a <a href="#docstore">docstore object</a> for this dataset, which enables
fast lookups by <code>doc_id</code>.
</p>
</div>

<h4><code>dataset.docs_lang() -> str</code></h4>

<div class="methodinfo">
<p>
Returns the two-character <a href="https://en.wikipedia.org/wiki/ISO_639-1">ISO 639-1
language code</a> (e.g., <span class="str">"en"</span> for English) of the documents in this collection.
Returns <span class="kwd">None</span> if there are multiple languages, a language not represented by an
ISO 639-1 code, or the language is otherwise unknown.
</p>
</div>


<h4><code>dataset.queries_iter() -> iter[namedtuple]</code></h4>

<div class="methodinfo">
<p>
Returns an iterator over namedtuples representing queries in the dataset.
</p>
</div>


<h4><code>dataset.queries_cls() -> type</code></h4>

<div class="methodinfo">
<p>
Returns the type of the namedtuple returned by <code>queries_iter</code>,
including <code>_fields</code> and <code>__annotations__</code>.
</p>
</div>


<h4><code>dataset.queries_lang() -> str</code></h4>

<div class="methodinfo">
<p>
Returns the two-character <a href="https://en.wikipedia.org/wiki/ISO_639-1">ISO 639-1
language code</a> (e.g., <span class="str">"en"</span> for English) of the queries.
Returns <span class="kwd">None</span> if there are multiple languages, a language not represented by an
ISO 639-1 code, or the language is otherwise unknown. Note that some datasets include
translations as different query fields.
</p>
</div>


<h4><code>dataset.qrels_iter() -> iter[namedtuple]</code></h4>

<div class="methodinfo">
<p>
Returns an iterator over namedtuples representing query relevance assessments in the dataset.
</p>
</div>


<h4><code>dataset.qrels_cls() -> type</code></h4>

<div class="methodinfo">
<p>
Returns the type of the namedtuple returned by <code>qrels_iter</code>,
including <code>_fields</code> and <code>__annotations__</code>.
</p>
</div>


<h4><code>dataset.qrels_defs() -> dict[int, str]</code></h4>

<div class="methodinfo">
<p>
Returns a mapping between relevance levels and a textual description of
what the level represents. (E.g., 0 represting not relevant, 1 representing
possibly relevant, 2 representing definitely relevant.)
</p>
</div>


<h4><code>dataset.qrels_dict() -> dict[str, dict[str, int]]</code></h4>

<div class="methodinfo">
<p>
Returns a dict of dicts representing all qrels for this collection. Note
that this will load all qrels into memory. The outer dict key is the
<code>query_id</code> and the inner key is the <code>doc_id</code>.
This is useful in tools such as <a href="https://github.com/cvangysel/pytrec_eval">pytrec_eval</a>.
</p>
</div>


<h4><code>dataset.scoreddocs_iter() -> iter[namedtuple]</code></h4>

<div class="methodinfo">
<p>
Returns an iterator over namedtuples representing scored docs (e.g., initial rankings
for re-ranking tasks) in the dataset.
</p>
</div>


<h4><code>dataset.scoreddocs_cls() -> type</code></h4>

<div class="methodinfo">
<p>
Returns the type of the namedtuple returned by <code>scoreddocs_iter</code>,
including <code>_fields</code> and <code>__annotations__</code>.
</p>
</div>


<h4><code>dataset.docpairs_iter() -> iter[namedtuple]</code></h4>

<div class="methodinfo">
<p>
Returns an iterator over namedtuples representing doc pairs (e.g., training pairs) in the dataset.
</p>
</div>


<h4><code>dataset.docpairs_cls() -> type</code></h4>

<div class="methodinfo">
<p>
Returns the type of the namedtuple returned by <code>docpairs_iter</code>,
including <code>_fields</code> and <code>__annotations__</code>.
</p>
</div>


<h2 id="docstore">Docstore objects</h2>

<p>
Docstores enable fast lookups of documents by their <code>doc_id</code>.
</p>
<p>
The implementation depends on the dataset. For small datasets, a simple
index structure is built on disk to enable fast lookups. For large datasets,
you wouldn't want to make a copy of the collection, so lookups are accelerated
by taking advantage of the source file structure and decompression checkpoints.
</p>
<p>
For small datasets, docstores also enable faster iteration and fancy slicing.
In some cases, a docstore instance is automatically generated during the first
call to <code>docs_iter</code> to enable faster iteration in the future.
</p>

<h4><code>docstore.get(doc_id: str) -> namedtuple</code></h4>
<div class="methodinfo">
<p>
Gets a single document by <code>doc_id</code>. Returns a single <span class="kwd">namedtuple</span>
or throws a <span class="kwd">KeyError</span> if the document it not in the collection.
</p>
</div>

<h4><code>docstore.get_many(doc_ids: iter[str]) -> dict[str, namedtuple]</code></h4>
<div class="methodinfo">
<p>
Gets documents whose IDs appear in <code>doc_ids</code>. Returns a <span class="kwd">dict</span>
mapping string IDs to <span class="kwd">namedtuple</span>. Missing documents will not appear in
the dictionary.
</p>
</div>

<h4><code>docstore.get_many_iter(doc_ids: iter[str]) -> iter[namedtuple]</code></h4>
<div class="methodinfo">
<p>
Returns an iterator over documents whose IDs appear in <code>doc_ids</code>. The order of the
documents is not guaranteed to be the same as doc_ids. (This is to allow implementations to
optmize the order in which documents are retrieved from disk.) Missing documents will not
appear in the iterator.
</p>
</div>
''')
    with page_template('python-beta.html', out_dir, version, title='Beta Python API') as out:
        out.write(f'''
<div class="warn">
This is an experimental version of the Python API, and may be buggy and subject to change in future
versions. See <a href="python.html">here</a> for the official python API. For now, both versions
of the python API live side-by-side.
</div>

<h2 id="dataset">Dataset objects</h2>

<p>
Datasets can be obtained through <code>ir_datasts.load(<span class="str">"dataset-id"</span>)</code>
or constructed with <code>ir_datasets.create_dataset(...)</code>. Dataset objects provide the
following methods:
</p>

<h4><code>dataset.has_docs() -> bool</code></h4>

<div class="methodinfo">
<p>Returns <code class="kwd">True</code> if this dataset supports <code>dataset.docs_*</code> methods.</p>
</div>

<h4><code>dataset.has_queries() -> bool</code></h4>

<div class="methodinfo">
<p>Returns <code class="kwd">True</code> if this dataset supports <code>dataset.queries_*</code> methods.</p>
</div>

<h4><code>dataset.has_qrels() -> bool</code></h4>

<div class="methodinfo">
<p>Returns <code class="kwd">True</code> if this dataset supports <code>dataset.qrels_*</code> methods.</p>
</div>

<h4><code>dataset.has_scoreddocs() -> bool</code></h4>

<div class="methodinfo">
<p>Returns <code class="kwd">True</code> if this dataset supports <code>dataset.scoreddocs_*</code> methods.</p>
</div>

<h4><code>dataset.has_docpairs() -> bool</code></h4>

<div class="methodinfo">
<p>Returns <code class="kwd">True</code> if this dataset supports <code>dataset.docpairs_*</code> methods.</p>
</div>


<h4><code>iter(dataset.docs) -> iter[namedtuple]</code></h4>

<div class="methodinfo">
<p>Returns an iterator of <code>namedtuple</code>s, where each item is a document in the collection.</p>
</div>

<h4><code>len(dataset.docs) -> int</code></h4>

<div class="methodinfo">
<p>Returns the number of documents in the collection.</p>
</div>

<h4><code>dataset.docs[start:stop:skip] -> iter[namedtuple]</code></h4>

<div class="methodinfo">
<p>Returns an iterator of <code>namedtuple</code>s by index, specified by the slice given.</p>

<code class="example">
<div class="comment"># First 10 documents</div>
<div>dataset.docs[:10]</div>

<div class="comment"># Last 10 documents</div>
<div>dataset.docs[-10:]</div>

<div class="comment"># Every 2 documents</div>
<div>dataset.docs[::2]</div>

<div class="comment"># Every 2 documents, starting with the first document</div>
<div>dataset.docs[1::2]</div>

<div class="comment"># The first half of the collection</div>
<div>dataset.docs[:1/2]</div>

<div class="comment"># The middle third of collection</div>
<div>dataset.docs[1/3:2/3]</div>
</code>

<p>
Note that the fancy slicing mechanics are faster and more sophisticated than
<code>itertools.islice</code>; documents are not processed if they are skipped.
</p>
</div>

<h4><code>dataset.docs.type -> type</code></h4>

<div class="methodinfo">
<p>
Returns the <code class="kwd">NamedTuple</code> type that the <code>docs_iter</code> returns.
The available fields and type information can be found with <code>_fields</code> and <code>__annotations__</code>:
</p>

<code class="example">
<div>dataset.docs.type._fields</div>
</code>
<code class="output">
<div>(<span class="str">'doc_id'</span>, <span class="str">'title'</span>, <span class="str">'doi'</span>, <span class="str">'date'</span>, <span class="str">'abstract'</span>)</div>
</code>
<code class="example">
<div>dataset.docs.type.__annotations__</div>
</code>
<code class="output">
<div>{{</div>
<div>&nbsp;&nbsp;<span class="str">'doc_id'</span>: <span class="kwd">str</span>,</div>
<div>&nbsp;&nbsp;<span class="str">'title'</span>: <span class="kwd">str</span>,</div>
<div>&nbsp;&nbsp;<span class="str">'doi'</span>: <span class="kwd">str</span>,</div>
<div>&nbsp;&nbsp;<span class="str">'date'</span>: <span class="kwd">str</span>,</div>
<div>&nbsp;&nbsp;<span class="str">'abstract'</span>: <span class="kwd">str</span></div>
<div>}}</div>
</code>
</div>


<h4><code>dataset.docs.lookup(doc_ids) -> Dict[str, namedtuple]</code></h4>

<div class="methodinfo">
<p>
Returns a dictionary mapping all doc_ids found in the collection to their contents.
</p>
</div>

<h4><code>dataset.docs.lookup_iter(doc_ids) -> Iterable[namedtuple]</code></h4>

<div class="methodinfo">
<p>
Returns an iterable of all docs associated with the specified doc_ids found in the collection.
</p>
</div>

<h4><code>dataset.docs.lang -> str</code></h4>

<div class="methodinfo">
<p>
Returns the two-character <a href="https://en.wikipedia.org/wiki/ISO_639-1">ISO 639-1
language code</a> (e.g., <span class="str">"en"</span> for English) of the documents in this collection.
Returns <span class="kwd">None</span> if there are multiple languages, a language not represented by an
ISO 639-1 code, or the language is otherwise unknown.
</p>
</div>


<h4><code>iter(dataset.queries) -> iter[namedtuple]</code></h4>

<div class="methodinfo">
<p>
Returns an iterator over namedtuples representing queries in the dataset.
</p>
</div>

<h4><code>len(dataset.queries) -> int</code></h4>

<div class="methodinfo">
<p>
Returns the number of queries in the dataset.
</p>
</div>


<h4><code>dataset.queries.type -> type</code></h4>

<div class="methodinfo">
<p>
Returns the type of the namedtuple returned by <code>iter(queries)</code>,
including <code>_fields</code> and <code>__annotations__</code>.
</p>
</div>


<h4><code>dataset.queries.lang -> str</code></h4>

<div class="methodinfo">
<p>
Returns the two-character <a href="https://en.wikipedia.org/wiki/ISO_639-1">ISO 639-1
language code</a> (e.g., <span class="str">"en"</span> for English) of the queries.
Returns <span class="kwd">None</span> if there are multiple languages, a language not represented by an
ISO 639-1 code, or the language is otherwise unknown. Note that some datasets include
translations as different query fields.
</p>
</div>

<h4><code>dataset.queries.lookup(query_ids) -> Dict[str, namedtuple]</code></h4>

<div class="methodinfo">
<p>
Returns a dictionary mapping all query_ids found in the dataset to their contents.
</p>
</div>

<h4><code>dataset.queries.lookup_iter(query_ids) -> Iterable[namedtuple]</code></h4>

<div class="methodinfo">
<p>
Returns an iterable of all docs associated with the specified query_ids found in the dataset.
</p>
</div>


<h4><code>iter(dataset.qrels) -> iter[namedtuple]</code></h4>

<div class="methodinfo">
<p>
Returns an iterator over namedtuples representing query relevance assessments in the dataset.
</p>
</div>

<h4><code>len(dataset.qrels) -> int</code></h4>

<div class="methodinfo">
<p>
Returns the numer of qrels in the dataset.
</p>
</div>

<h4><code>dataset.qrels.type -> type</code></h4>

<div class="methodinfo">
<p>
Returns the type of the namedtuple returned by <code>qrels_iter</code>,
including <code>_fields</code> and <code>__annotations__</code>.
</p>
</div>


<h4><code>dataset.qrels.defs -> dict[int, str]</code></h4>

<div class="methodinfo">
<p>
Returns a mapping between relevance levels and a textual description of
what the level represents. (E.g., 0 represting not relevant, 1 representing
possibly relevant, 2 representing definitely relevant.)
</p>
</div>


<h4><code>dataset.qrels.asdict() -> dict[str, dict[str, int]]</code></h4>

<div class="methodinfo">
<p>
Returns a dict of dicts representing all qrels for this collection. Note
that this will load all qrels into memory. The outer dict key is the
<code>query_id</code> and the inner key is the <code>doc_id</code>.
This is useful in tools such as <a href="https://github.com/cvangysel/pytrec_eval">pytrec_eval</a>.
</p>
</div>


<h4><code>iter(dataset.scoreddocs) -> iter[namedtuple]</code></h4>

<div class="methodinfo">
<p>
Returns an iterator over namedtuples representing scored docs (e.g., initial rankings
for re-ranking tasks) in the dataset.
</p>
</div>


<h4><code>dataset.scoreddocs.type -> type</code></h4>

<div class="methodinfo">
<p>
Returns the type of the namedtuple returned by <code>scoreddocs_iter</code>,
including <code>_fields</code> and <code>__annotations__</code>.
</p>
</div>


<h4><code>iter(dataset.docpairs) -> iter[namedtuple]</code></h4>

<div class="methodinfo">
<p>
Returns an iterator over namedtuples representing doc pairs (e.g., training pairs) in the dataset.
</p>
</div>


<h4><code>dataset.docpairs.type -> type</code></h4>

<div class="methodinfo">
<p>
Returns the type of the namedtuple returned by <code>docpairs_iter</code>,
including <code>_fields</code> and <code>__annotations__</code>.
</p>
</div>
''')


def generate_cli_docs(out_dir, version):
    with page_template('cli.html', out_dir, version, title='Command Line Interface') as out:
        out.write(f'''
<h2 id="export">export command</h2>

<p>
Data can be exported to stdout in various formats using the <code>ir_datasets export</code> command.
</p>

<h4><code>ir_datasts export [dataset-id] docs [--fields] [--format]</code></h4>

<div class="methodinfo">
<p>Exports documents</p>
<p><code>--fields</code>: select which fields from the document to export (defaults to all)</p>
<p><code>--format</code>: select output format to use: <code>tsv</code> (default) or <code>jsonl</code></p>
</div>

<h4><code>ir_datasts export [dataset-id] queries [--fields] [--format]</code></h4>

<div class="methodinfo">
<p>Exports queries</p>
<p><code>--fields</code>: select which fields from the query to export (defaults to all)</p>
<p><code>--format</code>: select output format to use: <code>tsv</code> (default) or <code>jsonl</code></p>
</div>

<h4><code>ir_datasts export [dataset-id] qrels [--fields] [--format]</code></h4>

<div class="methodinfo">
<p>Exports queries</p>
<p><code>--fields</code>: select which fields from the qrels to export (defaults to all)</p>
<p><code>--format</code>: select output format to use: <code>trec</code>  (default), <code>tsv</code> or <code>jsonl</code></p>
</div>

<h4><code>ir_datasts export [dataset-id] scoreddocs [--fields] [--format]</code></h4>

<div class="methodinfo">
<p>Exports queries</p>
<p><code>--fields</code>: select which fields from the scoreddocs to export (defaults to all)</p>
<p><code>--format</code>: select output format to use: <code>trec</code>  (default), <code>tsv</code> or <code>jsonl</code></p>
</div>

<h2 id="export">lookup command</h2>

<p>
You can look up documents by their <code>doc_id</code> using the <code>ir_datasets lookup</code> command.
</p>

<h4><code>ir_datasts lookup [dataset-id] [doc_ids ...] [--fields] [--format]</code></h4>

<div class="methodinfo">
<p>Efficiently finds documents that have the provided doc_ids</p>
<p><code>--fields</code>: select which fields from the documents to export (defaults to all)</p>
<p><code>--format</code>: select output format to use: <code>trec</code>  (default), <code>tsv</code> or <code>jsonl</code></p>
</div>


<h2 id="export">doc_fifo command</h2>

<p>
You can create output FIFOs suitable for Anserini indexing using the <code>ir_datasets doc_fifo</code> command.
</p>

<p>
Note that unlike export and lookup, these always output as JSONL in a format that Anserini can use to index
(id and content fields). All selected fields are concatenated.
</p>

<p>
This command will output a command you can run for indexing with Anserini. This process remains running
until all documents are sent to fifos.
</p>

<h4><code>ir_datasts doc_fifos [dataset-id] [--fields] [--count]</code></h4>

<div class="methodinfo">
<p>Creates a temporary directory with fifos</p>
<p><code>--fields</code>: select which fields from the documents to export (defaults to all). These fields are concatenated.</p>
<p><code>--count</code>: how many fifos to make? Defualts to 1 less than the number of processors (or 1).</p>
<p><code>--dir</code>: where to put the fifos? Defaults to a new temp directory.</p>
</div>
''')


def generate_downloads(out_dir, version):
    with page_template('downloads.html', out_dir, version, title='Download dashboard') as out:
        out.write('''
<div id="Downloads">
</div>
<script type="text/javascript">
$(function () {
    $.ajax({
        'url': 'https://smac.pub/irdsdlc'
    }).done(function (data) {
        $.each(data, function (ds, downloads) {
            $('#Downloads').append(generateDownloads(ds, downloads));
        });
    });
});
</script>
''')


@contextmanager
def page_template(file, base_dir, version, title=None, source=None):
    with open(get_file_path(base_dir, version, file), 'wt') as out:
        no_index = '<meta name="robots" content="noindex,nofollow" />' if version else ''
        out.write(f'''<!DOCTYPE html>
<html>
<head>
<link rel="stylesheet" href="main.css" />
<script src="https://code.jquery.com/jquery-1.12.4.min.js" integrity="sha256-ZosEbRLbNQzLpnKIkEdrPv7lOy9C27hHQ+Xp8a4MxAQ=" crossorigin="anonymous"></script>
<script src="https://code.jquery.com/ui/1.12.1/jquery-ui.min.js" integrity="sha256-VazP97ZCwtekAsvgPBSUwPFKdrwD3unUfSGVYrahUqU=" crossorigin="anonymous"></script>
<script src="main.js"></script>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
{no_index}
<title>{title + ' - ir_datasets' if title else 'ir_datasets'}</title>
<body>
<div class="page">
''')
        path_segment = 'blob' if source else 'tree'
        url = 'https://github.com/allenai/ir_datasets/' + (f'{path_segment}/{version or ("v" + ir_datasets.__version__)}/' if version else '') + (f'ir_datasets/{source}' if source else '')
        text = source or 'allenai/ir_datasets'
        if version: # a specific version -- warn the user
            out.write(f'''
<div class="banner">This documentation is for <strong>{version}</strong>. See <a href="../{file}">here</a> for documentation of the current latest version on pypi.</div>
''')
        if file != 'index.html':
            out.write(f'''
<div style="position: absolute; top: 4px; left: 4px;"><a href="index.html">&larr; home</a></div>
''')
        out.write(f'''
<div style="position: absolute; top: 4px; right: 4px;">Github: <a href="{url}">{text}</a></div>
''')
        out.write(f'<h1><code>ir_datasets</code>{": " + title if title else ""}</h1>')
        yield out
        out.write(f'''
</div>
</body>
</html>
''')




def generate_ghdl(top_level):
    # Not really documentation, but it was easy to throw that in here
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
        python -m test.downloads --filter "^{top}/" --output "{top}.json" --randdelay 60
    - name: Upload artifact
      if: always() # don't skip if the Test step fails (defeats the whole purpose)
      uses: actions/upload-artifact@v2
      with:
        name: "{top}.json"
        path: "{top}.json"
''')



def generate_css(base_dir, version):
    with open(get_file_path(base_dir, version, 'main.css'), 'wt') as out:
        out.write(r'''
@import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;700&display=swap');
body {
  font-family: "Roboto", Arial, sans-serif;
  font-size: 14px;
  background-color: #eee;
  padding: 0;
  margin: 0;
  line-height: 1.3;
}
.dataset {
  margin: 24px 0;
}
.str {
  color: #710000;
  font-weight: bold;
}
.kwd {
  color: #4287f5;
  font-weight: bold;
}
.comment {
  color: #288642;
}
.tag {
  border-radius: 10px;
  color: white;
  background-color: black;
  font-size: 10px;
  padding: 1px 5px;
}
.tag-docs { background-color: #0b83d9; }
.tag-queries { background-color: #2da608; }
.tag-qrels { background-color: #bf7000; }
.tag-scoreddocs { background-color: #9e5bd9; }
.tag-docpairs { background-color: #06a892; }
p, blockquote {
  margin: 6px 0;
  color: #333;
}
blockquote {
  border-left: 3px solid #a5bcd1;
  padding-left: 4px;
  margin-left: 6px;
}
.desc {
  color: #333;
}
.ds-ref {
  font-family: monospace;
  color: #444;
  font-weight: bold;
  cursor: pointer;
}
.page {
  position: relative; /* allow absolute position within page */
  background-color: #fff;
  max-width: 800px;
  margin: 8px auto;
  padding: 8px 24px;
  box-shadow: 0 0 8px #999;
  border: 1px solid #ddd;
}
.showhide {
    cursor: pointer;
    background: #ddd;
    padding: 2px 8px;
    font-size: 11px;
    color: #333;
}
.showhide:hover {
    background: #ccc;
}
.showhide.shown::before {
  content: '▼ hide ';
}
.showhide.hidden::before {
  content: '► show ';
}
ul {
  margin: 4px;
  padding-left: 16px;
}
.ds-name {
  font-size: 16px;
}
hr {
  border: none;
  border-top: 1px solid #888;
  margin: 24px -24px;
}
cite {
  border: 1px dotted #444;
  white-space: pre;
  overflow: auto;
  font-family: monospace;
  font-style: normal;
  display: block;
  padding: 4px;
  margin: 4px 0;
}
.banner {
    padding: 24px 16px 8px 16px;
    background-color: #ffc150;
    margin-top: -8px;
    margin-left: -24px;
    margin-right: -24px;
    text-align: center;
}

.tab-content {
  border: 1px solid #666666;
  margin: 0 4px 4px 4px;
  padding: 4px;
}

.tab {
  border: 1px solid #666666;
  margin: 4px 4px 0 4px;
  padding: 0 4px;
  display: inline-block;
  cursor: pointer;
  background-color: #666;
  color: white;
  line-height: 1.5;
}

.tab.selected {
  color: black;
  background-color: white;
  border-bottom: 1px solid white;
  position: relative;
  bottom: -1px;
}

code.example {
  display: block;
  border-left: 4px solid #4287f5;
  padding-left: 6px;
  margin-left: 4px;
  margin-top: 2px;
  margin-bottom: 2px;
}
code.output {
  display: block;
  border-left: 4px solid #32a852;
  padding-left: 6px;
  margin-left: 4px;
  margin-top: 2px;
  margin-bottom: 2px;
}

.type {
  border: 1px solid black;
  font-family: monospace;
  display: inline-block;
  padding: 4px 8px;
  border-radius: 4px;
  margin: 4px;
  background-color: rgba(66, 135, 245, 0.1);
  vertical-align: middle;
}

.type-name {
  font-weight: bold;
}

.type-fields {
  margin: 0;
  padding: 0;
  list-style: none;
}

.type-fields > li::before {
  content: "[" attr(data-tuple-idx) "] ";
  margin-left: 2px;
}

.index {
  list-style: none;
  margin: 0;
  padding: 0;
  padding-left: 8px;
}

.prefix {
  color: #777;
}

.index a {
  text-decoration: none;
}

.index a:hover {
  text-decoration: underline;
}

.lang-code {
  display: inline-block;
  border-radius: 6px;
  padding: 2px 4px;
  background-color: #4287f5;
  color: white;
  font-family: monospace;
}

td, th {
  text-align: left;
  padding: 0 8px;
}

th {
  background-color: #4287f5;
  color: white;
  text-align: left;
  padding: 0 8px;
}

tr:nth-child(2n+1) {
  background-color: #EEE;
}

tbody tr:nth-child(2n + 1) {
  background-color: #FFF;
}

tbody tr:nth-child(2n) {
  background-color: #EEE;
}

tbody tr:first-child td {
    padding-top: 12px;
}

tbody tr:last-child td {
    border-bottom: 2px solid #333;
}

.relScore {
  font-family: monospace;
  text-align: center;
}

.center {
  text-align: center;
}

.sep {
  border-top: 3px solid #333;
}

.warn {
  color: white;
  background-color: #c17300;
  padding: 8px 16px;
  border-radius: 8px;
}

.warn::before {
  content: "Warning: ";
  font-weight: bold;
}

h4 {
  margin-bottom: 0px;
}

.methodinfo {
  margin-left: 16px;
}

.stick-top {
  position: sticky;
  top: 0;
}

details {
    margin: 8px 0;
}

#DataAccess {
    border: 1px solid #91b6ca;
    margin: 8px;
    padding: 6px;
    background-color: #eff8fc;
    border-radius: 4px;
}

#DataAccess h3 {
    margin: 3px 0;
}

#Downloads {
    min-height: 21px;
    margin: 8px 0;
}

.screen-small-show {
    display: none;
}

@media screen and (max-width: 500px){
  .page {
    padding: 6px;
    margin: 0;
    border: 0;
  }
  h1 {
    margin-bottom: 2px;
  }
  h3 {
    margin-bottom: 4px;
  }
  hr {
    margin: 8px -6px;
  }
  .dataset {
    margin: 6px 0;
  }
  .screen-small-hide {
    display: none;
  }
  .screen-small-show {
    display: inherit;
  }
  .banner {
    margin-top: -6px;
    margin-left: -6px;
    margin-right: -6px;
  }
}
''')


def generate_js(base_dir, version):
    with open(get_file_path(base_dir, version, 'main.js'), 'wt') as out:
        out.write(r'''
function scrollIntoViewIfNeeded(target) { 
    if (target.getBoundingClientRect().bottom > window.innerHeight) {
        target.scrollIntoView(false);
    }

    if (target.getBoundingClientRect().top < 0) {
        target.scrollIntoView();
    } 
}
function selectText(element) {
    if (document.selection) { // IE
        var range = document.body.createTextRange();
        range.moveToElementText(element);
        range.select();
    } else if (window.getSelection) {
        var range = document.createRange();
        range.selectNode(element);
        window.getSelection().removeAllRanges();
        window.getSelection().addRange(range);
    }
}
$(document).ready(function() {
    $('.ds-ref').click(function () {
        var target = $('[id="' + $(this).text() + '"]');
        target.effect("highlight", {}, 1000);
        scrollIntoViewIfNeeded(target[0]);
    });
    var left = 0, top = 0;
    $(document).on('mousedown', '.select', function(e) {left = e.pageX; top = e.pageY;});
    $(document).on('mouseup', '.select', function(e) { if (left == e.pageX && top == e.pageY) {selectText(this);} });
    $(document).on('click', '.jumpto', function(e) {
        var target = $('[id="' + $(e.target).attr('href').substr(1) + '"]');
        target.effect("highlight", {}, 1000);
        scrollIntoViewIfNeeded(target[0]);
        return false;
    });
    $('.tabs').each(function (i, e) {
        var first = $(e).find('.tab-content:first');
        $(e).find('.tab-content').hide();
        first.show();
        $(e).find('.tab:first').addClass('selected');
        $(e).find('.tab').prependTo(e);
    });
    $(document).on('click', '.tab', function(e) {
        var $target = $(e.target);
        var tabs = $target.closest('.tabs');
        tabs.find('.tab-content').hide();
        $('[id="'+$target.attr('target')+'"]').show();
        tabs.find('.tab.selected').removeClass('selected');
        $target.addClass('selected');
    });
    $('#DatasetJump').change(function () {
        var targetRow = $('#DatasetJump').val();
        if (targetRow) {
            $('#' + targetRow)[0].scrollIntoView();
            $('#DatasetJump').val(''); // clear selection
        }
    });
});
function toEmoji(test) {
    if (test) {
        return '✅';
    }
    return '❌';
}
function toTime(duration) {
    if (duration < 60) {
        return duration.toFixed(2) + 's';
    }
    var minutes = Math.floor(duration / 60);
    var seconds = duration % 60;
    return minutes.toFixed(0) + 'm ' + seconds.toFixed(0) + 's';
}
function toFileSize(size) {
    if (!size) {
        return '';
    }
    var unit = 'B';
    var units = ['KB', 'MB', 'GB'];
    while (units.length > 0 && size > 1000) {
        size = size / 1000;
        unit = units.shift();
    }
    if (unit === 'B') {
        size = size.toFixed(0);
    } else {
        size = size.toFixed(1);
    }
    return size + ' ' + unit;
}
function generateDownloads(title, downloads) {
    if (downloads.length === 0) {
        return $('<div></div>');
    }
    var allGood = true;
    var $content = $('<table></table>');
    $content.append($('<tr><th>Avail</th><th>Download ID</th><th>Size</th><th>Time</th><th>Last Tested At</th><th>Expected MD5 Hash</th></tr>'));
    var goodCount = 0;
    var totalCount = 0;
    $.each(downloads, function (i, dl) {
        var good = dl.result === "PASS";
        totalCount += 1;
        if (!good) {
            allGood = false;
        } else {
            goodCount += 1;
        }
        $content.append($('<tr></tr>')
            .append($('<td></td>').text(toEmoji(good)).attr('title', dl.result).css('text-align', 'center'))
            .append($('<td></td>').append($('<a></a>').attr('href', dl.url).text(dl.name)))
            .append($('<td></td>').text(toFileSize(dl.size)))
            .append($('<td></td>').text(toTime(dl.duration)))
            .append($('<td></td>').text(dl.time.substring(0, 19).replace('T', ' ')))
            .append($('<td></td>').append($('<code>').text(dl.md5)))
        );
    });
    return $('<details></details>')
        .append($('<summary></summary>').text(toEmoji(allGood) + ' ' + title + ' (' + goodCount.toString() + ' of ' + totalCount.toString() + ')'))
        .append($('<p>These files are automatically downloaded by ir_datasets as they are needed. We also periodically check that they are still available and unchanged through an automated <a href="https://github.com/allenai/ir_datasets/actions/workflows/verify_downloads.yml">GitHub action</a>. The latest results from that test are shown here:</p>'))
        .append($content)
        .prop('open', !allGood);
}
''')


def get_file_path(base_dir, version, file):
    return f'{base_dir}/{version}/{file}' if version else f'{base_dir}/{file}'


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

def emoji(ds, arg, top_level):
    has = getattr(ds, f'has_{arg}')()
    if has:
        instructions = hasattr(ds, f'documentation') and ds.documentation().get(f'{arg}_instructions')
        if instructions:
            return f'<a href="{top_level}.html#DataAccess" title="{instructions}. Click for details.">⚠️</a>'
        return f'<span style="cursor: help;" title="{arg} available as automatic download">✅</span>'
    return ''


def _lang(lang_code):
    if lang_code is None:
        return '<em>multiple/other/unknown</em>'
    return f'<span class="lang-code">{lang_code}</span>'


if __name__ == '__main__':
    main(sys.argv[1:])
