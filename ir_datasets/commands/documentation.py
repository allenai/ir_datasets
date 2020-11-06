import sys
import argparse
import ir_datasets


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets documentation', description='Generates documentation files.')
    parser.add_argument('--out_dir', default='./ir_datasets/docs')
    args = parser.parse_args(args)
    out_dir = args.out_dir
    out = open(f'{out_dir}/datasets.html', 'wt')
    out.write('''
<!DOCTYPE html>
<html>
<head>
<style>
@font-face {
  font-family: 'Titillium Web';
  font-style: normal;
  font-weight: 400;
  src: local('Titillium Web'), local('TitilliumWeb-Regular'), 
url("https://fonts.gstatic.com/s/titilliumweb/v4/7XUFZ5tgS-tD6QamInJTcRnhefaTZMTJ3r9AXhRp6aM.woff2") format('woff2');
  unicode-range: U+0000-00FF, U+0131, U+0152-0153, U+02C6, U+02DA, U+02DC, U+2000-206F, U+2074, U+20AC, U+2212, U+2215, U+E0FF, U+EFFD, U+F000;
}
@font-face {
  font-family: 'Titillium Web';
  font-style: normal;
  font-weight: 600;
  src: local('Titillium Web SemiBold'), local('TitilliumWeb-SemiBold'), url(https://fonts.gstatic.com/s/titilliumweb/v8/NaPDcZTIAOhVxoMyOr9n_E7ffBzCGItzY5abuWI.woff2) format('woff2');
  unicode-range: U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+2000-206F, U+2074, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD;
}
body {
  font-family: "Titillium Web", Calibri, Arial, sans-serif;
  font-size: 13px;
  background-color: #eee;
  padding: 0;
  margin: 0;
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
  margin: 4px 0;
  color: #666;
}
blockquote {
  border-left: 3px solid #a5bcd1;
  padding-left: 4px;
  margin-left: 6px;
}
.desc {
  color: #666;
}
.ds-ref {
  font-family: monospace;
  color: #444;
  font-weight: bold;
  cursor: pointer;
}
.page {
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
  overflow: scroll;
  font-family: monospace;
  font-style: normal;
  display: block;
  padding: 4px;
  margin: 4px 0;
}
.tag {
  cursor: pointer;
}
#Popup {
  display: none;
  position: fixed;
  left: 50%;
  top: 100px;
  width: 400px;
  background: white;
  border: 1px solid black;
  padding: 8px;
  margin-left: -200px;
}
#CodeSample {
  width: 100%;
  overflow: auto;
  white-space: nowrap;
}
#ClosePopup {
  position: absolute;
  top: 0px;
  right: 0;
  font-size: 18px;
  cursor: pointer;
  color: #444;
  width: 27px;
  text-align: center;
}
#ClosePopup:hover {
  color: black;
}
#Backdrop {
  display: none;
  background-color: black;
  opacity: 0.15;
  position: fixed;
  top: 0; left: 0; bottom: 0; right: 0;
}
.jumpto {
    padding: 0 4px;
    display: inline-block;
}
</style>
<script src="https://code.jquery.com/jquery-1.12.4.min.js" integrity="sha256-ZosEbRLbNQzLpnKIkEdrPv7lOy9C27hHQ+Xp8a4MxAQ=" crossorigin="anonymous"></script>
<script src="https://code.jquery.com/ui/1.12.1/jquery-ui.min.js" integrity="sha256-VazP97ZCwtekAsvgPBSUwPFKdrwD3unUfSGVYrahUqU=" crossorigin="anonymous"></script>
<script type="application/javascript">
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
    $('.showhide').click(function () {
        var query = '[data-parent]';
        if (!$(this).is('.all')) {
            query = '[data-parent="' + $(this).closest('.dataset').attr('id') + '"]';
        }
        var isHidden = $(this).hasClass('hidden');
        $(query).toggle(isHidden);
        $(this).toggleClass('shown hidden');
        if ($(this).is('.all')) {
            $('.showhide').toggleClass('shown', isHidden);
            $('.showhide').toggleClass('hidden', !isHidden);
        }
    });
    $('.showhide.all').click();
    $('.ds-ref').click(function () {
        var target = $('[id="' + $(this).text() + '"]');
        target.effect("highlight", {}, 1000);
        scrollIntoViewIfNeeded(target[0]);
    });
    $('.tag[data-fields]').click(function() {
        var dsName = $(this).closest('.dataset').find('.ds-name').text();
        var fields = $(this).attr('data-fields');
        var type = $(this).text();
        $('#CodeSample').html('<code class="select"><div><span class="kwd">import</span> ir_datasets</div><div>dataset = ir_datasets.load(<span class="str">'+dsName+')</div><div><span class="kwd">for</span> ' + fields + ' <span class="kwd">in</span> dataset.' + type + '_iter():</div><div>&nbsp;&nbsp;&nbsp;&nbsp;<span class="kwd">pass</span></div></code>');
        $('#Popup,#Backdrop').show();
    });
    $('#Backdrop,#ClosePopup').click(function() {
        $('#Popup,#Backdrop').hide();
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
});
</script>
<body>
<div id="Backdrop"></div>
<div id="Popup">
<div id="ClosePopup">✖</div>
<div id="CodeSample"></div>
</div>
<div class="page">
<h1><code>ir_datasets</code></h1>
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
Want a new dataset, added functionality, or a bug fixed? Feel free to post an issue or make a pull
request!
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
<p>Jump to: {jump_list}</p>
<span class="showhide shown all">all subsets</span>
''')
    for name in sorted(ir_datasets.registry):
        dataset = ir_datasets.registry[name]
        parent = name.split('/')[0]
        if parent == name:
            parent = ''
            showhide = '<span class="showhide shown">subsets</span>'
            hr = '<hr />'
        else:
            parent = f' data-parent="{parent}"'
            showhide = ''
            hr = ''
        tags = []
        if dataset.has_docs():
            tags.append(('docs', dataset.docs_cls()))
        if dataset.has_queries():
            tags.append(('queries', dataset.queries_cls()))
        if dataset.has_qrels():
            tags.append(('qrels', dataset.qrels_cls()))
        if dataset.has_scoreddocs():
            tags.append(('scoreddocs', dataset.scoreddocs_cls()))
        if dataset.has_docpairs():
            tags.append(('docpairs', dataset.docpairs_cls()))
        tags = ' '.join(f'<span class="tag tag-{t}" data-fields="{", ".join(c._fields)}">{t}</span>' for t, c in tags)
        if hasattr(dataset, 'desc'):
            desc = dataset.desc()
        else:
            desc = '<p><i>(no description provided)</i></p>'
        if hasattr(dataset, 'bibtex'):
            bibtex = f'<cite class="select">{dataset.bibtex()}</cite>'
        else:
            bibtex = ''
        out.write(f'''
{hr}
<div class="dataset" id="{name}"{parent}>
<code class="ds-name select"><span class="str">{repr(name)}</span></code>
<div>Provides: {tags}</div>
<div class="desc">
{desc}
{bibtex}
</div>
{showhide}
</div>
''')
    out.write(f'''
</div>
</body>
</html>
''')
    out.flush()
    out.close()


if __name__ == '__main__':
    main(sys.argv[1:])
