import math
import os
import multiprocessing
import ir_datasets


_logger = ir_datasets.log.easy()


def bs4_extract(html):
    ignore = {'[document]', 'noscript', 'header', 'html', 'meta', 'head', 'input', 'script', 'style'}
    bs4 = ir_datasets.lazy_libs.bs4()
    soup = bs4.BeautifulSoup(html, 'html.parser')
    output = ''
    for t in soup.find_all(text=True):
        if t.parent.name not in ignore and not isinstance(t, bs4.element.Comment):
            output += '{} '.format(t)
    return output


class HtmlDocExtractor:
    def __init__(self, dataset, extractor='bs4', parallel=0.8):
        self._dataset = dataset
        self._extractor = extractor
        if isinstance(parallel, int) and parallel < 0:
            parallel = os.cpu_count() + parallel
        if isinstance(parallel, float):
            parallel = math.floor(os.cpu_count() * parallel)
        if parallel < 1:
            parallel = 1
        self._parallel = parallel
        docs_cls = self._dataset.docs_cls()
        fields = {f: i for i, f in enumerate(docs_cls._fields)}
        field_content_type = []
        for field, i in fields.items():
            if field.endswith('_content_type'):
                base_field = field[:-len('_content_type')]
                if base_field in fields:
                    field_content_type.append((fields[base_field], i))
        self._field_content_type = field_content_type
        if len(field_content_type) == 0:
            _logger.warn('no fields with _content_type found; HtmlDocExtractor will have not effect.')

    def __getattr__(self, attr):
        return getattr(self._dataset, attr)

    def docs_iter(self):
        docs_cls = self._dataset.docs_cls()
        arg_iter = ((doc, self._field_content_type, self._extractor, docs_cls) for doc in self._dataset.docs_iter())

        if self._parallel == 1:
            yield from map(_doc_map, arg_iter)
        else:
            with multiprocessing.Pool(self._parallel) as pool:
                yield from pool.imap(_doc_map, arg_iter)

def _doc_map(args):
    doc, field_content_type, extractor, docs_cls = args
    extractor = {
        'bs4': bs4_extract
    }[extractor]
    result = list(doc)
    any_updates = False
    for content_idx, type_idx in field_content_type:
        if result[type_idx] in ('text/html', 'application/xhtml+xml'):
            result[content_idx] = extractor(result[content_idx])
            result[type_idx] = 'text/plain'
            any_updates = True
    if any_updates:
        doc = docs_cls(*result)
    return doc
