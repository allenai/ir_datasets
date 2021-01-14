import math
import os
import multiprocessing
from threading import Semaphore
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


class HtmlDocIter:
    def __init__(self, it, extractor):
        self.it = it
        self.extractor = extractor
        self.mapped_it = _doc_map_it(it, self.extractor)

    def __next__(self):
        return next(self.mapped_it)

    def __iter__(self):
        return self

    def __getitem__(self, key):
        if isinstance(key, int):
            doc = self.it[key]
            return _doc_map((doc, self.extractor._field_content_type, self.extractor._extractor, self.extractor._dataset.docs_cls()))
        return HtmlDocIter(self.it[key], self.extractor)


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
        return HtmlDocIter(self._dataset.docs_iter(), self)

    def docs_store(self):
        return HtmlDocExtractorDocStoreWrapper(self._dataset.docs_store(), self)


class HtmlDocExtractorDocStoreWrapper(ir_datasets.indices.Docstore):
    def __init__(self, docstore, extractor):
        self.docstore = docstore
        self.extractor = extractor

    def get_many_iter(self, doc_ids):
        return _doc_map_it(self.docstore.get_many_iter(doc_ids), self.extractor)

    def clear_cache(self):
        self.docstore.clear_cache()



def _doc_map_it(it, extractor):
    docs_cls = extractor._dataset.docs_cls()
    arg_iter = ((doc, extractor._field_content_type, extractor._extractor, docs_cls) for doc in it)

    if extractor._parallel == 1:
        return map(_doc_map, arg_iter)

    # By default, pool.imap is super greedy and will read in as much of it as possible.
    # This could mean loading too much into memory, or simply doing extra work. So instead,
    # we do two things:
    #  1) Wait until the first call to next() before sending *anything* to the pool, and
    #  2) Limit the imap buffer to a maximum size by only releasing data from the source iter
    #     when the buffer has space.
    semaphore = Semaphore(extractor._parallel)
    def it_in():
        while semaphore.acquire():
            result = next(arg_iter, StopIteration)
            if result is StopIteration:
                break
            yield result

    def it_out():
        # thi will only start with the first call to next()
        with multiprocessing.Pool(extractor._parallel) as pool:
            for result in pool.imap(_doc_map, it_in()):
                semaphore.release() # allow next item to begin processing
                yield result

    return it_out()


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
