import unittest
from ir_datasets.datasets.wapo import WapoDocs


class _FakeWapoDocs(WapoDocs):
    def __init__(self, raw_docs):
        super().__init__(None, 'unused')
        self._raw_docs = raw_docs

    def docs_wapo_raw_iter(self):
        yield from self._raw_docs


def _raw_doc(paragraphs):
    return {
        'id': 'doc1',
        'article_url': 'https://example.com/doc1',
        'title': 'Title',
        'author': 'Author',
        'published_date': 1325376562000,
        'contents': [
            {'type': 'sanitized_html', 'mime': 'text/html', 'content': p}
            for p in paragraphs
        ],
    }


class TestWapoDocs(unittest.TestCase):
    def _doc(self, paragraphs):
        (doc,) = _FakeWapoDocs([_raw_doc(paragraphs)])._docs_iter()
        return doc

    def test_html_paragraph_starting_with_text_keeps_all_text(self):
        para = 'Set down your champagne and gaze west, as the bubbly planet <b>Venus</b> kicks off Sunday night’s first evening of the New Year.'
        doc = self._doc([para])
        self.assertEqual(
            'Set down your champagne and gaze west, as the bubbly planet Venus kicks off Sunday night’s first evening of the New Year.',
            doc.body)
        self.assertEqual((para,), doc.body_paras_html)

    def test_html_paragraph_with_several_tags_and_entities(self):
        doc = self._doc([
            'First <a href="https://example.com/x">linked</a> and <i>italic</i> text &amp; more.',
            '<span class="dateline">NEW YORK</span> — Second paragraph.',
        ])
        self.assertEqual(
            'First linked and italic text & more.\nNEW YORK — Second paragraph.',
            doc.body)


if __name__ == '__main__':
    unittest.main()
