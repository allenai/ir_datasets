from unittest import main
from re import compile

from ir_datasets.datasets.msmarco_document import MsMarcoAnchorTextDocument
from ir_datasets.datasets.msmarco_document_v2 import MsMarcoV2AnchorTextDocument
from test.integration.base import DatasetIntegrationTest

class TestMsMarcoAnchorText(DatasetIntegrationTest):
    # noinspection PyTypeChecker
    def test_docs(self):
        self._test_docs("msmarco-document/anchor-text", count=1703834, items={
            0: MsMarcoAnchorTextDocument('D2292456', 'Database Administrator Database Administrator Database Administrator', ['Database Administrator', 'Database Administrator', 'Database Administrator']),
            1703833: MsMarcoAnchorTextDocument('D3498137', 'Legal Dictionary derail Legal Dictionary derail derail derail derail derail derail derail derail derail Legal Dictionary derail Legal Legal derail Legal Dictionary derail derail derail derail derail Legal Legal derail derail', ['Legal Dictionary', 'derail', 'Legal Dictionary', 'derail', 'derail', 'derail', 'derail', 'derail', 'derail', 'derail', 'derail', 'derail', 'Legal Dictionary', 'derail', 'Legal', 'Legal', 'derail', 'Legal Dictionary', 'derail', 'derail', 'derail', 'derail', 'derail', 'Legal', 'Legal', 'derail', 'derail']),
        })
        
        self._test_docs("msmarco-document-v2/anchor-text", count=4821244, items={
            0: MsMarcoV2AnchorTextDocument('msmarco_doc_53_1505820116', 'this simple tutorial', ['this simple tutorial']),
            4821243: MsMarcoV2AnchorTextDocument('msmarco_doc_43_1173903097', 'Emily Deschanel Biography for Emily Deschanel Emily Deschanel \u2013 Biography http://www.imdb.com/name/nm0221043/bio Biography for Emily Deschanel Emily Deschanel \u2013 Biography Emily Deschanel http://www.imdb.com/name/nm0221043/bio http://www.imdb.com/name/nm0221043/bio', ['Emily Deschanel', 'Biography for Emily Deschanel', 'Emily Deschanel \u2013 Biography', 'http://www.imdb.com/name/nm0221043/bio', 'Biography for Emily Deschanel', 'Emily Deschanel \u2013 Biography', 'Emily Deschanel', 'http://www.imdb.com/name/nm0221043/bio', 'http://www.imdb.com/name/nm0221043/bio']),
        })


if __name__ == "__main__":
    main()

