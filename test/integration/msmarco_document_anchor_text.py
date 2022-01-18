from unittest import main
from re import compile

from ir_datasets.formats import GenericDoc
from test.integration.base import DatasetIntegrationTest

class TestMsMarcoAnchorText(DatasetIntegrationTest):
    # noinspection PyTypeChecker
    def test_docs(self):
        self._test_docs("msmarco-document-anchor-text/v1", count=1703834, items={
            0: GenericDoc('D525932', 'external locus of control external locus of control psychcentral.com external locus of control external locus of control external locus of control external locus external locus of control External Locus of Control external locus of control external locus of control. psychcentral.com locus of control external locus of control external locus of control. External Locus of Control external locus ELOC external locus of control external locus of control. external locus of control. external locus'),
            1703833: GenericDoc('D3430230', '.RM .RM .rm .RM .RM rm .RM rm FileInfo .rm .RM .rm .rm .rm .rm .RM FileInfo.com: .RM File Format rm .rm .rm .RM .rm .rm rm http/fileinfo.com/extension/rm .rm .RM .RM format .rm FileInfo .rm .rm FileInfo.com: .RM File Format .RM FileInfo FileInfo.com: .RM File Format .RM'),
        })
        
        self._test_docs("msmarco-document-anchor-text/v2", count=4821244, items={
            0: GenericDoc('msmarco_doc_24_945818265', 'Mission Sub-1\'s the SUB-1 crossbow at Mission Archery Mission Crossbows SUB-1 Mission Sub-1\'s Mission SUB-1 Crossbow Mission crosbow website'),
            4821243: GenericDoc('msmarco_doc_29_971550653', 'classes class class classes classes class classes classes class class class class classes class class class class classes class classes class class'),
        })


if __name__ == "__main__":
    main()

