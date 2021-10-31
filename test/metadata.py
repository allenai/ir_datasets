import unittest
import ir_datasets


class TestMetadata(unittest.TestCase):
    def test_all_metadata_available(self):
        for dsid in ir_datasets.registry._registered:
            self._test_ds(dsid)

    # def test_clirmatrix_metadata_available(self):
    #     LANGS = ('af', 'als', 'am', 'an', 'ar', 'arz', 'ast', 'az', 'azb', 'ba', 'bar', 'be', 'bg', 'bn', 'bpy', 'br', 'bs', 'bug', 'ca', 'cdo', 'ce', 'ceb', 'ckb', 'cs', 'cv', 'cy', 'da', 'de', 'diq', 'el', 'eml', 'en', 'eo', 'es', 'et', 'eu', 'fa', 'fi', 'fo', 'fr', 'fy', 'ga', 'gd', 'gl', 'gu', 'he', 'hi', 'hr', 'hsb', 'ht', 'hu', 'hy', 'ia', 'id', 'ilo', 'io', 'is', 'it', 'ja', 'jv', 'ka', 'kk', 'kn', 'ko', 'ku', 'ky', 'la', 'lb', 'li', 'lmo', 'lt', 'lv', 'mai', 'mg', 'mhr', 'min', 'mk', 'ml', 'mn', 'mr', 'mrj', 'ms', 'my', 'mzn', 'nap', 'nds', 'ne', 'new', 'nl', 'nn', 'no', 'oc', 'or', 'os', 'pa', 'pl', 'pms', 'pnb', 'ps', 'pt', 'qu', 'ro', 'ru', 'sa', 'sah', 'scn', 'sco', 'sd', 'sh', 'si', 'simple', 'sk', 'sl', 'sq', 'sr', 'su', 'sv', 'sw', 'szl', 'ta', 'te', 'tg', 'th', 'tl', 'tr', 'tt', 'uk', 'ur', 'uz', 'vec', 'vi', 'vo', 'wa', 'war', 'wuu', 'xmf', 'yi', 'yo', 'zh')
    #     MULTI8_LANGS = {'ar', 'de', 'en', 'es', 'fr', 'ja', 'ru', 'zh'}
    #     for doc_lang in LANGS:
    #         self._test_ds(f'clirmatrix/{doc_lang}')
    #         for query_lang in LANGS:
    #             if query_lang == doc_lang:
    #                 continue
    #             for split in ['train', 'dev', 'test1', 'test2']:
    #                 self._test_ds(f'clirmatrix/{doc_lang}/bi139-base/{query_lang}/{split}')
    #                 self._test_ds(f'clirmatrix/{doc_lang}/bi139-full/{query_lang}/{split}')
    #                 if query_lang in MULTI8_LANGS and doc_lang in MULTI8_LANGS:
    #                     self._test_ds(f'clirmatrix/{doc_lang}/multi8/{query_lang}/{split}')

    def _test_ds(self, dsid):
        with self.subTest(dsid):
            dataset = ir_datasets.load(dsid)
            metadata = dataset.metadata()
            for etype in ir_datasets.EntityType:
                if dataset.has(etype):
                    self.assertTrue(etype.value in metadata, f"{dsid} missing {etype.value} metadata")
                    self.assertTrue('count' in metadata[etype.value], f"{dsid} missing {etype.value} metadata")


if __name__ == '__main__':
    unittest.main()
