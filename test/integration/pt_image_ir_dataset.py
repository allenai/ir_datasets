import unittest
import ir_datasets

from ir_datasets.formats import TrecQrel
from ir_datasets.formats import GenericQuery

from test.integration.base import DatasetIntegrationTest

from ir_datasets.datasets.pt_image_ir_dataset import PtImageIrImage


class TestPtImageIr(DatasetIntegrationTest):
    def test_articles(self):
        # Test that the dataset 'pt-image-ir-dataset' has 4743 articles (excluding header)
        # Just test the count and basic structure
        docs = list(ir_datasets.load("pt-image-ir-dataset").docs_iter())
        self.assertEqual(len(docs), 4743)

        # Test first document structure
        first_doc = docs[0]
        self.assertEqual(first_doc.doc_id, "art001")
        self.assertEqual(first_doc.date, "2023-12-01")
        self.assertTrue(first_doc.title.startswith("Comemorações"))
        self.assertTrue(first_doc.text.startswith("O Presidente"))
        self.assertTrue("img00001" in first_doc.images)

    def test_images(self):
        # Test that the dataset 'pt-image-ir-dataset/images' has 42920 images (excluding header)
        # Testing start (index 0), middle (index 21459), and end (index 42919) entries
        self._test_docs(
            "pt-image-ir-dataset/images",
            count=42920,
            items={
                0: PtImageIrImage(
                    doc_id="img00001",
                    text="https://www.presidencia.pt/media/bspfpsfp/231201-prmrs-mfl-0461-4542.jpg",
                ),
                21459: PtImageIrImage(
                    doc_id="img21460",
                    text="https://www.presidencia.pt/media/c5wbrtqc/191219-prmrs-ro-0017-8746.jpg",
                ),
                42919: PtImageIrImage(
                    doc_id="img42920",
                    text="https://www.presidencia.pt/media/dw1kvy3f/170602-prmrs-ro-0002-1624.jpg",
                ),
            },
        )

    def test_queries(self):
        # Test that the dataset 'pt-image-ir-dataset' has 80 queries (excluding header)
        # Testing start (index 0), middle (index 39), and end (index 79) entries
        self._test_queries(
            "pt-image-ir-dataset",
            count=80,
            items={
                0: GenericQuery("q01", "Emoções de tristeza em rostos"),
                39: GenericQuery("q40", "Brexit"),
                79: GenericQuery("q80", "Algarve"),
            },
        )

    def test_qrels(self):
        # Test that the dataset 'pt-image-ir-dataset' has 5201 qrels
        # Testing start (index 0), middle (index 2600), and end (index 5200) entries
        self._test_qrels(
            "pt-image-ir-dataset",
            count=5201,
            items={
                0: TrecQrel("q01", "img40494", 0, "0"),
                2600: TrecQrel("q40", "img22242", 0, "0"),
                5200: TrecQrel("q80", "img24820", 1, "0"),
            },
        )

    def test_images_qrels(self):
        # Test qrels for the images-only dataset variant
        # Should have the same qrels as the main dataset
        self._test_qrels(
            "pt-image-ir-dataset/images",
            count=5201,
            items={
                0: TrecQrel("q01", "img40494", 0, "0"),
                2600: TrecQrel("q40", "img22242", 0, "0"),
                5200: TrecQrel("q80", "img24820", 1, "0"),
            },
        )

    def test_images_queries(self):
        # Test queries for the images-only dataset variant
        # Should have the same queries as the main dataset
        self._test_queries(
            "pt-image-ir-dataset/images",
            count=80,
            items={
                0: GenericQuery("q01", "Emoções de tristeza em rostos"),
                39: GenericQuery("q40", "Brexit"),
                79: GenericQuery("q80", "Algarve"),
            },
        )


if __name__ == "__main__":
    unittest.main()
