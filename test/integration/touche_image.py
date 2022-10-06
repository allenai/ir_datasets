from re import compile
from unittest import main

from ir_datasets.formats import ToucheImageDoc
from test.integration.base import DatasetIntegrationTest


class TestToucheImage(DatasetIntegrationTest):

    # noinspection PyTypeChecker
    def test_docs(self):
        self._test_docs("touche-image/2022-06-13", count=23841, items={
            0: ToucheImageDoc(
                doc_id="I000330ba4ea0ad13",
                png=compile(b"\x89PNG.*"),
                webp=compile(b"RIFF\xd0\xf3\x05\x00WEBPVP8.*"),
                url="https://www.e-dmj.org/upload//thumbnails/dmj-2020-0258f3.jpg",
                phash="1000000011001011011101010011101010010111011010101000011101101100",
                pages=[]
            ),
            23840: ToucheImageDoc(
                doc_id="Iffff8be6926a808e",
                png=compile(b"\x89PNG.*"),
                webp=compile(b"RIFF\x0e\\+\x00\x00WEBPVP8.*"),
                url="https://assets.pewresearch.org/wp-content/uploads/sites/11/2012/07/death-penalty-2011-1.png",
                phash="0001011011111110101001010100010110101011101000101111000001110000",
                pages=[]
            ),
        })


if __name__ == "__main__":
    main()
