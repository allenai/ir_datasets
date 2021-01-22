import re
import unittest
from ir_datasets.datasets.car import CarQuery
from ir_datasets.formats import TrecQrel, GenericDoc
from .base import DatasetIntegrationTest


class TestCar(DatasetIntegrationTest):
    def test_car_docs(self):
        self._test_docs('car/v1.5', count=29678367, items={
            0: GenericDoc('0000000e7e72cafb61a9f356b7dceb25c5e028db', re.compile("^Ukraine was one of the most dangerous places for journalists in the world during the euromaidan demo.{311}ened in Donetsk in April 2014\\. In July 2014 a firebomb was thrown at the TV channel ''112 Ukraine''\\.$", flags=48)),
            9: GenericDoc('000006d5c22f4efbb6b963ea819e976a4b28600b', re.compile('^To mark the 40th anniversary of "Bohemian Rhapsody", the song was released on a limited edition 12" .{174}on CD, DVD \\& Blu\\-ray\\. This includes the first ever live recorded performance of "Bohemian Rhapsody"\\.$', flags=48)),
            29678366: GenericDoc('ffffffb9eec6224bef5da06e829eef59a37748c6', re.compile('^Fisher recommended Louis as First Sea Lord: "He is the most capable administrator in the Admiralty\'s.{472}that would prepare the navy\'s plans in case of war\\. He was promoted to full admiral on 13 July 1912\\.$', flags=48)),
        })

    def test_car_queries(self):
        self._test_queries('car/v1.5/trec-y1', count=2287, items={
            0: CarQuery('Fudge/History', 'Fudge History', 'Fudge', ('History',)),
            9: CarQuery('Glass%20ceiling/Glass%20Ceiling%20Index', 'Glass ceiling Glass Ceiling Index', 'Glass ceiling', ('Glass Ceiling Index',)),
            2286: CarQuery('Global%20catastrophic%20risk/Organizations', 'Global catastrophic risk Organizations', 'Global catastrophic risk', ('Organizations',)),
        })
        self._test_queries('car/v1.5/test200', count=1987, items={
            0: CarQuery('Hog-dog%20rodeo/Typical%20match', 'Hog-dog rodeo Typical match', 'Hog-dog rodeo', ('Typical match',)),
            9: CarQuery('Infield%20fly%20rule/The%20rule/Foul%20balls', 'Infield fly rule The rule Foul balls', 'Infield fly rule', ('The rule', 'Foul balls')),
            1986: CarQuery('Structural%20information%20theory/Visual%20regularity', 'Structural information theory Visual regularity', 'Structural information theory', ('Visual regularity',)),
        })
        self._test_queries('car/v1.5/train/fold0', count=467946, items={
            0: CarQuery('Kindertotenlieder/Text%20and%20music', 'Kindertotenlieder Text and music', 'Kindertotenlieder', ('Text and music',)),
            9: CarQuery('Northrop%20YB-35/Variants', 'Northrop YB-35 Variants', 'Northrop YB-35', ('Variants',)),
            467945: CarQuery('1987%E2%80%9388%20Greek%20Cup/Final', '1987–88 Greek Cup Final', '1987–88 Greek Cup', ('Final',)),
        })

    def test_car_qrels(self):
        self._test_qrels('car/v1.5/trec-y1/auto', count=5820, items={
            0: TrecQrel('Aftertaste/Aftertaste%20processing%20in%20the%20cerebral%20cortex', '38c1bd25ddca2705164677a3f598c46df85afba7', 1, '0'),
            9: TrecQrel('Aftertaste/Temporal%20taste%20perception', '8a41a87100d139bb9c108c8cab2ac3baaabea3ce', 1, '0'),
            5819: TrecQrel('Yellowstone%20National%20Park/Recreation', 'e80b5185da1493edde41bea19a389a3f62167369', 1, '0'),
        })
        self._test_qrels('car/v1.5/trec-y1/manual', count=29571, items={
            0: TrecQrel('Hadley%20cell/Hadley%20cell%20expansion', '389c8a699f4db2f0278700d1c32e63ac369906cd', -1, '0'),
            9: TrecQrel('Water%20cycle/Effects%20on%20biogeochemical%20cycling', '844a0a0d5860ff1da8a9fcfb16cc4ce04ffb963f', 1, '0'),
            29570: TrecQrel('Rancidification/Reducing%20rancidification', '20a4e9af2853803a08854a1cc8973534e2235658', -1, '0'),
        })
        self._test_qrels('car/v1.5/test200', count=4706, items={
            0: TrecQrel('ASME/ASME%20codes%20and%20standards', '16d8f62407d2cdd283a71735e5c83f7d7947b93a', 1, '0'),
            9: TrecQrel('Activity%20theory/An%20explanation', 'c0ee784b8f0eb3b80aaf85f42d5148655192cc1d', 1, '0'),
            4705: TrecQrel('Zang-fu/Yin/yang%20and%20the%20Five%20Elements', 'fe6f4dd186037e09bf00f0f08bf172babac7930b', 1, '0'),
        })
        self._test_qrels('car/v1.5/train/fold0', count=1054369, items={
            0: TrecQrel("$pread/''$pread''%20Book", '2f545ffad1581dea4a2e4720aa9feb7389e1956a', 1, '0'),
            9: TrecQrel('%22Wild%20Bill%22%20Hickok/Death/Burial', '528b68a3355672c9b8bd5003428b72f54074b3fb', 1, '0'),
            1054368: TrecQrel('Zygmunt%20Szcz%C4%99sny%20Feli%C5%84ski/Views%20on%20Poland', 'fd77154f625ca721e554cbd0e4f33b51d4d92af6', 1, '0'),
        })


if __name__ == '__main__':
    unittest.main()
