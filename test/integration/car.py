import re
import unittest
from ir_datasets.datasets.car import CarQuery
from ir_datasets.formats import TrecQrel, GenericDoc
from .base import DatasetIntegrationTest


class TestCar(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('car/v1.5', count=29678367, items={
            0: GenericDoc('0000000e7e72cafb61a9f356b7dceb25c5e028db', re.compile("^Ukraine was one of the most dangerous places for journalists in the world during the euromaidan demo.{311}ened in Donetsk in April 2014\\. In July 2014 a firebomb was thrown at the TV channel ''112 Ukraine''\\.$", flags=48)),
            9: GenericDoc('000006d5c22f4efbb6b963ea819e976a4b28600b', re.compile('^To mark the 40th anniversary of "Bohemian Rhapsody", the song was released on a limited edition 12" .{174}on CD, DVD \\& Blu\\-ray\\. This includes the first ever live recorded performance of "Bohemian Rhapsody"\\.$', flags=48)),
            29678366: GenericDoc('ffffffb9eec6224bef5da06e829eef59a37748c6', re.compile('^Fisher recommended Louis as First Sea Lord: "He is the most capable administrator in the Admiralty\'s.{472}that would prepare the navy\'s plans in case of war\\. He was promoted to full admiral on 13 July 1912\\.$', flags=48)),
        })
        self._test_docs('car/v2.0', count=29794697, items={
            0: GenericDoc('00000047dc43083f49b68399c6deeed5c0e81c1f', re.compile('^On 28 October 1943, Fuller sailed from Efate, New Hebrides, for the initial landings on Bougainville.{456}damage, and twice more during the following month and a half carried reinforcements to Bougainville\\.$', flags=48)),
            9: GenericDoc('0000070402dbaf074bc1e3ba487036322ef8ce86', re.compile('^In 1662, the then Governor of Jamaica, Lord Windsor, received royal instructions to protect  the "Ca.{527} its landward side to five feet on its seaward side, with the walls being about five feet in height\\.$', flags=48)),
            29794696: GenericDoc('ffffffb9eec6224bef5da06e829eef59a37748c6', re.compile('^Fisher recommended Louis as First Sea Lord: "He is the most capable administrator in the Admiralty\'s.{472}that would prepare the navy\'s plans in case of war\\. He was promoted to full admiral on 13 July 1912\\.$', flags=48)),
        })

    def test_queries(self):
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
        self._test_queries('car/v1.5/train/fold1', count=466596, items={
            0: CarQuery('Roderick%20Spode/Overview', 'Roderick Spode Overview', 'Roderick Spode', ('Overview',)),
            9: CarQuery('Alan%20Hale%20Jr./Personal%20life', 'Alan Hale Jr. Personal life', 'Alan Hale Jr.', ('Personal life',)),
            466595: CarQuery('Brian%20Eno/Personal%20life%20and%20beliefs', 'Brian Eno Personal life and beliefs', 'Brian Eno', ('Personal life and beliefs',)),
        })
        self._test_queries('car/v1.5/train/fold2', count=469323, items={
            0: CarQuery('Lost%20in%20Space%20(film)/Plot', 'Lost in Space (film) Plot', 'Lost in Space (film)', ('Plot',)),
            9: CarQuery('Dick%20&%20Dom%20in%20da%20Bungalow/Bungalow%20Games/Forfeit%20Auction', 'Dick & Dom in da Bungalow Bungalow Games Forfeit Auction', 'Dick & Dom in da Bungalow', ('Bungalow Games', 'Forfeit Auction')),
            469322: CarQuery('Erick%20van%20Egeraat/Awards%20and%20recognition', 'Erick van Egeraat Awards and recognition', 'Erick van Egeraat', ('Awards and recognition',)),
        })
        self._test_queries('car/v1.5/train/fold3', count=463314, items={
            0: CarQuery('Bradford,%20Ontario/History', 'Bradford, Ontario History', 'Bradford, Ontario', ('History',)),
            9: CarQuery('CBBC/Scheduling', 'CBBC Scheduling', 'CBBC', ('Scheduling',)),
            463313: CarQuery('Br%C3%BCel%20&%20Kj%C3%A6r/Organisational%20developments', 'Brüel & Kjær Organisational developments', 'Brüel & Kjær', ('Organisational developments',)),
        })
        self._test_queries('car/v1.5/train/fold4', count=468789, items={
            0: CarQuery('Status%20symbol/By%20region%20and%20time', 'Status symbol By region and time', 'Status symbol', ('By region and time',)),
            9: CarQuery('History%20of%20Greece/Ancient%20Greece%20(1100%E2%80%93146%20BC)/Iron%20Age%20(1100%E2%80%93800%20BC)', 'History of Greece Ancient Greece (1100–146 BC) Iron Age (1100–800 BC)', 'History of Greece', ('Ancient Greece (1100–146 BC)', 'Iron Age (1100–800 BC)')),
            468788: CarQuery('Manchester%20International%20Organ%20Competition/1986%20-%20Fifth%20competition', 'Manchester International Organ Competition 1986 - Fifth competition', 'Manchester International Organ Competition', ('1986 - Fifth competition',)),
        })

    def test_qrels(self):
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
        self._test_qrels('car/v1.5/train/fold1', count=1052398, items={
            0: TrecQrel('$100,000%20infield/Eddie%20Collins', 'c7aa3c7821a112a149d85f650cbca4ec23c63617', 1, '0'),
            9: TrecQrel("%60Abdu'l-Bah%C3%A1/Acre/Marriage%20and%20family%20life", '4da4ea634ccae1173e553129b368e95962969ec8', 1, '0'),
            1052397: TrecQrel('Zygosity/Types/Nullizygous', '36186e2655db62fd9c31701302f86636b03d2511', 1, '0'),
        })
        self._test_qrels('car/v1.5/train/fold2', count=1061162, items={
            0: TrecQrel("$h*!%20My%20Dad%20Says/''Surviving%20Jack''", 'dc4866e5b230ffb48b6f808f41ccf8063fbdc9fa', 1, '0'),
            9: TrecQrel('%22Left-Wing%22%20Communism:%20An%20Infantile%20Disorder/%22Left-wing%22%20communism%20in%20Germany', '22ec581e3e1c5397e64bc6f0066dc8aea12fc71f', 1, '0'),
            1061161: TrecQrel('ZynAddSubFX/Windows%20version', 'b9d1be10b54e5efcbf3e6f1e5f2fbaf7c8af303c', 1, '0'),
        })
        self._test_qrels('car/v1.5/train/fold3', count=1046784, items={
            0: TrecQrel('$2%20billion%20arms%20deal/Confessional%20statements', '0e512b5962fa5ea838a578cbf414ae09b863a33f', 1, '0'),
            9: TrecQrel('$2%20billion%20arms%20deal/Investigative%20committee', '812cb64a35f482bd60f82c1d67204c73612cb6a7', 1, '0'),
            1046783: TrecQrel('Zyuden%20Sentai%20Kyoryuger/Video%20game', '844b90cf6f7c62e5bf51625a4d216baec2825bf9', 1, '0'),
        })
        self._test_qrels('car/v1.5/train/fold4', count=1061911, items={
            0: TrecQrel('$1,000%20genome/Additional%20Resources', '67ea5eae967657a8f0282066e3086573e41726d5', 1, '0'),
            9: TrecQrel('$1,000%20genome/Commercial%20efforts', 'a7ac9041cd833d6b09cc5270b495e9f94704027f', 1, '0'),
            1061910: TrecQrel('Zyron/Products', 'f355f98b4e3d5b08f60abe61022e9393202b9718', 1, '0'),
        })


if __name__ == '__main__':
    unittest.main()
