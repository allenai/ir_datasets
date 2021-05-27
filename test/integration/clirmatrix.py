import re
import unittest
import ir_datasets
from ir_datasets.formats import GenericDoc, GenericQuery, TrecQrel
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()

# Note: there's > 100k combinations here, so we are only testing a few cases

class TestCLIRMatrix(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('clirmatrix/af', count=87705, items={
            0: GenericDoc('123393', 'Weeskindertjies (plant) weeskind'),
            9: GenericDoc('14515', re.compile('^Die Groot Beer \\(Latyn: Ursa Major\\) is ’n sterrebeeld wat heeljaar in die Noordelike Halfrond sigbaar.{873}8\xa0mag\\. 47\xa0Ursae Majoris het twee bevestigde planete, wat 2,54 en 0,76 keer die massa van Jupiter is\\.$', flags=48)),
            87704: GenericDoc('18801', re.compile('^Die Suid\\-Afrikaanse Leër is die landmagkomponent van die Suid\\-Afrikaanse Nasionale Weermag en van sy.{964}Amptelike webwerf Hierdie artikel is ’n saadjie\\. Voel vry om Wikipedia te help deur dit uit te brei\\.$', flags=48)),
        })
        self._test_docs('clirmatrix/en', count=5984197, items={
            0: GenericDoc('4274592', re.compile('^Transtar was the model name given to the line of trucks produced by the Studebaker Corporation of So.{910}asons, the Transtar name was dropped for the 1959 4E series Studebaker trucks and changed to Deluxe\\.$', flags=48)),
            9: GenericDoc('23065547', re.compile('^Standard sea\\-level conditions \\(SSL\\), also known as sea\\-level standard \\(SLS\\), defines a set of atmosp.{827}orda, Introduction to Aerospace Engineering with a Flight Test Perspective, John Wiley \\& Sons, 2017\\.$', flags=48)),
            5984196: GenericDoc('2160901', re.compile('^Resentment \\(also called ranklement or bitterness\\) is a complex, multilayered emotion that has been d.{1021}of by others; and having achievements go unrecognized, while others succeed without working as hard\\.$', flags=48)),
        })
        self._test_docs('clirmatrix/simple', count=153408, items={
            0: GenericDoc('12559', re.compile('^A superlative, in grammar, is an adjective describing a noun that is the best example of a given qua.{684}the adverb "most" before the adjective\\. For instance, you do not say "funnest," or "interestingest"\\.$', flags=48)),
            9: GenericDoc('120355', re.compile('^Occult refers to an area of knowledge or thought that is hidden\\. The word occult has many uses in th.{1069}pretation of Hinduism within Theosophy or the various occult interpretations of the Jewish Kabbalah\\.$', flags=48)),
            153407: GenericDoc('54463', re.compile('^The history of the Christian religion and the Christian church began with Jesus and his apostles\\. Ch.{934}t\\. Peter, was that they did not, and the matter was further addressed with the Council of Jerusalem\\.$', flags=48)),
        })
        self._test_docs('clirmatrix/zh', count=1089043, items={
            0: GenericDoc('449241', '虿盆，商朝时酷刑之一。将作弊官人跣剥干净，送下坑中，餵毒蛇、毒蝎等物。相传商朝最后一任君主纣王曾在大将黄飞虎之妻与纣王之妃子苏妲己发生口角之后将其推下虿盆，令其惨死。此刑罚在历史上使用较少。'),
            9: GenericDoc('664068', re.compile('^篡位是一個貶义詞，即不合法或有爭議地取得王位\\(皇位\\)。包括殺上任皇帝/太子/廢立/逼迫上現任皇帝或君主交出皇位 在非君主制语境下，亦可泛指非法谋夺更高权力的行为（例如違反憲法而推行独裁，或在權限以外越.{29}为在元武宗\\(1307年\\)至元寧宗\\(1332年\\)的25年間，竟然換了八個皇帝，当中有三位皇帝\\(元天順帝、元明宗、元寧宗\\)在位時間甚至不足一年。 在同一王朝中通过杀害或逼退合法继承人或在位者的篡位者 政变$', flags=48)),
            1089042: GenericDoc('6844113', re.compile('^谷風隧道為台灣的一條公路隧道，屬「台9線蘇花公路山區路段改善計劃」\\(蘇花改\\)南澳\\~和平段的其中一座隧道，北起鼓音橋，南接漢本高架橋，它穿越中央山脈鼓音溪至花蓮縣漢本的山區。谷風隧道南下及北上線均為45.{425}作、避難聯絡通道襯砌、通風隔板施作、新建通風機房，此外還須在避難聯絡通道內安裝照明系統及通訊設備，主隧道亦須安裝隧道照明燈具結線，安裝水霧支管，安裝噴流風機，此外隧道的所有土建工程及機電工程同步施工。$', flags=48)),
        })

    def test_queries(self):
        self._test_queries('clirmatrix/af/bi139-base/en/train', count=9999, items={
            0: GenericQuery('690', 'Aruba'),
            9: GenericQuery('5615', 'Cretaceous'),
            9998: GenericQuery('62732112', 'Efrain Gusquiza'),
        })
        self._test_queries('clirmatrix/af/bi139-base/en/dev', count=1000, items={
            0: GenericQuery('2038', 'August Horch'),
            9: GenericQuery('77606', 'Charles VIII of France'),
            999: GenericQuery('62708410', '2020 in Morocco'),
        })
        self._test_queries('clirmatrix/af/bi139-base/en/test1', count=1000, items={
            0: GenericQuery('3649', 'Geography of the British Virgin Islands'),
            9: GenericQuery('107443', 'Coalinga, California'),
            999: GenericQuery('62716625', 'Kevin Hall (disambiguation)'),
        })
        self._test_queries('clirmatrix/af/bi139-base/en/test2', count=1000, items={
            0: GenericQuery('6011', 'Chomsky hierarchy'),
            9: GenericQuery('97597', 'Flag of San Marino'),
            999: GenericQuery('62707449', 'Machiel Kiel'),
        })
        self._test_queries('clirmatrix/en/bi139-base/af/train', count=10000, items={
            0: GenericQuery('3', 'Lys van Afrikaanse skrywers'),
            9: GenericQuery('95', 'Geskiedenis'),
            9999: GenericQuery('285953', 'Jean-Claude Casadesus'),
        })
        self._test_queries('clirmatrix/en/bi139-full/af/train', count=58745, items={
            0: GenericQuery('3', 'Lys van Afrikaanse skrywers'),
            9: GenericQuery('26', 'Benue-Kongo-tale'),
            58744: GenericQuery('286010', 'Lugmag van die Volksbevrydingsleër'),
        })
        self._test_queries('clirmatrix/en/multi8/fr/train', count=10000, items={
            0: GenericQuery('45187', 'Mort'),
            9: GenericQuery('7740', 'Lituanie'),
            9999: GenericQuery('28573', 'Chiffres arabes'),
        })
        self._test_queries('clirmatrix/fr/multi8/en/train', count=10000, items={
            0: GenericQuery('8221', 'Death'),
            9: GenericQuery('17675', 'Lithuania'),
            9999: GenericQuery('1786', 'Arabic numerals'),
        })
        self._test_queries('clirmatrix/de/multi8/en/train', count=10000, items={
            0: GenericQuery('8221', 'Death'),
            9: GenericQuery('17675', 'Lithuania'),
            9999: GenericQuery('1786', 'Arabic numerals'),
        })

    def test_qrels(self):
        self._test_qrels('clirmatrix/af/bi139-base/en/train', count=999900, items={
            0: TrecQrel('690', '14013', 6, '0'),
            9: TrecQrel('690', '15050', 0, '0'),
            999899: TrecQrel('62732112', '259879', 0, '0'),
        })
        self._test_qrels('clirmatrix/af/bi139-base/en/dev', count=100000, items={
            0: TrecQrel('2038', '13762', 3, '0'),
            9: TrecQrel('2038', '272786', 0, '0'),
            99999: TrecQrel('62708410', '258719', 0, '0'),
        })
        self._test_qrels('clirmatrix/af/bi139-base/en/test1', count=100000, items={
            0: TrecQrel('3649', '50129', 5, '0'),
            9: TrecQrel('3649', '93300', 0, '0'),
            99999: TrecQrel('62716625', '140128', 0, '0'),
        })
        self._test_qrels('clirmatrix/af/bi139-base/en/test2', count=100000, items={
            0: TrecQrel('6011', '11475', 6, '0'),
            9: TrecQrel('6011', '69338', 0, '0'),
            99999: TrecQrel('62707449', '112726', 0, '0'),
        })
        self._test_qrels('clirmatrix/en/bi139-base/af/train', count=1000000, items={
            0: TrecQrel('3', '1617690', 5, '0'),
            9: TrecQrel('3', '3943287', 3, '0'),
            999999: TrecQrel('285953', '43443609', 0, '0'),
        })
        self._test_qrels('clirmatrix/en/bi139-full/af/train', count=3011938, items={
            0: TrecQrel('3', '1617690', 5, '0'),
            9: TrecQrel('3', '3943287', 3, '0'),
            3011937: TrecQrel('286010', '400853', 1, '0'),
        })
        self._test_qrels('clirmatrix/en/multi8/fr/train', count=1000000, items={
            0: TrecQrel('45187', '49703357', 5, '0'),
            9: TrecQrel('45187', '12161221', 3, '0'),
            999999: TrecQrel('28573', '40255894', 0, '0'),
        })
        self._test_qrels('clirmatrix/fr/multi8/en/train', count=1000000, items={
            0: TrecQrel('8221', '45187', 6, '0'),
            9: TrecQrel('8221', '1331378', 4, '0'),
            999999: TrecQrel('1786', '9567503', 0, '0'),
        })
        self._test_qrels('clirmatrix/de/multi8/en/train', count=1000000, items={
            0: TrecQrel('8221', '5204', 6, '0'),
            9: TrecQrel('8221', '1092811', 4, '0'),
            999999: TrecQrel('1786', '10264293', 0, '0'),
        })

if __name__ == '__main__':
    unittest.main()
