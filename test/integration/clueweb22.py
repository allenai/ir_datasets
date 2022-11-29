from datetime import datetime
from itertools import islice
from unittest import main
from re import compile

from tqdm.auto import tqdm

from ir_datasets import log, load, Dataset
from ir_datasets.formats import ClueWeb22LDoc, ClueWeb22ADoc
from ir_datasets.formats.clueweb22 import AnnotationType, Anchor, ClueWeb22BDoc
from .base import DatasetIntegrationTest

_logger = log.easy()


class TestClueWeb22(DatasetIntegrationTest):

    def _test_docs_slice(
            self,
            dataset: Dataset,
            indices: slice,
            num_expected: int,
            name: str,
            skip_islice: bool = False,
    ):
        with _logger.duration(f"{name} (slice)"):
            docs_slice = list(tqdm(dataset.docs_iter()[indices]))
        docs_slice = sorted(docs_slice)
        self.assertEqual(len(docs_slice), num_expected)
        if skip_islice:
            return

        with _logger.duration(f"{name} (islice)"):
            docs_islice = list(islice(
                tqdm(dataset.docs_iter()),
                indices.start, indices.stop, indices.step
            ))
        docs_islice = sorted(docs_islice)
        self.assertEqual(len(docs_islice), num_expected)

        self.assertListEqual(docs_slice, docs_islice)

    def test_clueweb22_meta(self):
        dataset = load("clueweb22")
        self.assertFalse(dataset.has_docs())
        self.assertFalse(dataset.has_queries())
        self.assertFalse(dataset.has_qrels())
        self.assertFalse(dataset.has_scoreddocs())
        self.assertFalse(dataset.has_docpairs())
        self.assertFalse(dataset.has_qlogs())

        subsets = ["b", "a", "l"]
        for subset in subsets:
            subset_dataset: Dataset = load(f"clueweb22/{subset}")
            self.assertTrue(subset_dataset.has_docs())
            self.assertFalse(subset_dataset.has_queries())
            self.assertFalse(subset_dataset.has_qrels())
            self.assertFalse(subset_dataset.has_scoreddocs())
            self.assertFalse(subset_dataset.has_docpairs())
            self.assertFalse(subset_dataset.has_qlogs())
            self.assertEqual(subset_dataset.docs_lang(), None)

            for lang in [
                "de", "en", "es", "fr", "it", "ja", "nl", "po", "pt", "zh",
                "other-languages",
            ]:
                lang_dataset: Dataset = load(f"clueweb22/{subset}/{lang}")
                self.assertTrue(lang_dataset.has_docs())
                self.assertFalse(lang_dataset.has_queries())
                self.assertFalse(lang_dataset.has_qrels())
                self.assertFalse(lang_dataset.has_scoreddocs())
                self.assertFalse(lang_dataset.has_docpairs())
                self.assertFalse(lang_dataset.has_qlogs())
                self.assertEqual(lang_dataset.docs_lang(), lang)

            subset_views = subsets[subsets.index(subset) + 1:]
            for subset_view in subset_views:
                subset_view_dataset: Dataset = load(
                    f"clueweb22/{subset}/as-{subset_view}"
                )
                self.assertTrue(subset_view_dataset.has_docs())
                self.assertFalse(subset_view_dataset.has_queries())
                self.assertFalse(subset_view_dataset.has_qrels())
                self.assertFalse(subset_view_dataset.has_scoreddocs())
                self.assertFalse(subset_view_dataset.has_docpairs())
                self.assertFalse(subset_view_dataset.has_qlogs())
                self.assertEqual(subset_view_dataset.docs_lang(), None)

                for lang in [
                    "de", "en", "es", "fr", "it", "ja", "nl", "po", "pt", "zh",
                    "other-languages",
                ]:
                    lang_dataset: Dataset = load(
                        f"clueweb22/{subset}/as-{subset_view}/{lang}"
                    )
                    self.assertTrue(lang_dataset.has_docs())
                    self.assertFalse(lang_dataset.has_queries())
                    self.assertFalse(lang_dataset.has_qrels())
                    self.assertFalse(lang_dataset.has_scoreddocs())
                    self.assertFalse(lang_dataset.has_docpairs())
                    self.assertFalse(lang_dataset.has_qlogs())
                    self.assertEqual(lang_dataset.docs_lang(), lang)

    def test_clueweb22_l_docs(self):
        # noinspection PyTypeChecker
        self._test_docs(
            "clueweb22/b/as-l",
            count=200_000_000,
            items={
                0: ClueWeb22LDoc(
                    doc_id="clueweb22-de0000-00-00000",
                    url="https://www.alba.info/karriere/",
                    url_hash="EF1C364E908E1460885D7DF3C91B6FE5",
                    language="de",
                    text=compile(
                        '.*Speichert den Zustimmungsstatus des Benutzers für Cookies auf der aktuellen Domäne.$'
                    ),
                ),
                1_000: ClueWeb22LDoc(
                    doc_id="clueweb22-de0000-00-01000",
                    url="https://totallygamergirl.com/2021/10/22/forza-horizon-4-festival-spielliste-kw-42-2021-aufgaben-belohnungen-und-voraussetzungen/",
                    url_hash="FCB5D1104F48D49F2F68DA4AB1D3E0A7",
                    language="de",
                    text=compile(
                        '.*Bildquelle: eigene Screenshots aus Forza Horizon 4$'
                    ),
                ),
                100_000_000: ClueWeb22LDoc(
                    doc_id='clueweb22-es0000-56-11365',
                    url='https://conjugador.reverso.net/conjugacion-espanol-verbo-parlar.html',
                    url_hash='8AA0B21CFF8A1F0B1F0B9EFFF30D5EEA',
                    language='es',
                    text=compile(
                        '.*supervisar, meter, convocar, estructurar, graznar, implementar, desarmar, elegir, objetar%'
                    )
                ),
            },
        )

    def test_clueweb22_a_docs(self):
        # noinspection PyTypeChecker
        self._test_docs(
            "clueweb22/b/as-a",
            count=200_000_000,
            items={
                0: ClueWeb22ADoc(
                    doc_id="clueweb22-de0000-00-00000",
                    url="https://www.alba.info/karriere/",
                    url_hash="EF1C364E908E1460885D7DF3C91B6FE5",
                    language="de",
                    text=compile(
                        '.*Speichert den Zustimmungsstatus des Benutzers für Cookies auf der aktuellen Domäne.$'
                    ),
                    date=datetime(2022, 8, 24, 1, 39, 58, 761009),
                    html=compile(b"^<html"),
                    vdom_nodes={
                        AnnotationType.LIST: [877],
                        AnnotationType.TABLE: [55, 269, 459],
                        AnnotationType.PARAGRAPH: [987, 990, 1008, 1169, 1466],
                        AnnotationType.TITLE: [],
                        AnnotationType.HEADING: [
                            940, 969, 1014, 1046, 1052, 1086, 1092, 1120, 1126,
                            1175, 1181, 1215, 1221, 1249, 1255, 1333, 1341,
                            1365, 1373, 1397, 1405, 1429, 1437, 1472, 1478
                        ],
                        AnnotationType.NONE: [],
                        AnnotationType.PRIMARY: [
                            68, 71, 78, 81, 87, 90, 96, 99, 105, 108, 120, 123,
                            130, 133, 139, 142, 148, 151, 163, 166, 173, 176,
                            182, 185, 191, 194, 205, 208, 214, 217, 223, 226,
                            237, 240, 246, 249, 255, 258, 282, 285, 288, 291,
                            294, 297, 300, 303, 306, 309, 316, 319, 322, 325,
                            328, 331, 334, 337, 340, 343, 349, 352, 358, 361,
                            367, 370, 382, 385, 393, 404, 407, 413, 416, 422,
                            425, 437, 440, 448, 472, 475, 478, 481, 484, 487,
                            490, 497, 500, 503, 506, 509, 512, 515, 518, 524,
                            527, 533, 536, 542, 545, 557, 560, 568, 882, 888,
                            894, 900, 906, 912, 941, 970, 988, 991, 1009, 1015,
                            1040, 1047, 1053, 1080, 1087, 1093, 1114, 1121,
                            1127, 1163, 1170, 1176, 1182, 1209, 1216, 1222,
                            1243, 1250, 1256, 1328, 1334, 1342, 1360, 1366,
                            1374, 1392, 1398, 1406, 1424, 1430, 1438, 1461,
                            1467, 1473, 1479, 1665
                        ],
                    },
                    vdom_data=compile(b".*clueweb22-de0000-00-00000$"),
                    inlink_anchors=[
                        Anchor(
                            url='https://www.alba.info/karriere/studierende-absolventen/werkstudenten/',
                            url_hash='F2C6239BCCFA656DD8E77E922D1C1C8A',
                            text='Karriere',
                            language='de',
                        ),
                    ],
                    outlink_anchors=[
                        Anchor(
                            url='https://www.alba.info/service/albasigner/login/',
                            url_hash='AFDD751E32ED78C559C2FF4C08543DD6',
                            text='\nLogin\n',
                            language='de',
                        ),
                    ],
                ),
                1_000: ClueWeb22ADoc(
                    doc_id="clueweb22-de0000-00-01000",
                    url="https://totallygamergirl.com/2021/10/22/forza-horizon-4-festival-spielliste-kw-42-2021-aufgaben-belohnungen-und-voraussetzungen/",
                    url_hash="FCB5D1104F48D49F2F68DA4AB1D3E0A7",
                    language="de",
                    text=compile(
                        '.*Bildquelle: eigene Screenshots aus Forza Horizon 4$'
                    ),
                    date=datetime(2022, 8, 24, 4, 1, 57, 988007),
                    html=compile(b"^<html"),
                    vdom_nodes={
                        AnnotationType.LIST: [
                            1311, 1332, 1353, 1379, 1410, 1436, 1467, 1498,
                            1529, 1563, 1584, 1622
                        ],
                        AnnotationType.TABLE: [],
                        AnnotationType.PARAGRAPH: [
                            1313, 1318, 1323, 1339, 1355, 1360, 1370, 1381,
                            1386, 1391, 1396, 1401, 1412, 1417, 1422, 1427,
                            1438, 1443, 1469, 1474, 1500, 1505, 1531, 1549,
                            1554, 1586, 1599, 1609, 1629, 1646, 1650, 1656
                        ],
                        AnnotationType.TITLE: [1686],
                        AnnotationType.HEADING: [
                            1329, 1350, 1376, 1407, 1433, 1464, 1495, 1526,
                            1560, 1581, 1615, 1640
                        ],
                        AnnotationType.NONE: [],
                        AnnotationType.PRIMARY: [
                            1282, 1285, 1314, 1316, 1319, 1321, 1324, 1326,
                            1330, 1335, 1337, 1340, 1342, 1345, 1347, 1351,
                            1356, 1358, 1361, 1363, 1366, 1368, 1371, 1373,
                            1377, 1382, 1384, 1387, 1389, 1392, 1394, 1397,
                            1399, 1402, 1404, 1408, 1413, 1415, 1418, 1420,
                            1423, 1425, 1428, 1430, 1434, 1439, 1441, 1444,
                            1446, 1449, 1451, 1454, 1456, 1459, 1461, 1465,
                            1470, 1472, 1475, 1477, 1480, 1482, 1485, 1487,
                            1490, 1492, 1496, 1501, 1503, 1506, 1508, 1511,
                            1513, 1516, 1518, 1521, 1523, 1527, 1532, 1534,
                            1535, 1537, 1540, 1542, 1545, 1547, 1550, 1552,
                            1555, 1557, 1561, 1566, 1568, 1571, 1573, 1576,
                            1578, 1582, 1587, 1589, 1590, 1592, 1595, 1597,
                            1600, 1602, 1605, 1607, 1610, 1612, 1616, 1625,
                            1627, 1630, 1632, 1635, 1637, 1641, 1647, 1651,
                            1657, 1666, 1671, 1675, 1680, 1686, 2167
                        ],
                    },
                    vdom_data=compile(b".*clueweb22-de0000-00-01000$"),
                    inlink_anchors=[
                        Anchor(
                            url='https://totallygamergirl.com/2021/10/22/forza-horizon-5-hopsital-records-und-radio-eternal-playlists/',
                            url_hash='2BC8A98A59245A44DEB462C2F766AB2D',
                            text='Zurück Forza Horizon 4 Festival Spielliste KW 42 2021 – Aufgaben, Belohnungen und Voraussetzungen',
                            language='de',
                        ),
                    ],
                    outlink_anchors=[
                        Anchor(
                            url='https://www.youtube.com/user/totallygamergirlcom',
                            url_hash='875F99F48588686B6B3B71F65FA626E1',
                            text='',
                            language='en',
                        ),
                    ],
                ),
                1_000_000_000: ClueWeb22ADoc(
                    doc_id='clueweb22-es0000-56-11365',
                    url='https://conjugador.reverso.net/conjugacion-espanol-verbo-parlar.html',
                    url_hash='8AA0B21CFF8A1F0B1F0B9EFFF30D5EEA',
                    language='es',
                    text=compile(
                        ".*Conjugue también: impregnar, supervisar, meter, convocar, estructurar, graznar, implementar, desarmar, elegir, objetar$"
                    ),
                    date=datetime(2022, 8, 25, 6, 58, 5, 282445),
                    html=compile(b"^<html"),
                    vdom_nodes={
                        AnnotationType.NONE: [],
                        AnnotationType.PRIMARY: [
                            469, 470, 472, 473, 475, 476, 478, 479, 481, 482,
                            484, 485, 487, 488, 490, 491, 493, 494, 496, 497,
                            505, 506, 508, 511, 518, 519, 521, 528, 529, 531,
                            538, 539, 541, 542, 544, 545, 547, 549, 550, 552,
                            554, 571, 584, 587, 593, 595, 598, 604, 607, 614,
                            616, 619, 625, 628, 634, 636, 638, 641, 643, 645,
                            648, 650, 652, 655, 657, 659, 662, 664, 666, 669,
                            671, 673, 675, 682, 684, 686, 689, 691, 693, 696,
                            698, 700, 703, 705, 707, 710, 712, 714, 717, 719,
                            721, 723, 729, 731, 734, 736, 739, 741, 744, 746,
                            749, 751, 754, 756, 758, 764, 766, 768, 771, 773,
                            775, 778, 780, 782, 785, 787, 789, 792, 794, 796,
                            799, 801, 803, 805, 812, 814, 816, 819, 821, 823,
                            826, 828, 830, 833, 835, 837, 840, 842, 844, 847,
                            849, 851, 853, 859, 861, 864, 866, 869, 871, 874,
                            876, 879, 881, 884, 886, 888, 894, 896, 899, 901,
                            904, 906, 909, 911, 914, 916, 919, 921, 923, 930,
                            932, 935, 937, 940, 942, 945, 947, 950, 952, 955,
                            957, 959, 962, 968, 970, 973, 975, 978, 980, 983,
                            985, 988, 990, 993, 999, 1001, 1004, 1006, 1009,
                            1011, 1014, 1016, 1019, 1021, 1024, 1026, 1028,
                            1035, 1037, 1040, 1042, 1045, 1047, 1050, 1052,
                            1055, 1057, 1060, 1062, 1064, 1070, 1072, 1074,
                            1077, 1079, 1081, 1084, 1086, 1088, 1091, 1093,
                            1095, 1098, 1100, 1102, 1105, 1107, 1109, 1111,
                            1117, 1119, 1121, 1124, 1126, 1128, 1131, 1133,
                            1135, 1138, 1140, 1142, 1145, 1147, 1149, 1152,
                            1154, 1156, 1158, 1165, 1167, 1169, 1172, 1174,
                            1176, 1179, 1181, 1183, 1186, 1188, 1190, 1193,
                            1195, 1197, 1200, 1202, 1204, 1206, 1212, 1214,
                            1216, 1219, 1221, 1223, 1226, 1228, 1230, 1233,
                            1235, 1237, 1240, 1242, 1244, 1247, 1249, 1251,
                            1253, 1259, 1261, 1263, 1266, 1268, 1270, 1273,
                            1275, 1277, 1280, 1282, 1284, 1287, 1289, 1291,
                            1294, 1296, 1298, 1300, 1307, 1309, 1312, 1314,
                            1317, 1319, 1322, 1324, 1327, 1329, 1332, 1334,
                            1336, 1342, 1344, 1347, 1349, 1352, 1354, 1357,
                            1359, 1362, 1364, 1367, 1369, 1371, 1377, 1379,
                            1382, 1384, 1387, 1389, 1392, 1394, 1397, 1399,
                            1402, 1404, 1406, 1409, 1433, 1462, 1463, 1465,
                            1468, 1478, 1479, 1481, 1491, 1492, 1494, 1503,
                            1504, 1508, 1509, 1513, 1514, 1572, 1589, 1595,
                            1597, 1605, 1646, 1653, 1671, 1672
                        ],
                        AnnotationType.HEADING: [
                            586, 597, 606, 618, 627, 961, 992, 1408, 1500,
                            1505, 1510, 1570, 1669
                        ],
                        AnnotationType.TITLE: [1671, 1672],
                        AnnotationType.PARAGRAPH: [],
                        AnnotationType.TABLE: [],
                        AnnotationType.LIST: [
                            726, 761, 809, 856, 891, 927, 965, 996, 1032, 1067,
                            1114, 1162, 1209, 1256, 1339
                        ]
                    },
                    vdom_data=compile(b".*clueweb22-es0000-56-11365"),
                    inlink_anchors=[],
                    outlink_anchors=[
                        Anchor(
                            url='https://diccionario.reverso.net/informatica-espanol-ingles/',
                            url_hash='069C6FCC43872EDE0F5665259C444259',
                            text='Diccionario informática Inglés Español',
                            language='es'
                        ),
                    ]
                ),
            },
        )

    def test_clueweb22_b_docs(self):
        # noinspection PyTypeChecker
        self._test_docs(
            "clueweb22/b",
            count=200_000_000,
            items={
                0: ClueWeb22BDoc(
                    doc_id="clueweb22-de0000-00-00000",
                    url="https://www.alba.info/karriere/",
                    url_hash="EF1C364E908E1460885D7DF3C91B6FE5",
                    language="de",
                    text=compile(
                        '.*Speichert den Zustimmungsstatus des Benutzers für Cookies auf der aktuellen Domäne.$'
                    ),
                    date=datetime(2022, 8, 24, 1, 39, 58, 761009),
                    html=compile(b"^<html"),
                    vdom_nodes={
                        AnnotationType.LIST: [877],
                        AnnotationType.TABLE: [55, 269, 459],
                        AnnotationType.PARAGRAPH: [987, 990, 1008, 1169, 1466],
                        AnnotationType.TITLE: [],
                        AnnotationType.HEADING: [
                            940, 969, 1014, 1046, 1052, 1086, 1092, 1120, 1126,
                            1175, 1181, 1215, 1221, 1249, 1255, 1333, 1341,
                            1365, 1373, 1397, 1405, 1429, 1437, 1472, 1478
                        ],
                        AnnotationType.NONE: [],
                        AnnotationType.PRIMARY: [
                            68, 71, 78, 81, 87, 90, 96, 99, 105, 108, 120, 123,
                            130, 133, 139, 142, 148, 151, 163, 166, 173, 176,
                            182, 185, 191, 194, 205, 208, 214, 217, 223, 226,
                            237, 240, 246, 249, 255, 258, 282, 285, 288, 291,
                            294, 297, 300, 303, 306, 309, 316, 319, 322, 325,
                            328, 331, 334, 337, 340, 343, 349, 352, 358, 361,
                            367, 370, 382, 385, 393, 404, 407, 413, 416, 422,
                            425, 437, 440, 448, 472, 475, 478, 481, 484, 487,
                            490, 497, 500, 503, 506, 509, 512, 515, 518, 524,
                            527, 533, 536, 542, 545, 557, 560, 568, 882, 888,
                            894, 900, 906, 912, 941, 970, 988, 991, 1009, 1015,
                            1040, 1047, 1053, 1080, 1087, 1093, 1114, 1121,
                            1127, 1163, 1170, 1176, 1182, 1209, 1216, 1222,
                            1243, 1250, 1256, 1328, 1334, 1342, 1360, 1366,
                            1374, 1392, 1398, 1406, 1424, 1430, 1438, 1461,
                            1467, 1473, 1479, 1665
                        ],
                    },
                    vdom_data=compile(b".*clueweb22-de0000-00-00000$"),
                    inlink_anchors=[
                        Anchor(
                            url='https://www.alba.info/karriere/studierende-absolventen/werkstudenten/',
                            url_hash='F2C6239BCCFA656DD8E77E922D1C1C8A',
                            text='Karriere',
                            language='de',
                        ),
                    ],
                    outlink_anchors=[
                        Anchor(
                            url='https://www.alba.info/service/albasigner/login/',
                            url_hash='AFDD751E32ED78C559C2FF4C08543DD6',
                            text='\nLogin\n',
                            language='de',
                        ),
                    ],
                ),
                1_000: ClueWeb22BDoc(
                    doc_id="clueweb22-de0000-00-01000",
                    url="https://totallygamergirl.com/2021/10/22/forza-horizon-4-festival-spielliste-kw-42-2021-aufgaben-belohnungen-und-voraussetzungen/",
                    url_hash="FCB5D1104F48D49F2F68DA4AB1D3E0A7",
                    language="de",
                    text=compile(
                        '.*Bildquelle: eigene Screenshots aus Forza Horizon 4$'
                    ),
                    date=datetime(2022, 8, 24, 4, 1, 57, 988007),
                    html=compile(b"^<html"),
                    vdom_nodes={
                        AnnotationType.LIST: [
                            1311, 1332, 1353, 1379, 1410, 1436, 1467, 1498,
                            1529, 1563, 1584, 1622
                        ],
                        AnnotationType.TABLE: [],
                        AnnotationType.PARAGRAPH: [
                            1313, 1318, 1323, 1339, 1355, 1360, 1370, 1381,
                            1386, 1391, 1396, 1401, 1412, 1417, 1422, 1427,
                            1438, 1443, 1469, 1474, 1500, 1505, 1531, 1549,
                            1554, 1586, 1599, 1609, 1629, 1646, 1650, 1656
                        ],
                        AnnotationType.TITLE: [1686],
                        AnnotationType.HEADING: [
                            1329, 1350, 1376, 1407, 1433, 1464, 1495, 1526,
                            1560, 1581, 1615, 1640
                        ],
                        AnnotationType.NONE: [],
                        AnnotationType.PRIMARY: [
                            1282, 1285, 1314, 1316, 1319, 1321, 1324, 1326,
                            1330, 1335, 1337, 1340, 1342, 1345, 1347, 1351,
                            1356, 1358, 1361, 1363, 1366, 1368, 1371, 1373,
                            1377, 1382, 1384, 1387, 1389, 1392, 1394, 1397,
                            1399, 1402, 1404, 1408, 1413, 1415, 1418, 1420,
                            1423, 1425, 1428, 1430, 1434, 1439, 1441, 1444,
                            1446, 1449, 1451, 1454, 1456, 1459, 1461, 1465,
                            1470, 1472, 1475, 1477, 1480, 1482, 1485, 1487,
                            1490, 1492, 1496, 1501, 1503, 1506, 1508, 1511,
                            1513, 1516, 1518, 1521, 1523, 1527, 1532, 1534,
                            1535, 1537, 1540, 1542, 1545, 1547, 1550, 1552,
                            1555, 1557, 1561, 1566, 1568, 1571, 1573, 1576,
                            1578, 1582, 1587, 1589, 1590, 1592, 1595, 1597,
                            1600, 1602, 1605, 1607, 1610, 1612, 1616, 1625,
                            1627, 1630, 1632, 1635, 1637, 1641, 1647, 1651,
                            1657, 1666, 1671, 1675, 1680, 1686, 2167
                        ],
                    },
                    vdom_data=compile(b".*clueweb22-de0000-00-01000$"),
                    inlink_anchors=[
                        Anchor(
                            url='https://totallygamergirl.com/2021/10/22/forza-horizon-5-hopsital-records-und-radio-eternal-playlists/',
                            url_hash='2BC8A98A59245A44DEB462C2F766AB2D',
                            text='Zurück Forza Horizon 4 Festival Spielliste KW 42 2021 – Aufgaben, Belohnungen und Voraussetzungen',
                            language='de',
                        ),
                    ],
                    outlink_anchors=[
                        Anchor(
                            url='https://www.youtube.com/user/totallygamergirlcom',
                            url_hash='875F99F48588686B6B3B71F65FA626E1',
                            text='',
                            language='en',
                        ),
                    ],
                ),
                1_000_000_000: ClueWeb22BDoc(
                    doc_id='clueweb22-es0000-56-11365',
                    url='https://conjugador.reverso.net/conjugacion-espanol-verbo-parlar.html',
                    url_hash='8AA0B21CFF8A1F0B1F0B9EFFF30D5EEA',
                    language='es',
                    text=compile(
                        ".*Conjugue también: impregnar, supervisar, meter, convocar, estructurar, graznar, implementar, desarmar, elegir, objetar$"
                    ),
                    date=datetime(2022, 8, 25, 6, 58, 5, 282445),
                    html=compile(b"^<html"),
                    vdom_nodes={
                        AnnotationType.NONE: [],
                        AnnotationType.PRIMARY: [
                            469, 470, 472, 473, 475, 476, 478, 479, 481, 482,
                            484, 485, 487, 488, 490, 491, 493, 494, 496, 497,
                            505, 506, 508, 511, 518, 519, 521, 528, 529, 531,
                            538, 539, 541, 542, 544, 545, 547, 549, 550, 552,
                            554, 571, 584, 587, 593, 595, 598, 604, 607, 614,
                            616, 619, 625, 628, 634, 636, 638, 641, 643, 645,
                            648, 650, 652, 655, 657, 659, 662, 664, 666, 669,
                            671, 673, 675, 682, 684, 686, 689, 691, 693, 696,
                            698, 700, 703, 705, 707, 710, 712, 714, 717, 719,
                            721, 723, 729, 731, 734, 736, 739, 741, 744, 746,
                            749, 751, 754, 756, 758, 764, 766, 768, 771, 773,
                            775, 778, 780, 782, 785, 787, 789, 792, 794, 796,
                            799, 801, 803, 805, 812, 814, 816, 819, 821, 823,
                            826, 828, 830, 833, 835, 837, 840, 842, 844, 847,
                            849, 851, 853, 859, 861, 864, 866, 869, 871, 874,
                            876, 879, 881, 884, 886, 888, 894, 896, 899, 901,
                            904, 906, 909, 911, 914, 916, 919, 921, 923, 930,
                            932, 935, 937, 940, 942, 945, 947, 950, 952, 955,
                            957, 959, 962, 968, 970, 973, 975, 978, 980, 983,
                            985, 988, 990, 993, 999, 1001, 1004, 1006, 1009,
                            1011, 1014, 1016, 1019, 1021, 1024, 1026, 1028,
                            1035, 1037, 1040, 1042, 1045, 1047, 1050, 1052,
                            1055, 1057, 1060, 1062, 1064, 1070, 1072, 1074,
                            1077, 1079, 1081, 1084, 1086, 1088, 1091, 1093,
                            1095, 1098, 1100, 1102, 1105, 1107, 1109, 1111,
                            1117, 1119, 1121, 1124, 1126, 1128, 1131, 1133,
                            1135, 1138, 1140, 1142, 1145, 1147, 1149, 1152,
                            1154, 1156, 1158, 1165, 1167, 1169, 1172, 1174,
                            1176, 1179, 1181, 1183, 1186, 1188, 1190, 1193,
                            1195, 1197, 1200, 1202, 1204, 1206, 1212, 1214,
                            1216, 1219, 1221, 1223, 1226, 1228, 1230, 1233,
                            1235, 1237, 1240, 1242, 1244, 1247, 1249, 1251,
                            1253, 1259, 1261, 1263, 1266, 1268, 1270, 1273,
                            1275, 1277, 1280, 1282, 1284, 1287, 1289, 1291,
                            1294, 1296, 1298, 1300, 1307, 1309, 1312, 1314,
                            1317, 1319, 1322, 1324, 1327, 1329, 1332, 1334,
                            1336, 1342, 1344, 1347, 1349, 1352, 1354, 1357,
                            1359, 1362, 1364, 1367, 1369, 1371, 1377, 1379,
                            1382, 1384, 1387, 1389, 1392, 1394, 1397, 1399,
                            1402, 1404, 1406, 1409, 1433, 1462, 1463, 1465,
                            1468, 1478, 1479, 1481, 1491, 1492, 1494, 1503,
                            1504, 1508, 1509, 1513, 1514, 1572, 1589, 1595,
                            1597, 1605, 1646, 1653, 1671, 1672
                        ],
                        AnnotationType.HEADING: [
                            586, 597, 606, 618, 627, 961, 992, 1408, 1500,
                            1505, 1510, 1570, 1669
                        ],
                        AnnotationType.TITLE: [1671, 1672],
                        AnnotationType.PARAGRAPH: [],
                        AnnotationType.TABLE: [],
                        AnnotationType.LIST: [
                            726, 761, 809, 856, 891, 927, 965, 996, 1032, 1067,
                            1114, 1162, 1209, 1256, 1339
                        ]
                    },
                    vdom_data=compile(b".*clueweb22-es0000-56-11365"),
                    inlink_anchors=[],
                    outlink_anchors=[
                        Anchor(
                            url='https://diccionario.reverso.net/informatica-espanol-ingles/',
                            url_hash='069C6FCC43872EDE0F5665259C444259',
                            text='Diccionario informática Inglés Español',
                            language='es'
                        ),
                    ]
                ),
            },
        )

    def test_clueweb22_slice(self):
        for subset_view in [None, "a", "l"]:
            dataset_id = "clueweb22/b"
            if subset_view:
                dataset_id += f"/as-{subset_view}"
            dataset = load(dataset_id)

            self._test_docs_slice(
                dataset, slice(None, 100), 100, "start of file"
            )
            self._test_docs_slice(
                dataset, slice(None, 100, 2), 50, "start of file with step"
            )
            self._test_docs_slice(
                dataset, slice(5000, 5100), 100, "middle of file",
            )
            self._test_docs_slice(
                dataset, slice(23200, 23241), 41, "end of file",
            )
            self._test_docs_slice(
                dataset, slice(23241, 23300), 59, "start of new file",
            )
            self._test_docs_slice(
                dataset, slice(23200, 23300), 100, "across file boundary",
            )
            self._test_docs_slice(
                dataset, slice(2278243, 2278343), 100, "later file",
                skip_islice=True,
            )
            self._test_docs_slice(
                dataset, slice(100_000_000, 100_000_100), 100, "middle of dataset",
                skip_islice=True,
            )

    def test_clueweb22_docstore(self):
        ids = [
            "clueweb22-de0000-01-00014",
            "clueweb22-de0000-01-12119",
            "clueweb22-de0000-05-19516",
        ]
        ids_nearby = [
            "clueweb22-de0000-01-00020",
            "clueweb22-de0000-01-12201",
            "clueweb22-de0000-05-19412",
        ]
        ids_earlier = [
            "clueweb22-de0000-01-00001",
            "clueweb22-de0000-05-08131",
        ]
        for subset_view in [None, "a", "l"]:
            dataset_id = "clueweb22/b"
            if subset_view:
                dataset_id += f"/as-{subset_view}"
            docstore = load(dataset_id).docs_store()
            docstore.clear_cache()
            with _logger.duration("cold fetch"):
                docstore.get_many(ids)
            docstore.clear_cache()
            with _logger.duration("cold fetch (cleared)"):
                docstore.get_many(ids)
            with _logger.duration("warm fetch"):
                docstore.get_many(ids)
            docstore = load(dataset_id).docs_store()
            with _logger.duration("warm fetch (new docstore)"):
                docstore.get_many(ids)
            with _logger.duration("cold fetch (nearby)"):
                docstore.get_many(ids_nearby)
            with _logger.duration("cold fetch (earlier)"):
                docstore.get_many(ids_earlier)
            docstore.clear_cache()
            with _logger.duration("cold fetch (earlier, cleared)"):
                docstore.get_many(ids_earlier)


if __name__ == "__main__":
    main()
