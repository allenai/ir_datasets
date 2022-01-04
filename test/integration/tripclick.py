import re
import datetime
import unittest
from ir_datasets.formats import TrecQrel, TitleUrlTextDoc, GenericQuery, GenericScoredDoc, GenericDocPair
from ir_datasets.datasets.tripclick import TripClickQlog, TripClickPartialDoc, LogItem
from .base import DatasetIntegrationTest


class TestTripclick(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('tripclick', count=1523878, items={
            0: TitleUrlTextDoc('283040', re.compile(r'^V.*\.$', flags=48), re.compile(r'^http://www.ncb.*85551$', flags=48), re.compile('^BACKGROUND : .* Medical Society\\.\n$', flags=48)),
            9: TitleUrlTextDoc('283070', re.compile(r'^N.*\.$', flags=48), re.compile(r'^http://www.ncb.*t_uids=16775235$', flags=48), re.compile('^BACKGROUND : Res.* Medical Society\\.\n$', flags=48)),
            1523877: TitleUrlTextDoc('11701272', re.compile(r'^M.*m$', flags=48), re.compile(r'^https://www.ncbi.nlm.*58120938564.pdf$', flags=48), re.compile('^OBJECTIVE : To.* of Neurology\\.\n$', flags=48)),
        })
        self._test_docs('tripclick/logs', count=5196956, items={
            0: TripClickPartialDoc('96657', re.compile(r'^Syst.*$', flags=48), re.compile(r'^https://www.journalslibrary.*hta1060/$', flags=48)),
            9: TripClickPartialDoc('96666', re.compile(r'^Econom.*$', flags=48), re.compile(r'^https://www.journalslibrary..*hta3230/$', flags=48)),
            5196955: TripClickPartialDoc('11707140', re.compile(r'^Basic Life.*$', flags=48), re.compile(r'^https://covid19evidence.*201021-151716$', flags=48)),
        })

    def test_queries(self):
        self._test_queries('tripclick/train', count=685649, items={
            0: GenericQuery('8', re.compile(r'^a.*s$', flags=48)),
            9: GenericQuery('136', re.compile(r'^c.*g$', flags=48)),
            685648: GenericQuery('1647720', re.compile(r'^c.*e$', flags=48)),
        })
        self._test_queries('tripclick/train/head', count=3529, items={
            0: GenericQuery('8', re.compile(r'^a.*s$', flags=48)),
            9: GenericQuery('136', re.compile(r'^c.*g$', flags=48)),
            3528: GenericQuery('1630245', re.compile(r'^I.*d$', flags=48)),
        })
        self._test_queries('tripclick/train/head/dctr', count=3529, items={
            0: GenericQuery('8', re.compile(r'^a.*s$', flags=48)),
            9: GenericQuery('136', re.compile(r'^c.*g$', flags=48)),
            3528: GenericQuery('1630245', re.compile(r'^I.*d$', flags=48)),
        })
        self._test_queries('tripclick/train/torso', count=105964, items={
            0: GenericQuery('5', re.compile(r'^p.*e$', flags=48)),
            9: GenericQuery('43', re.compile(r'^p.*e$', flags=48)),
            105963: GenericQuery('1647511', re.compile(r'^h.*s$', flags=48)),
        })
        self._test_queries('tripclick/train/tail', count=576156, items={
            0: GenericQuery('1', re.compile(r'^c.*d$', flags=48)),
            9: GenericQuery('65', re.compile(r'^s.*t$', flags=48)),
            576155: GenericQuery('1647720', re.compile(r'^c.*e$', flags=48)),
        })
        self._test_queries('tripclick/val', count=3525, items={
            0: GenericQuery('38', re.compile(r'^a.*e$', flags=48)),
            9: GenericQuery('226', re.compile(r'^h.*t$', flags=48)),
            3524: GenericQuery('1645595', re.compile(r'^h.*n$', flags=48)),
        })
        self._test_queries('tripclick/val/head', count=1175, items={
            0: GenericQuery('38', re.compile(r'^a.*e$', flags=48)),
            9: GenericQuery('226', re.compile(r'^h.*t$', flags=48)),
            1174: GenericQuery('1630209', re.compile(r'^A.*e$', flags=48)),
        })
        self._test_queries('tripclick/val/head/dctr', count=1175, items={
            0: GenericQuery('38', re.compile(r'^a.*e$', flags=48)),
            9: GenericQuery('226', re.compile(r'^h.*t$', flags=48)),
            1174: GenericQuery('1630209', re.compile(r'^A.*e$', flags=48)),
        })
        self._test_queries('tripclick/val/torso', count=1175, items={
            0: GenericQuery('534', re.compile(r'^l.*n$', flags=48)),
            9: GenericQuery('4773', re.compile(r'^h.*l$', flags=48)),
            1174: GenericQuery('1626635', re.compile(r'^p.*f$', flags=48)),
        })
        self._test_queries('tripclick/val/tail', count=1175, items={
            0: GenericQuery('1052', re.compile(r'^r.*y$', flags=48)),
            9: GenericQuery('22440', re.compile(r'^g.*w$', flags=48)),
            1174: GenericQuery('1645595', re.compile(r'^h.*n$', flags=48)),
        })
        self._test_queries('tripclick/test', count=3525, items={
            0: GenericQuery('24', re.compile(r'^p.*g$', flags=48)),
            9: GenericQuery('354', re.compile(r'^a.*e$', flags=48)),
            3524: GenericQuery('1646719', re.compile(r'^p.*e$', flags=48)),
        })
        self._test_queries('tripclick/test/head', count=1175, items={
            0: GenericQuery('24', re.compile(r'^p.*g$', flags=48)),
            9: GenericQuery('354', re.compile(r'^a.*e$', flags=48)),
            1174: GenericQuery('1610957', re.compile(r'^S.*l$', flags=48)),
        })
        self._test_queries('tripclick/test/torso', count=1175, items={
            0: GenericQuery('152', re.compile(r'^v.*s$', flags=48)),
            9: GenericQuery('2700', re.compile(r'^p.*g$', flags=48)),
            1174: GenericQuery('1641005', re.compile(r'^h.*s$', flags=48)),
        })
        self._test_queries('tripclick/test/tail', count=1175, items={
            0: GenericQuery('4752', re.compile(r'^h.*e$', flags=48)),
            9: GenericQuery('15118', re.compile(r'^i.*n$', flags=48)),
            1174: GenericQuery('1646719', re.compile(r'^p.*e$', flags=48)),
        })

    def test_qlogs(self):
        self._test_qlogs('tripclick/logs', count=5317350, items={
            0: TripClickQlog(re.compile(r'3fjdej.{10}2yccq255', flags=48), re.compile(r'48a.{4}86f2', flags=48), 'community aed', re.compile(r'comm.*d', flags=48), datetime.datetime(2013, 1, 1, 1, 17, 26, 57000), (LogItem('981744', True),)),
            9: TripClickQlog(re.compile(r'htgqag5.{10}qeult45', flags=48), re.compile(r'd23b.{4}539', flags=48), 'primary wound closure', re.compile(r'prim.*ure', flags=48), datetime.datetime(2013, 1, 1, 2, 15, 33, 930000), (LogItem('1185098', True),)),
            19: TripClickQlog(re.compile(r'2qmdal45.{10}mbgrro', flags=48), re.compile(r'342.{4}6d4f', flags=48), 'status epilepticus treatment', re.compile(r'\(tit.*atment\)', flags=48), datetime.datetime(2013, 1, 1, 4, 50, 20, 603000), (LogItem('834890', True),)),
            63: TripClickQlog(re.compile(r'c352x5mog.{10}bup45', flags=48), re.compile(r'e511.{4}6e9', flags=48), 'conjunctivitis medication', re.compile(r'conju.*ication', flags=48), datetime.datetime(2013, 1, 1, 7, 54, 5, 737000), (LogItem('929760', True),)),
            5317349: TripClickQlog(re.compile(r'vcxwj5.{10}llhur3g0', flags=48), re.compile(r'921.{4}0c8d', flags=48), 'GORD in children', re.compile(r'\(GOR.*dren\)', flags=48), datetime.datetime(2020, 10, 28, 4, 56, 17, 943000), (LogItem('11203043', False), LogItem('11203042', True), LogItem('11203038', False), LogItem('11203041', False), LogItem('9459386', False), LogItem('9007494', False), LogItem('11172888', False), LogItem('9514712', False), LogItem('9159084', False), LogItem('11054069', False), LogItem('9338829', False), LogItem('9338609', False), LogItem('9007349', False), LogItem('9614037', False), LogItem('9184023', False), LogItem('10010303', False), LogItem('11054239', False), LogItem('9616794', False), LogItem('11185267', False), LogItem('11185318', False))),
        })

    def test_qrels(self):
        self._test_qrels('tripclick/train', count=2705212, items={
            0: TrecQrel('8', '1398048', 1, '0'),
            9: TrecQrel('8', '1431742', 1, '0'),
            2705211: TrecQrel('1647720', '11698361', 1, '0'),
        })
        self._test_qrels('tripclick/train/head', count=116821, items={
            0: TrecQrel('8', '1398048', 1, '0'),
            9: TrecQrel('8', '1431742', 1, '0'),
            116820: TrecQrel('1630245', '10818871', 1, '0'),
        })
        self._test_qrels('tripclick/train/head/dctr', count=128420, items={
            0: TrecQrel('8', '1398048', 3, '0'),
            9: TrecQrel('8', '5651514', 1, '0'),
            128419: TrecQrel('1630245', '9448244', 0, '0'),
        })
        self._test_qrels('tripclick/train/torso', count=966898, items={
            0: TrecQrel('5', '1099235', 1, '0'),
            9: TrecQrel('15', '9028026', 0, '0'),
            966897: TrecQrel('1647511', '11429892', 1, '0'),
        })
        self._test_qrels('tripclick/train/tail', count=1621493, items={
            0: TrecQrel('1', '981744', 1, '0'),
            9: TrecQrel('27', '1194092', 1, '0'),
            1621492: TrecQrel('1647720', '11698361', 1, '0'),
        })
        self._test_qrels('tripclick/val', count=82409, items={
            0: TrecQrel('38', '1390633', 1, '0'),
            9: TrecQrel('38', '9137657', 0, '0'),
            82408: TrecQrel('1645595', '9982749', 1, '0'),
        })
        self._test_qrels('tripclick/val/head', count=64364, items={
            0: TrecQrel('38', '1390633', 1, '0'),
            9: TrecQrel('38', '9137657', 0, '0'),
            64363: TrecQrel('1630209', '11086242', 1, '0'),
        })
        self._test_qrels('tripclick/val/head/dctr', count=66812, items={
            0: TrecQrel('38', '1390633', 2, '0'),
            9: TrecQrel('38', '7858667', 0, '0'),
            66811: TrecQrel('1630209', '9358372', 0, '0'),
        })
        self._test_qrels('tripclick/val/torso', count=14133, items={
            0: TrecQrel('534', '1397165', 1, '0'),
            9: TrecQrel('534', '5671894', 1, '0'),
            14132: TrecQrel('1626635', '10258672', 1, '0'),
        })
        self._test_qrels('tripclick/val/tail', count=3912, items={
            0: TrecQrel('1052', '951102', 1, '0'),
            9: TrecQrel('9347', '296234', 1, '0'),
            3911: TrecQrel('1645595', '9982749', 1, '0'),
        })

    def test_scoreddocs(self):
        self._test_scoreddocs('tripclick/val', count=3503310, items={
            0: GenericScoredDoc('38', '869893', 10.582),
            9: GenericScoredDoc('38', '484662', 9.7492),
            3503309: GenericScoredDoc('1645595', '2058354', 6.3192),
        })
        self._test_scoreddocs('tripclick/val/head', count=1166804, items={
            0: GenericScoredDoc('38', '869893', 10.582),
            9: GenericScoredDoc('38', '484662', 9.7492),
            1166803: GenericScoredDoc('1630209', '1336783', 10.0589),
        })
        self._test_scoreddocs('tripclick/val/head/dctr', count=1166804, items={
            0: GenericScoredDoc('38', '869893', 10.582),
            9: GenericScoredDoc('38', '484662', 9.7492),
            1166803: GenericScoredDoc('1630209', '1336783', 10.0589),
        })
        self._test_scoreddocs('tripclick/val/torso', count=1170314, items={
            0: GenericScoredDoc('534', '5671894', 10.4897),
            9: GenericScoredDoc('534', '11100678', 9.9284),
            1170313: GenericScoredDoc('1626635', '11074840', 4.848999),
        })
        self._test_scoreddocs('tripclick/val/tail', count=1166192, items={
            0: GenericScoredDoc('1052', '9086112', 11.7205),
            9: GenericScoredDoc('1052', '6298266', 10.1826),
            1166191: GenericScoredDoc('1645595', '2058354', 6.3192),
        })
        self._test_scoreddocs('tripclick/test', count=3486402, items={
            0: GenericScoredDoc('24', '11072695', 7.9575),
            9: GenericScoredDoc('24', '9124053', 7.802),
            3486401: GenericScoredDoc('1646719', '1047809', 4.342899),
        })
        self._test_scoreddocs('tripclick/test/head', count=1159303, items={
            0: GenericScoredDoc('24', '11072695', 7.9575),
            9: GenericScoredDoc('24', '9124053', 7.802),
            1159302: GenericScoredDoc('1610957', '10780205', 9.3906),
        })
        self._test_scoreddocs('tripclick/test/torso', count=1161972, items={
            0: GenericScoredDoc('152', '9083897', 21.2913),
            9: GenericScoredDoc('152', '9379499', 20.041401),
            1161971: GenericScoredDoc('1641005', '9003321', 9.9069),
        })
        self._test_scoreddocs('tripclick/test/tail', count=1165127, items={
            0: GenericScoredDoc('4752', '1306524', 30.8922),
            9: GenericScoredDoc('4752', '472516', 27.3608),
            1165126: GenericScoredDoc('1646719', '1047809', 4.342899),
        })

    def test_docpairs(self):
        self._test_docpairs('tripclick/train', count=23221224, items={
            0: GenericDocPair('338572', '1424623', '725225'),
            9: GenericDocPair('1016988', '7785567', '5019636'),
            23221223: GenericDocPair('3141', '9337445', '9337479'),
        })
        self._test_docpairs('tripclick/train/hofstaetter-triples', count=10000000, items={
            0: GenericDocPair('1064190', '1435361', '9317735'),
            9: GenericDocPair('1053769', '7796753', '10783812'),
            9999999: GenericDocPair('1219267', '9260879', '1194457'),
        })


if __name__ == '__main__':
    unittest.main()
