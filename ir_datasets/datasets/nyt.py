import io
import tarfile
from typing import NamedTuple
import ir_datasets
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.util import Lazy, DownloadConfig, Migrator
from ir_datasets.datasets.base import Dataset, FilteredQueries, FilteredQrels, YamlDocumentation
from ir_datasets.formats import BaseDocs, BaseQueries, BaseQrels, GenericQuery, GenericQrel, TrecQueries, TrecQrels


NAME = 'nyt'


QREL_DEFS = {
    1: 'title is associated with article body',
}

CORE_QREL_DEFS = {
    0: "not relevant",
    1: "relevant",
    2: "highly relevant",
}


VALID_IDS = {'1206388', '46335', '1223589', '1642970', '144845', '420493', '1186325', '564166', '1092844', '1232733', '243508', '946470', '1147459', '84957', '87385', '1298633', '1327402', '1482333', '1069716', '1575477', '1110091', '655579', '1562062', '541298', '1571257', '639395', '1341710', '663400', '1174700', '1406944', '1368755', '1315376', '1609162', '1746895', '1447812', '193348', '882027', '213652', '126658', '799474', '1677212', '1254313', '43743', '250901', '426439', '1803638', '1111630', '1220244', '1142672', '944176', '860862', '342011', '1556809', '1574691', '292048', '855559', '1473717', '157893', '252570', '305646', '198014', '1444467', '1842149', '161276', '455333', '146910', '1414339', '1413851', '1352725', '509114', '563685', '1738087', '1115555', '639541', '427073', '1435887', '862324', '476212', '870108', '315852', '144389', '684154', '845724', '117999', '35935', '716125', '1818546', '551762', '687923', '1817616', '135841', '618338', '1597113', '1549790', '1292666', '147051', '1778945', '1347630', '1337511', '299371', '1384273', '388274', '938995', '263847', '195638', '303927', '646946', '1620311', '1455534', '325463', '1380230', '1038853', '1040633', '1831119', '363686', '260491', '1611855', '147526', '542544', '581106', '1766627', '899656', '236785', '1408409', '300748', '742732', '986023', '1662861', '1083296', '152722', '1458233', '1203328', '1810235', '996231', '1226680', '427277', '517560', '1230947', '185677', '1524891', '492603', '1023515', '334223', '1219069', '1021319', '152336', '1227959', '1501876', '765819', '395940', '524179', '1494335', '66871', '105130', '1660760', '744794', '1616161', '876120', '714837', '35529', '42617', '198139', '1811671', '147293', '1041065', '841417', '1346509', '200467', '850536', '1235945', '184078', '1269259', '1314141', '1368414', '387436', '896464', '84650', '375608', '423014', '1201696', '883245', '137547', '1376881', '1207160', '280170', '968570', '1438840', '626732', '1085071', '632127', '1206647', '399973', '1316303', '1187122', '805546', '1727291', '570037', '1178896', '555992', '977573', '1340396', '632958', '63542', '1280664', '977205', '1567169', '783676', '814977', '1668678', '1735184', '1074278', '1652858', '1108702', '955404', '1784962', '1185130', '250831', '818408', '623624', '134405', '104342', '965709', '956076', '1260229', '27255', '1500603', '1127679', '1722973', '1734641', '309555', '1681934', '695555', '48767', '433808', '995051', '180797', '123367', '378006', '1216681', '324683', '1711346', '211935', '1801492', '103678', '446767', '594334', '860460', '660793', '1393998', '266826', '876460', '994066', '1282229', '1587147', '815344', '1103826', '343997', '1200405', '179480', '742314', '1780439', '1066709', '1330760', '1368900', '1549318', '1110897', '619788', '188464', '173770', '34154', '578909', '645650', '1157537', '62836', '700552', '1388063', '408649', '848686', '1694615', '1617883', '1765655', '1466678', '155464', '1445513', '1303273', '231804', '581627', '742052', '1212886', '1405769', '481040', '1855639', '54259', '111905', '1313586', '387001', '1185491', '1670617', '906527', '69825', '499522', '1819890', '164762', '970999', '1179216', '993221', '372699', '296270', '1185999', '792835', '1037962', '1740374', '1624046', '954664', '368818', '1087747', '1026355', '812422', '1544110', '1226870', '155570', '1190376', '869921', '296349', '595907', '614301', '1241703', '442373', '995807', '1369864', '1709789', '114305', '184927', '1120202', '584073', '828184', '1473187', '1521230', '440704', '1013610', '1830313', '721770', '1658974', '313921', '692325', '368461', '985252', '290240', '1251117', '1538562', '422046', '1630032', '1181653', '125066', '1837263', '1656997', '441', '490006', '1643057', '165954', '69049', '1199388', '1507218', '1329673', '509136', '1466695', '16687', '508419', '268880', '969961', '340902', '253378', '256155', '863620', '1683671', '1560798', '675553', '1748098', '458865', '1665924', '1055150', '66385', '215071', '13148', '986080', '236365', '517825', '873311', '441741', '720189', '572737', '1225926', '624119', '997868', '515426', '691257', '419206', '1130476', '100471', '6461', '1807548', '1544601', '407787', '380030', '1152266', '1065150', '694778', '811554', '1854529', '444117', '1099590', '922315', '1217477', '1779802', '369061', '775743', '72992', '144419', '552889', '1181556', '1292830', '1778514', '1489202', '914269', '1706337', '1196929', '184181', '314027', '1227737', '559948', '784834', '1704396', '1256508', '1508836', '317087', '96486', '747998', '1632274', '950708', '1649807', '446890', '593993', '814566', '1292672', '560408', '1077779', '978883', '393982', '844217', '398230', '183055', '53060', '1210135', '916178', '1532407', '1139738', '1518821', '728959', '1304148', '491724', '1568275', '712403', '1728481', '660217', '821176', '1222683', '1778005', '1195123', '1817074', '974513', '426701', '1111638', '1240027', '1664639', '1464379', '521007', '1199739', '578456', '1439699', '284928', '494919', '491912', '232568', '923474', '99386', '1643092', '1790124', '1061993', '621986', '1122877', '100662', '1473138', '1030173', '71586', '1096287', '1138157', '262640', '602945', '1300130', '1338721', '1270177', '39801', '1692635', '56624', '211659', '1646283', '324374', '255385', '1255526', '1786203', '1406143', '1788514', '289251', '672936', '452286', '137862', '185683', '1430', '1380422', '845912', '775802', '647375', '145796', '355527', '146542', '1410218', '345442', '190717', '371036', '1797336', '120994', '1718571', '1054043', '4558', '428059', '1396897', '1201117', '1158485', '1089656', '519981', '43015', '520964', '1494349', '1094063', '1392684', '978574', '1052143', '1118795', '1687088', '1314160', '162771', '911024', '1820168', '1192318', '91766', '143489', '1004985', '518421', '166275', '370104', '974150', '546915', '1323563', '1798085', '938123', '182313', '1364401', '9506', '557187', '112370', '611777', '1159485', '1403348', '683930', '797900', '1383582', '114608', '350383', '1604331', '568871', '1047323', '394651', '165898', '283949', '810556', '105425', '1013875', '1464119', '1312394', '1695169', '58536', '1169598', '1125874', '1665958', '769476', '594319', '683707', '882361', '1302321', '450679', '254550', '1033539', '1301128', '1320428', '41154', '1657029', '1227578', '171871', '1792745', '288902', '453868', '271254', '409591', '143722', '535764', '1830350', '578047', '230266', '111402', '773754', '1245031', '1350576', '1624207', '1807992', '1015799', '1794740', '511024', '789525', '319777', '1132669', '1327710', '1272568', '1390168', '1533260', '617767', '638910', '496086', '1205039', '1626665', '191596', '1810513', '1556267', '1100153', '207238', '1501543', '834402', '279588', '568816', '1632682', '822260', '343317', '430137', '1768788', '545282', '279954', '165473', '828347', '1470816', '1327112', '1529515', '1016007', '270386', '1702078', '286404', '1088273', '1322387', '1643857', '489043', '380855', '1083556', '1619528', '583350', '132853', '546862', '1253587', '535138', '264437', '943235', '1620828', '1006607', '553760', '828792', '1624460', '1434951', '833541', '212690', '200229', '1064862', '220330', '1579543', '363926', '1258350', '1184051', '720391', '1459592', '457690', '38548', '81369', '1679222', '390074', '286007', '378270', '816642', '283001', '372084', '411601', '910971', '1590440', '135775', '1112005', '75424', '213834', '689492', '1005355', '1139329', '808335', '720425', '1267233', '263546', '1222854', '258056', '837513', '940506', '1103175', '1378900', '1385626', '237112', '730612', '301649', '273771', '497029', '736059', '1193481', '797044', '1144902', '1030001', '719277', '1119289', '1337197', '942773', '982474', '584235', '1707268', '1754255', '1104478', '1534921', '128481', '470969', '347013', '509587', '408644', '772685', '1733430', '1317735', '848134', '404829', '267884', '953680', '1303696', '884333', '968388', '1201708', '1112434', '303328', '1304264', '1133757', '1724836', '1334405', '1829066', '925761', '946016', '552534', '943383', '1100246', '1846843', '1088146', '544438', '1753939', '74810', '1807078', '100915', '1236323', '803592', '429972', '393687', '1378937', '456043', '1613185', '613184', '417913', '1563559', '1339387', '1502489', '656071', '365604', '1151482', '1259752', '277596', '673808', '161493', '873580', '832327', '260612', '924572', '1064547', '1125330', '1641045', '1151695', '256879', '394244', '556588', '1305678', '1263185', '136826', '1399892', '557148', '1358190', '1776190', '249236', '1492533', '1303288', '521017', '1066272', '541133', '1623539', '137859', '687241', '237814', '1369332', '371264', '24081', '1552898', '1502059', '1047404', '1023221', '177279', '1267817', '1411135', '191656', '980600', '951516', '499404', '1695509', '811244', '238763', '1284303', '585143', '1033260', '942257', '1349353', '1429932', '140492', '1044892', '418808', '698145', '1796223', '59227', '194957', '269275', '730734', '1145222', '253742', '581098', '45351', '66070', '426605', '1050966', '529688', '1801056', '1718077', '1266182', '129555', '1531233', '74473', '302447', '215843', '792070', '1104761', '1573381', '202553', '60314', '1503921', '280964', '711987', '136821', '832921', '1419515', '1662966', '1819530', '716942', '219736', '436016', '1735969', '713752', '60858', '121707', '689812', '193395', '1624062', '1330056', '563645', '1492653', '1449544', '376209', '1750188', '1478352', '410699', '777880', '1029514', '108914', '720269', '1448513', '74549', '972109', '215002', '404357', '1647764', '550693', '1255375', '1293865', '1264570', '896848', '789563', '826347', '903589', '1018558', '277290', '1683375', '1496790', '1112399', '860557', '127350', '1015623', '312660', '233953', '1565217', '1639977', '1607902', '397905', '490534', '1513419', '174443', '1215224', '66269', '275494', '209655', '516500', '1675849', '836893', '947869', '789401', '1553981', '155710', '496679', '821652', '1139493', '286234', '128146', '1207153', '1199733', '1778364', '1704065', '326315', '317132', '1824346', '319345', '1219375', '99297', '1850878', '755324', '1737932', '1556261', '1389561', '128767', '24850', '1105008', '1046487', '390245', '899371', '623036', '1190883', '1218126', '334762', '1496567', '1228970', '540795', '689403', '1465965', '1585171', '734591', '1257610', '685476', '784313', '1178416', '1468942', '883627', '1000719', '952670', '51709', '933442'}


class NytDoc(NamedTuple):
    doc_id: str
    headline: str
    body: str
    source_xml: str


class NytDocs(BaseDocs):
    def __init__(self, dlc):
        self._dlc = dlc

    def docs_path(self, force=True):
        return self._dlc.path(force)

    def docs_cls(self):
        return NytDoc

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        BeautifulSoup = ir_datasets.lazy_libs.bs4().BeautifulSoup
        with self._dlc.stream() as stream:
            with tarfile.open(fileobj=stream, mode='r|gz') as tgz_outer:
                for member_o in tgz_outer:
                    if not member_o.isfile() or not (member_o.name.endswith('.tar') or member_o.name.endswith('.tgz')):
                        continue
                    file = tgz_outer.extractfile(member_o)
                    with tarfile.open(fileobj=file, mode='r|gz' if member_o.name.endswith('.tgz') else 'r|') as tgz_inner:
                        for member_i in tgz_inner:
                            if not member_i.isfile():
                                continue
                            full_xml = tgz_inner.extractfile(member_i).read()
                            soup = BeautifulSoup(full_xml, 'lxml-xml')
                            did = soup.find('doc-id')
                            did = did['id-string'] if did else ''
                            headline = soup.find('hl1') # 'headline' element can contain multiple (e.g. hl2 for online)
                            headline = headline.get_text() if headline else ''
                            full_text = soup.find('block', {'class': 'full_text'})
                            full_text = full_text.get_text().strip() if full_text else ''
                            yield NytDoc(did, headline, full_text, full_xml)

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{self.docs_path()}.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=ir_datasets.util.count_hint(NAME),
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'

class NytQueries(BaseQueries):
    def __init__(self, collection):
        self._collection = collection

    def queries_iter(self):
        for doc in self._collection.docs_iter():
            yield GenericQuery(doc.doc_id, doc.headline)

    def queries_namespace(self):
        return NAME

    def queries_lang(self):
        return 'en'


class NytQrels(BaseQrels):
    def __init__(self, collection):
        self._collection = collection

    def qrels_iter(self):
        for doc in self._collection.docs_iter():
            yield GenericQrel(doc.doc_id, doc.doc_id, 1)

    def qrels_defs(self):
        return QREL_DEFS


def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    migrator = Migrator(base_path/'irds_version.txt', 'v2',
        affected_files=[base_path/'nyt.tgz.pklz4'],
        message='Migrating nyt (extracting body text)')

    collection = migrator(NytDocs(dlc['source']))

    base = Dataset(collection, documentation('_'))

    # core17
    subsets['trec-core-2017'] = Dataset(
        TrecQueries(dlc['trec-core-2017/queries'], namespace='trec-core-2017', lang='en'),
        TrecQrels(dlc['trec-core-2017/qrels'], CORE_QREL_DEFS),
        collection,
        documentation('trec-core-2017'))

    # wksup
    all_queries = NytQueries(collection)
    all_qrels = NytQrels(collection)
    match_qids = Lazy(lambda: VALID_IDS)
    subsets['wksup'] = Dataset(
        all_queries,
        all_qrels,
        collection,
        documentation('wksup/train'))
    subsets['wksup/train'] = Dataset(
        FilteredQueries(all_queries, match_qids, mode='exclude'),
        FilteredQrels(all_qrels, match_qids, mode='exclude'),
        collection,
        documentation('wksup/train'))
    subsets['wksup/valid'] = Dataset(
        FilteredQueries(all_queries, match_qids, mode='include'),
        FilteredQrels(all_qrels, match_qids, mode='include'),
        collection,
        documentation('wksup/valid'))

    ir_datasets.registry.register('nyt', base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'nyt/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
