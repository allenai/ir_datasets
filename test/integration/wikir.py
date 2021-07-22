import re
import unittest
from ir_datasets.formats import GenericDoc, GenericQuery, TrecQrel, GenericScoredDoc
from .base import DatasetIntegrationTest


class TestWikir(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('wikir/en1k', count=369721, items={
            0: GenericDoc('1781133', re.compile('^it was used in landing craft during world war ii and is used today in private boats and training fac.{814}gine cooling is via liquid in a water jacket in a boat cool external water is pumped into the engine$', flags=48)),
            9: GenericDoc('931408', re.compile('^they moved south to england to cannock staffordshire where they formed balaam and the angel initiall.{894}es trains and automobiles their presence was largely ignored by the time that their 1993 album prime$', flags=48)),
            369720: GenericDoc('1230943', re.compile('^geetha jeevan born 6 may 1970 is the current mla of thoothukudi constituency in the 15th tamil nadu .{995}etha jeevan was defeated by s t chellapandian of the aiadmk party with about 27000 votes in contrast$', flags=48)),
        })
        self._test_docs('wikir/en59k', count=2454785, items={
            0: GenericDoc('0', re.compile('^these institutions are often described as stateless societies although several authors have defined .{1140}the word anarchism appears in english from 1642 as anarchisme and the word anarchy from 1539 various$', flags=48)),
            9: GenericDoc('9', re.compile('^given annually by the academy of motion picture arts and sciences ampas the awards are an internatio.{1001}demy awards the 91st academy awards ceremony honoring the best films of 2018 was held on february 24$', flags=48)),
            2454784: GenericDoc('2456663', re.compile('^he began his career in gay erotic art in 1978 as illustrator and cover artist for barazoku the first.{1018}o appear disconnected from reality on february 18 2003 kimura died at the age of 56 from a pulmonary$', flags=48)),
        })
        self._test_docs('wikir/en78k', count=2456637, items={
            0: GenericDoc('0', re.compile('^These institutions are often described as stateless societies although several authors have defined .{41155}nthesis anarchists and others of preserving tacitly statist authoritarian or bureaucratic tendencies$', flags=48)),
            9: GenericDoc('9', re.compile('^Given annually by the Academy of Motion Picture Arts and Sciences AMPAS the awards are an internatio.{35352}an language it is used generically to refer to any award or award ceremony regardless of which field$', flags=48)),
            2456636: GenericDoc('2456663', re.compile('^He began his career in gay erotic art in 1978 as illustrator and cover artist for Barazoku the first.{1192}\\-published in 1997 was published shortly after his death His collected works are held by Studio Kaiz$', flags=48)),
        })
        self._test_docs('wikir/ens78k', count=2456637, items={
            0: GenericDoc('0', re.compile('^These institutions are often described as stateless societies although several authors have defined .{41155}nthesis anarchists and others of preserving tacitly statist authoritarian or bureaucratic tendencies$', flags=48)),
            9: GenericDoc('9', re.compile('^Given annually by the Academy of Motion Picture Arts and Sciences AMPAS the awards are an internatio.{35352}an language it is used generically to refer to any award or award ceremony regardless of which field$', flags=48)),
            2456636: GenericDoc('2456663', re.compile('^He began his career in gay erotic art in 1978 as illustrator and cover artist for Barazoku the first.{1192}\\-published in 1997 was published shortly after his death His collected works are held by Studio Kaiz$', flags=48)),
        })
        self._test_docs('wikir/fr14k', count=736616, items={
            0: GenericDoc('0', re.compile('^il est aussi philologue d origine bourbonnaise fils d un notaire de châteaumeillant cher il fait ses.{910}rmé toute une génération de linguistes français parmi lesquels émile benveniste marcel cohen georges$', flags=48)),
            9: GenericDoc('9', re.compile('^ce dernier constituait à l époque de l antiquité un point de passage important sur la route de la so.{949}ndi lieu de la défaite des armées britanniques et rejoint en 1921 la société des nations en 1979 les$', flags=48)),
            736615: GenericDoc('739227', re.compile('^elle co anime le podcast red scare en français peur rouge fille d acrobates nekrasova émigre avec se.{901} émission reçoit des personnalités du milieu de la culture new yorkais nekrasova se dit partisane du$', flags=48)),
        })
        self._test_docs('wikir/es13k', count=645901, items={
            0: GenericDoc('0', re.compile('^su territorio está organizado en siete parroquias con una población total de 78 282 habitantes su ca.{976}e dictaba que se convocaría al somatén formado por los cabezas de familia con nacionalidad andorrana$', flags=48)),
            9: GenericDoc('9', re.compile('^matute fue una de las voces más personales de la literatura española del siglo y es considerada por .{942}en la que presenta influencias de heidi 1880 como el amor por la naturaleza y la relación de la niña$', flags=48)),
            645900: GenericDoc('648294', re.compile('^desde joven se trasladó a panamá con el fin de ejercer el comercio y también la abogacía en ambas ac.{977}entarlas nuevamente a la asamblea lo acogen igual que la bandera mediante la ley 64 de junio de 1904$', flags=48)),
        })
        self._test_docs('wikir/it16k', count=503012, items={
            0: GenericDoc('0', re.compile('^nel primo caso i mantici pompano l aria attraverso una camera del vento o serbatoio réservoir in fra.{1034}la bisogna attendere la metà del secolo con l affermarsi delle prime compagnie e dei primi marchi di$', flags=48)),
            9: GenericDoc('9', re.compile('^le sue esperienze nei primi anni cinquanta come studente della brandeis university nel massachusetts.{1140}bisti che cominciarono a raccoglierli freneticamente ovviamente hoffman puntava a mettere in risalto$', flags=48)),
            503011: GenericDoc('509379', re.compile('^si sposò con giulia de vito da cui ebbe cinque figli luca lorenzo giuseppe giustina angela e frances.{1056}nello stato nel pieno rispetto delle leggi e della giustizia giovanni antonio summonte sosteneva che$', flags=48)),
        })

    def test_queries(self):
        self._test_queries('wikir/en1k/training', count=1444, items={
            0: GenericQuery('123839', 'yanni'),
            9: GenericQuery('563603', 'trinidad and tobago'),
            1443: GenericQuery('341793', 'gloucestershire county cricket club'),
        })
        self._test_queries('wikir/en1k/validation', count=100, items={
            0: GenericQuery('1402535', 'irish sea'),
            9: GenericQuery('8858', 'north america'),
            99: GenericQuery('30711', '1992 summer olympics'),
        })
        self._test_queries('wikir/en1k/test', count=100, items={
            0: GenericQuery('158491', 'southern methodist university'),
            9: GenericQuery('104086', 'bulacan'),
            99: GenericQuery('712704', 'west indies'),
        })
        self._test_queries('wikir/en59k/training', count=57251, items={
            0: GenericQuery('453502', 'ruggero deodato'),
            9: GenericQuery('36255', 'karakum desert'),
            57250: GenericQuery('182474', 'university system of georgia'),
        })
        self._test_queries('wikir/en59k/validation', count=1000, items={
            0: GenericQuery('2230207', '2017 welsh local elections'),
            9: GenericQuery('1325', 'byzantium'),
            999: GenericQuery('1314794', 'cestoda'),
        })
        self._test_queries('wikir/en59k/test', count=1000, items={
            0: GenericQuery('1981012', 'canadian folklore'),
            9: GenericQuery('271014', 'sentinel range'),
            999: GenericQuery('73548', 'lanai'),
        })
        self._test_queries('wikir/en78k/training', count=62904, items={
            0: GenericQuery('368996', 'Germersheim'),
            9: GenericQuery('31044', 'Camp style'),
            62903: GenericQuery('426311', 'Rajshahi District'),
        })
        self._test_queries('wikir/en78k/validation', count=7862, items={
            0: GenericQuery('1411873', 'Auraiya district'),
            9: GenericQuery('1459284', '2010 AFL season'),
            7861: GenericQuery('532819', 'San Carlos canton'),
        })
        self._test_queries('wikir/en78k/test', count=7862, items={
            0: GenericQuery('25182', 'Maria Callas'),
            9: GenericQuery('124328', "1991 FIFA Women's World Cup"),
            7861: GenericQuery('382632', 'Davis Mountains'),
        })
        self._test_queries('wikir/ens78k/training', count=62904, items={
            0: GenericQuery('368996', 'Germersheim is a town in the German state of Rhineland-Palatinate'),
            9: GenericQuery('31044', 'Camp is an aesthetic style and sensibility that regards something'),
            62903: GenericQuery('426311', 'Rajshahi District is a district in mid-western Bangladesh'),
        })
        self._test_queries('wikir/ens78k/validation', count=7862, items={
            0: GenericQuery('1411873', 'Auraiya district is one of the districts of Uttar Pradesh'),
            9: GenericQuery('1459284', 'The 2010 Australian Football League season commenced on 25 March'),
            7861: GenericQuery('532819', 'San Carlos is the 10th canton in the province of'),
        })
        self._test_queries('wikir/ens78k/test', count=7862, items={
            0: GenericQuery('25182', 'Maria Callas Commendatore OMRI December 2 1923 – September 16'),
            9: GenericQuery('124328', "The 1991 FIFA Women's World Cup was the inaugural FIFA"),
            7861: GenericQuery('382632', 'The Davis Mountains originally known as Limpia Mountains are a'),
        })
        self._test_queries('wikir/fr14k/training', count=11341, items={
            0: GenericQuery('390701', 'trait biologique'),
            9: GenericQuery('200590', 'penza'),
            11340: GenericQuery('57294', 'rio uruguay'),
        })
        self._test_queries('wikir/fr14k/validation', count=1400, items={
            0: GenericQuery('82385', 'sagonne'),
            9: GenericQuery('7235', 'vecteur'),
            1399: GenericQuery('57832', 'la panne'),
        })
        self._test_queries('wikir/fr14k/test', count=1400, items={
            0: GenericQuery('13067', 'achille'),
            9: GenericQuery('30895', 'mexico tenochtitlan'),
            1399: GenericQuery('367952', 'carpentras'),
        })
        self._test_queries('wikir/es13k/training', count=11202, items={
            0: GenericQuery('207679', 'penguin random house grupo editorial'),
            9: GenericQuery('9645', 'nave espacial'),
            11201: GenericQuery('50392', 'la massana'),
        })
        self._test_queries('wikir/es13k/validation', count=1300, items={
            0: GenericQuery('191608', 'general hospital'),
            9: GenericQuery('22785', 'mannheim'),
            1299: GenericQuery('231526', 'hindustan aeronautics limited'),
        })
        self._test_queries('wikir/es13k/test', count=1300, items={
            0: GenericQuery('3088', 'isla grande de tierra del fuego'),
            9: GenericQuery('86459', 'k pop'),
            1299: GenericQuery('123345', 'la puebla de almoradiel'),
        })
        self._test_queries('wikir/it16k/training', count=13418, items={
            0: GenericQuery('16956', 'corte penale internazionale'),
            9: GenericQuery('27135', 'antibes'),
            13417: GenericQuery('196572', 'manuele ii paleologo'),
        })
        self._test_queries('wikir/it16k/validation', count=1600, items={
            0: GenericQuery('7528', 'bortigali'),
            9: GenericQuery('70550', 'buda'),
            1599: GenericQuery('27094', 'integrated development environment'),
        })
        self._test_queries('wikir/it16k/test', count=1600, items={
            0: GenericQuery('492243', 'sistema di lancio riutilizzabile'),
            9: GenericQuery('57632', 'devon'),
            1599: GenericQuery('5285', 'arenaria'),
        })

    def test_qrels(self):
        self._test_qrels('wikir/en1k/training', count=47699, items={
            0: TrecQrel('123839', '123839', 2, '0'),
            9: TrecQrel('188629', '1440095', 1, '0'),
            47698: TrecQrel('341793', '1672870', 1, '0'),
        })
        self._test_qrels('wikir/en1k/validation', count=4979, items={
            0: TrecQrel('1402535', '1402535', 2, '0'),
            9: TrecQrel('1402535', '6123', 1, '0'),
            4978: TrecQrel('30711', '1719628', 1, '0'),
        })
        self._test_qrels('wikir/en1k/test', count=4435, items={
            0: TrecQrel('158491', '158491', 2, '0'),
            9: TrecQrel('5728', '5728', 2, '0'),
            4434: TrecQrel('712704', '1577576', 1, '0'),
        })
        self._test_qrels('wikir/en59k/training', count=2443383, items={
            0: TrecQrel('453502', '453502', 2, '0'),
            9: TrecQrel('453502', '2228391', 1, '0'),
            2443382: TrecQrel('182474', '1971100', 1, '0'),
        })
        self._test_qrels('wikir/en59k/validation', count=68905, items={
            0: TrecQrel('2230207', '2230207', 2, '0'),
            9: TrecQrel('105188', '174985', 1, '0'),
            68904: TrecQrel('1314794', '2308683', 1, '0'),
        })
        self._test_qrels('wikir/en59k/test', count=104715, items={
            0: TrecQrel('1981012', '1981012', 2, '0'),
            9: TrecQrel('1164242', '788', 1, '0'),
            104714: TrecQrel('73548', '2377038', 1, '0'),
        })
        self._test_qrels('wikir/en78k/training', count=2435257, items={
            0: TrecQrel('368996', '368996', 2, '0'),
            9: TrecQrel('5737', '11828', 1, '0'),
            2435256: TrecQrel('426311', '2315108', 1, '0'),
        })
        self._test_qrels('wikir/en78k/validation', count=271874, items={
            0: TrecQrel('1411873', '1411873', 2, '0'),
            9: TrecQrel('1944076', '1944076', 2, '0'),
            271873: TrecQrel('532819', '2440624', 1, '0'),
        })
        self._test_qrels('wikir/en78k/test', count=353060, items={
            0: TrecQrel('25182', '25182', 2, '0'),
            9: TrecQrel('174105', '918850', 1, '0'),
            353059: TrecQrel('382632', '1309518', 1, '0'),
        })
        self._test_qrels('wikir/ens78k/training', count=2435257, items={
            0: TrecQrel('368996', '368996', 2, '0'),
            9: TrecQrel('5737', '11828', 1, '0'),
            2435256: TrecQrel('426311', '2315108', 1, '0'),
        })
        self._test_qrels('wikir/ens78k/validation', count=271874, items={
            0: TrecQrel('1411873', '1411873', 2, '0'),
            9: TrecQrel('1944076', '1944076', 2, '0'),
            271873: TrecQrel('532819', '2440624', 1, '0'),
        })
        self._test_qrels('wikir/ens78k/test', count=353060, items={
            0: TrecQrel('25182', '25182', 2, '0'),
            9: TrecQrel('174105', '918850', 1, '0'),
            353059: TrecQrel('382632', '1309518', 1, '0'),
        })
        self._test_qrels('wikir/fr14k/training', count=609240, items={
            0: TrecQrel('390701', '390701', 2, '0'),
            9: TrecQrel('289251', '173579', 1, '0'),
            609239: TrecQrel('57294', '282203', 1, '0'),
        })
        self._test_qrels('wikir/fr14k/validation', count=81255, items={
            0: TrecQrel('82385', '82385', 2, '0'),
            9: TrecQrel('80623', '591242', 1, '0'),
            81254: TrecQrel('57832', '659742', 1, '0'),
        })
        self._test_qrels('wikir/fr14k/test', count=55647, items={
            0: TrecQrel('13067', '13067', 2, '0'),
            9: TrecQrel('1839', '210397', 1, '0'),
            55646: TrecQrel('367952', '711334', 1, '0'),
        })
        self._test_qrels('wikir/es13k/training', count=477212, items={
            0: TrecQrel('207679', '207679', 2, '0'),
            9: TrecQrel('207679', '598495', 1, '0'),
            477211: TrecQrel('50392', '615133', 1, '0'),
        })
        self._test_qrels('wikir/es13k/validation', count=58757, items={
            0: TrecQrel('191608', '191608', 2, '0'),
            9: TrecQrel('191608', '554255', 1, '0'),
            58756: TrecQrel('231526', '276804', 1, '0'),
        })
        self._test_qrels('wikir/es13k/test', count=71339, items={
            0: TrecQrel('3088', '3088', 2, '0'),
            9: TrecQrel('3088', '253926', 1, '0'),
            71338: TrecQrel('123345', '559669', 1, '0'),
        })
        self._test_qrels('wikir/it16k/training', count=381920, items={
            0: TrecQrel('16956', '16956', 2, '0'),
            9: TrecQrel('8993', '8993', 2, '0'),
            381919: TrecQrel('196572', '136444', 1, '0'),
        })
        self._test_qrels('wikir/it16k/validation', count=45003, items={
            0: TrecQrel('7528', '7528', 2, '0'),
            9: TrecQrel('7528', '420171', 1, '0'),
            45002: TrecQrel('27094', '493604', 1, '0'),
        })
        self._test_qrels('wikir/it16k/test', count=49338, items={
            0: TrecQrel('492243', '492243', 2, '0'),
            9: TrecQrel('492243', '493306', 1, '0'),
            49337: TrecQrel('5285', '408735', 1, '0'),
        })

    def test_scoreddocs(self):
        self._test_scoreddocs('wikir/en1k/training', count=144400, items={
            0: GenericScoredDoc('123839', '806300', 20.720094194011075),
            9: GenericScoredDoc('123839', '1901730', 10.324072628860163),
            144399: GenericScoredDoc('341793', '441259', 14.099367141266189),
        })
        self._test_scoreddocs('wikir/en1k/validation', count=10000, items={
            0: GenericScoredDoc('1402535', '681497', 14.974678196110478),
            9: GenericScoredDoc('1402535', '245557', 12.087820628131816),
            9999: GenericScoredDoc('30711', '1705862', 8.521229143068965),
        })
        self._test_scoreddocs('wikir/en1k/test', count=10000, items={
            0: GenericScoredDoc('158491', '625257', 15.660703104969318),
            9: GenericScoredDoc('158491', '13801', 14.515017321771746),
            9999: GenericScoredDoc('712704', '140985', 4.887942090200023),
        })
        self._test_scoreddocs('wikir/en59k/training', count=5725100, items={
            0: GenericScoredDoc('453502', '2228391', 29.22122689830602),
            9: GenericScoredDoc('453502', '1594621', 19.78462456588529),
            5725099: GenericScoredDoc('182474', '669638', 12.514610996375488),
        })
        self._test_scoreddocs('wikir/en59k/validation', count=100000, items={
            0: GenericScoredDoc('2230207', '961529', 18.460208054253798),
            9: GenericScoredDoc('2230207', '2284579', 16.93401066213046),
            99999: GenericScoredDoc('1314794', '818625', 0.0),
        })
        self._test_scoreddocs('wikir/en59k/test', count=100000, items={
            0: GenericScoredDoc('1981012', '1968399', 13.390851551324499),
            9: GenericScoredDoc('1981012', '821056', 8.81720912528983),
            99999: GenericScoredDoc('73548', '818549', 0.0),
        })
        self._test_scoreddocs('wikir/en78k/training', count=6284800, items={
            0: GenericScoredDoc('368996', '1651819', 23.14194046616975),
            9: GenericScoredDoc('368996', '618593', 17.432096830331467),
            6284799: GenericScoredDoc('426311', '97253', 15.529782072936454),
        })
        self._test_scoreddocs('wikir/en78k/validation', count=785700, items={
            0: GenericScoredDoc('1411873', '1579044', 29.78805667499762),
            9: GenericScoredDoc('1411873', '1411879', 19.593174845080704),
            785699: GenericScoredDoc('532819', '456647', 14.645128248017446),
        })
        self._test_scoreddocs('wikir/en78k/test', count=785600, items={
            0: GenericScoredDoc('25182', '1413822', 26.770190544879753),
            9: GenericScoredDoc('25182', '567382', 21.329586618874135),
            785599: GenericScoredDoc('382632', '933740', 11.712924197712407),
        })
        self._test_scoreddocs('wikir/ens78k/training', count=6289800, items={
            0: GenericScoredDoc('368996', '368996', 42.5142628002974),
            9: GenericScoredDoc('368996', '1628082', 33.412777578029235),
            6289799: GenericScoredDoc('426311', '852080', 26.43338402309732),
        })
        self._test_scoreddocs('wikir/ens78k/validation', count=786100, items={
            0: GenericScoredDoc('1411873', '1579044', 48.87893849879893),
            9: GenericScoredDoc('1411873', '2301035', 34.36216854665406),
            786099: GenericScoredDoc('532819', '678583', 17.237808584137724),
        })
        self._test_scoreddocs('wikir/ens78k/test', count=786100, items={
            0: GenericScoredDoc('25182', '25182', 29.514812932099993),
            9: GenericScoredDoc('25182', '887295', 23.03166853881259),
            786099: GenericScoredDoc('382632', '1788341', 17.95820049117319),
        })
        self._test_scoreddocs('wikir/fr14k/training', count=1134100, items={
            0: GenericScoredDoc('390701', '357730', 12.84854585287806),
            9: GenericScoredDoc('390701', '358783', 11.805423649565245),
            1134099: GenericScoredDoc('57294', '431312', 11.597854170542954),
        })
        self._test_scoreddocs('wikir/fr14k/validation', count=140000, items={
            0: GenericScoredDoc('82385', '208929', 19.553876476904705),
            9: GenericScoredDoc('82385', '246297', 0.0),
            139999: GenericScoredDoc('57832', '246388', 0.0),
        })
        self._test_scoreddocs('wikir/fr14k/test', count=140000, items={
            0: GenericScoredDoc('13067', '71891', 24.77999788377413),
            9: GenericScoredDoc('13067', '246295', 0.0),
            139999: GenericScoredDoc('367952', '246382', 0.0),
        })
        self._test_scoreddocs('wikir/es13k/training', count=1120200, items={
            0: GenericScoredDoc('207679', '304542', 37.93233712840836),
            9: GenericScoredDoc('207679', '30053', 24.34821303424021),
            1120199: GenericScoredDoc('50392', '215709', 0.0),
        })
        self._test_scoreddocs('wikir/es13k/validation', count=130000, items={
            0: GenericScoredDoc('191608', '442864', 13.718837032435605),
            9: GenericScoredDoc('191608', '554969', 12.100914887895527),
            129999: GenericScoredDoc('231526', '582771', 9.14032945504454),
        })
        self._test_scoreddocs('wikir/es13k/test', count=130000, items={
            0: GenericScoredDoc('3088', '568822', 16.179207111846104),
            9: GenericScoredDoc('3088', '58331', 12.618831564218615),
            129999: GenericScoredDoc('123345', '215761', 0.0),
        })
        self._test_scoreddocs('wikir/it16k/training', count=1341800, items={
            0: GenericScoredDoc('16956', '108934', 19.476623542001622),
            9: GenericScoredDoc('16956', '470735', 11.086113055208344),
            1341799: GenericScoredDoc('196572', '312933', 8.677228312993646),
        })
        self._test_scoreddocs('wikir/it16k/validation', count=160000, items={
            0: GenericScoredDoc('7528', '509379', 0.0),
            9: GenericScoredDoc('7528', '169121', 0.0),
            159999: GenericScoredDoc('27094', '186973', 9.223293949280931),
        })
        self._test_scoreddocs('wikir/it16k/test', count=160000, items={
            0: GenericScoredDoc('492243', '13937', 22.972013737556047),
            9: GenericScoredDoc('492243', '380824', 10.295899790233975),
            159999: GenericScoredDoc('5285', '169149', 0.0),
        })


if __name__ == '__main__':
    unittest.main()
