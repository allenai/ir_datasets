import re
import unittest
import ir_datasets
from ir_datasets.formats import GenericDoc, GenericQuery, TrecQrel, GenericScoredDoc
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestMMarco(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('mmarco/es', count=8841823, items={
            0: GenericDoc('0', re.compile('^La presencia de la comunicación entre las mentes científicas fue igualmente importante para el éxito.{317}micos es lo que su éxito realmente significó; cientos de miles de vidas inocentes fueron destruidas\\.$', flags=48)),
            9: GenericDoc('9', re.compile('^Una de las principales razones por las que Hanford fue seleccionado como sitio para el Reactor B del.{38}al río Columbia, el río más grande que fluye hacia el Océano Pacífico desde la costa norteamericana\\.$', flags=48)),
            8841822: GenericDoc('8841822', re.compile('^Ver imagen a tamaño completo\\. Detrás de las escenas de la luz deslumbrante muestra que los espectado.{344}cipalmente sales metálicas y óxidos metálicos, que reaccionan para producir una variedad de colores\\.$', flags=48)),
        })
        self._test_docs('mmarco/fr', count=8841823, items={
            0: GenericDoc('0', re.compile('^La présence de la communication au milieu des esprits scientifiques était tout aussi importante pour.{355}st ce que leur succès signifiait vraiment; des centaines de milliers de vies innocentes ont disparu\\.$', flags=48)),
            9: GenericDoc('9', re.compile("^L'une des principales raisons pour lesquelles Hanford a été choisi comme site du réacteur B du proje.{42}re Columbia, la plus grande rivière qui coule dans l'océan Pacifique depuis la côte nord\\-américaine\\.$", flags=48)),
            8841822: GenericDoc('8841822', re.compile("^Affichage de l'image pleine taille\\. Dans les coulisses de la lumière éblouissante montre que les spe.{421} des sels métalliques et des oxydes métalliques, qui réagissent pour produire une gamme de couleurs\\.$", flags=48)),
        })
        self._test_docs('mmarco/pt', count=8841823, items={
            0: GenericDoc('0', re.compile('^A presença da comunicação em meio às mentes científicas era igualmente importante para o sucesso do .{294}micos é o que seu sucesso realmente significava; centenas de milhares de vidas inocentes destruídas\\.$', flags=48)),
            9: GenericDoc('9', re.compile('^Uma das principais razões pelas quais Hanford foi selecionado como um local para o Reator B do Proje.{22}roximidade com o Rio Columbia, o maior rio que flui para o Oceano Pacífico da costa norte\\-americana\\.$', flags=48)),
            8841822: GenericDoc('8841822', re.compile('^Ver imagem em tamanho completo\\. Atrás das cenas da luz deslumbrante mostra que os espectadores ooh e.{332}, principalmente sais metálicos e óxidos metálicos, que reagem para produzir uma variedade de cores\\.$', flags=48)),
        })
        self._test_docs('mmarco/it', count=8841823, items={
            0: GenericDoc('0', re.compile('^La presenza della comunicazione tra le menti scientifiche era altrettanto importante per il successo.{327}ò che il loro successo ha veramente significato; centinaia di migliaia di vite innocenti annientati\\.$', flags=48)),
            9: GenericDoc('9', re.compile("^Uno dei motivi principali per cui Hanford è stato scelto come sito per il reattore B del Progetto Ma.{28} al fiume Columbia, il più grande fiume che scorreva nell'Oceano Pacifico dalla costa nordamericana\\.$", flags=48)),
            8841822: GenericDoc('8841822', re.compile('^Visualizza immagine a grandezza naturale\\. Dietro le quinte della luce abbagliante mostra che gli spe.{353} principalmente sali metallici e ossidi di metallo, che reagiscono per produrre una serie di colori\\.$', flags=48)),
        })
        self._test_docs('mmarco/id', count=8841823, items={
            0: GenericDoc('0', re.compile('^Kehadiran komunikasi di tengah pikiran ilmiah sama pentingnya dengan keberhasilan Proyek Manhattan s.{249} adalah apa sebenarnya tujuan kesuksesan mereka; ratusan ribu nyawa yang tidak bersalah dilenyapkan\\.$', flags=48)),
            9: GenericDoc('9', 'Salah satu alasan utama Hanford dipilih sebagai lokasi untuk Reaktor B Proyek Manhattan adalah dekatnya ke Sungai Columbia, sungai terbesar yang mengalir ke Samudra Pasifik dari pesisir Amerika Utara.'),
            8841822: GenericDoc('8841822', re.compile('^Tilik gambar ukuran penuh\\. Di balik layar cahaya yang menyilaukan itu, para penonton memperlihatkan .{356}a khusus, terutama garam logam dan logam oksida, yang bereaksi untuk menghasilkan serangkaian warna\\.$', flags=48)),
        })
        self._test_docs('mmarco/de', count=8841823, items={
            0: GenericDoc('0', re.compile('^Die Präsenz der Kommunikation inmitten wissenschaftlicher Köpfe war für den Erfolg des Manhattan\\-Pro.{306}ängt, ist, was ihr Erfolg wirklich bedeutete; Hunderttausende unschuldiger Leben wurden ausgelöscht\\.$', flags=48)),
            9: GenericDoc('9', re.compile('^Einer der Hauptgründe, weshalb Hanford als Standort für den B Reactor des Manhattan\\-Projekts ausgewä.{34}mbia River, dem größten Fluss, der von der nordamerikanischen Küste in den Pazifischen Ozean fließt\\.$', flags=48)),
            8841822: GenericDoc('8841822', re.compile('^Sehen Sie das Bild in voller Größe an\\. Hinter den Kulissen des blendenden Lichts zeigt sich, dass di.{332}üllt, vor allem Metallsalze und Metalloxide, die reagieren, um eine Reihe von Farben zu produzieren\\.$', flags=48)),
        })
        self._test_docs('mmarco/ru', count=8841823, items={
            0: GenericDoc('0', re.compile('^▪ Присутствие общения в научных кругах имело не менее важное значение для успеха Манхэттенского прое.{286} то, что их успех действительно имел в виду; сотни тысяч ни в чем не повинных людей были уничтожены\\.$', flags=48)),
            9: GenericDoc('9', re.compile('^• Одной из главных причин, по которой Хэнфорд был выбран в качестве объекта для реактора B Манхэттен.{28}ость к реке Колумбия, крупнейшей реке, которая течет в Тихий океан от североамериканского побережья\\.$', flags=48)),
            8841822: GenericDoc('8841822', re.compile('^Посмотрите на изображение полного размера\\. За сценами ослепляющего света видно, что зрители 4 июля т.{329}бразом металлическими солями и оксидами металлов, которые реагируют на появление целого ряда цветов\\.$', flags=48)),
        })
        self._test_docs('mmarco/zh', count=8841823, items={
            0: GenericDoc('0', '科学思想中的交流对曼哈顿项目的成功同样重要,就像科学智慧一样。 科学思想中的交流对曼哈顿项目的成功同样重要,就像科学智慧一样。 原子研究者与工程师们唯一令人印象深刻的成就是他们的成功真正意味着什么;数十万无辜生命被毁灭。'),
            9: GenericDoc('9', 'Hanford被选为曼哈顿项目B反应堆场地的主要原因之一是它靠近哥伦比亚河,这是从北美海岸流入太平洋的最大河流。'),
            8841822: GenericDoc('8841822', '查看全尺寸图像 。 @ action: inmenu 7月4日的觀眾Oh和Ahh是精心策劃的煙火。 無論是紅、白、藍噴泉或紫煙花, 在每件手工制的烟火中,都有装满特殊化学品的小包,主要是金属盐和氧化金属,它们的反应是产生多种颜色。'),
        })

    def test_queries(self):
        self._test_queries('mmarco/es/train', count=808731, items={
            0: GenericQuery('121352', 'Definir extrema'),
            9: GenericQuery('492875', 'Temperatura del desinfectante'),
            808730: GenericQuery('50393', 'Beneficios de hervir limones y beber jugo.'),
        })
        self._test_queries('mmarco/es/dev', count=101092, items={
            0: GenericQuery('1048578', 'coste de interminables piscinas / spa de natación'),
            9: GenericQuery('1048587', '¿Qué es el patrón?'),
            101091: GenericQuery('524285', 'Cinta de correr inclinada significado'),
        })
        self._test_queries('mmarco/es/dev/small', count=6980, items={
            0: GenericQuery('1048585', '¿Qué es el hermano de Paula Deen?'),
            9: GenericQuery('524699', 'Número del servicio de tricare'),
            6979: GenericQuery('1048565', 'que juega michaelis sebastian'),
        })
        self._test_queries('mmarco/fr/train', count=808731, items={
            0: GenericQuery('121352', 'Définition extrême'),
            9: GenericQuery('492875', 'Température du désinfectant'),
            808730: GenericQuery('50393', 'Les bienfaits des citrons bouillants et du jus de boisson.'),
        })
        self._test_queries('mmarco/fr/dev', count=101093, items={
            0: GenericQuery('1048578', 'Coût des piscines sans fin / spa de natation'),
            9: GenericQuery('1048587', "Qu'est-ce que le patron"),
            101092: GenericQuery('524285', "Sens de l'inclinaison du tapis roulant"),
        })
        self._test_queries('mmarco/fr/dev/small', count=6980, items={
            0: GenericQuery('1048585', "Qu'est-ce que le frère de Paula Deen?"),
            9: GenericQuery('524699', 'Numéro de service tricare'),
            6979: GenericQuery('1048565', 'Qui joue des michaélis sébastes'),
        })
        self._test_queries('mmarco/pt/train', count=811690, items={
            0: GenericQuery('121352', 'Definir extremo'),
            9: GenericQuery('492875', 'Temperatura do desinfectante'),
            760: GenericQuery('1009273', 'Qual evangelista se tornou um ateu depois de realizar muitas reuniões?'),
            761: GenericQuery('1009273', 'O nome dele era Chuck...'),
            811689: GenericQuery('50393', 'Benefícios de ferver limão e beber suco.'),
        })
        self._test_queries('mmarco/pt/dev', count=101619, items={
            0: GenericQuery('1048578', 'custo de piscinas intermináveis / spa de banho'),
            9: GenericQuery('1048587', 'O que é patrono?'),
            29: GenericQuery('52', 'Pneumo é um prefixo que significa ar.'),
            30: GenericQuery('52', 'Sabendo disso, explique por que essa condição é chamada pneumotórax.'),
            101618: GenericQuery('524285', 'Esteira inclinada significando'),
        })
        self._test_queries('mmarco/pt/dev/small', count=7000, items={
            0: GenericQuery('1048585', 'O que é o irmão de Paula Deen?'),
            9: GenericQuery('524699', 'Número do serviço tricare.'),
            6999: GenericQuery('1048565', 'que joga sebastiano michaelis'),
        })
        self._test_queries('mmarco/pt/train/v1.1', count=808731, items={
            0: GenericQuery('121352', 'Definir extremo'),
            9: GenericQuery('492875', 'Temperatura do desinfectante'),
            760: GenericQuery('1009273', 'Qual evangelista se tornou um ateu depois de realizar muitas reuniões? O nome dele era Chuck...'),
            808730: GenericQuery('50393', 'Benefícios de ferver limão e beber suco.'),
        })
        self._test_queries('mmarco/pt/dev/v1.1', count=101093, items={
            0: GenericQuery('1048578', 'custo de piscinas intermináveis / spa de banho'),
            9: GenericQuery('1048587', 'O que é patrono?'),
            29: GenericQuery('52', 'Pneumo é um prefixo que significa ar. Sabendo disso, explique por que essa condição é chamada pneumotórax.'),
            101092: GenericQuery('524285', 'Esteira inclinada significando'),
        })
        self._test_queries('mmarco/pt/dev/small/v1.1', count=6980, items={
            0: GenericQuery('1048585', 'O que é o irmão de Paula Deen?'),
            9: GenericQuery('524699', 'Número do serviço tricare.'),
            6979: GenericQuery('1048565', 'que joga sebastiano michaelis'),
        })
        self._test_queries('mmarco/it/train', count=808731, items={
            0: GenericQuery('121352', "L'ente creditizio definisce l'estremo"),
            9: GenericQuery('492875', 'Temperatura di sanificante di polistirolo'),
            808730: GenericQuery('50393', 'Benefici di limone bollente e succo di bevuta.'),
        })
        self._test_queries('mmarco/it/dev', count=101093, items={
            0: GenericQuery('1048578', 'Costo delle infinite piscine/piscine termali'),
            9: GenericQuery('1048587', '[47] ciò che è patrono'),
            101092: GenericQuery('524285', 'Tapis roulant inclinazione significato'),
        })
        self._test_queries('mmarco/it/dev/small', count=6980, items={
            0: GenericQuery('1048585', "Cos'è il fratello di Paula deen?"),
            9: GenericQuery('524699', 'Numero di servizio di tricare del benzile'),
            6979: GenericQuery('1048565', 'Coccodrillo che gioca a michaelis Sebastian'),
        })
        self._test_queries('mmarco/id/train', count=808731, items={
            0: GenericQuery('121352', 'Definisi ekstrim'),
            9: GenericQuery('492875', 'Suhu pembersih'),
            808730: GenericQuery('50393', 'Manfaat lemon mendidih dan jus minum.'),
        })
        self._test_queries('mmarco/id/dev', count=101093, items={
            0: GenericQuery('1048578', 'Biaya kolam renang tak berujung/swim spa'),
            9: GenericQuery('1048587', 'Apa yang menjadi pelindungnya?'),
            101092: GenericQuery('524285', 'Arti dari treadmill treadmill'),
        })
        self._test_queries('mmarco/id/dev/small', count=6980, items={
            0: GenericQuery('1048585', 'Apa itu kakak paula deen'),
            9: GenericQuery('524699', 'Nomor layanan tricare'),
            6979: GenericQuery('1048565', 'Yang memainkan Sebastian Michaelis.'),
        })
        self._test_queries('mmarco/de/train', count=808731, items={
            0: GenericQuery('121352', 'Bestimmen Sie extreme'),
            9: GenericQuery('492875', 'Temperatur des Reinigungsmittels'),
            808730: GenericQuery('50393', 'Die Vorteile von kochenden Zitronen und Trinksaft.'),
        })
        self._test_queries('mmarco/de/dev', count=101093, items={
            0: GenericQuery('1048578', 'Kosten für endlose Pools/Schwimmbad'),
            9: GenericQuery('1048587', 'Was ist der Schutzpatron?'),
            101092: GenericQuery('524285', 'Laufband Neigung Bedeutung'),
        })
        self._test_queries('mmarco/de/dev/small', count=6980, items={
            0: GenericQuery('1048585', 'Was ist Paula deens Bruder?'),
            9: GenericQuery('524699', 'Nummer des Tricare-Services'),
            6979: GenericQuery('1048565', 'Wer spielt sebastian michaelis'),
        })
        self._test_queries('mmarco/ru/train', count=808731, items={
            0: GenericQuery('121352', '□ определить крайность'),
            9: GenericQuery('492875', '▸ Температура обеззараживающих веществ'),
            808730: GenericQuery('50393', '- выгоды от кипящего лимона и сока.'),
        })
        self._test_queries('mmarco/ru/dev', count=101093, items={
            0: GenericQuery('1048578', '- стоимость бесконечных пулов/плавучих спа'),
            9: GenericQuery('1048587', '□ что является покровителем'),
            101092: GenericQuery('524285', '&quot; беговая дорожка &quot; означает &quot; наклонная дорожка &quot;.'),
        })
        self._test_queries('mmarco/ru/dev/small', count=6980, items={
            0: GenericQuery('1048585', 'Что такое брат Паулы Дина?'),
            9: GenericQuery('524699', '□ номер службы трехразового ухода'),
            6979: GenericQuery('1048565', 'который играет себастьяна Михаэлиса'),
        })
        self._test_queries('mmarco/zh/train', count=808731, items={
            0: GenericQuery('121352', '定义极端'),
            9: GenericQuery('492875', '净化剂温度'),
            808730: GenericQuery('50393', '煮柠檬和喝果汁的好处。'),
        })
        self._test_queries('mmarco/zh/dev', count=101093, items={
            0: GenericQuery('1048578', '無止境的池塘/游泳垃圾邮件的成本'),
            1: GenericQuery('1048579', ''),
            9: GenericQuery('1048587', '什么是赞助人?'),
            101092: GenericQuery('524285', '踢踏机嵌入的含義Name'),
        })
        self._test_queries('mmarco/zh/dev/small', count=6980, items={
            0: GenericQuery('1048585', '{\\fn黑体\\fs22\\bord1\\shad0\\3aHBE\\4aH00\\fscx67\\fscy66\\2cHFFFFFF\\3cH808080}Paula Deen的弟弟是什么 {\\fn黑体\\fs22\\bord1\\shad0\\3aHBE\\4aH00\\fscx67\\fscy66\\2cHFFFFFF\\3cH808080}'),
            9: GenericQuery('524699', '三角护理服务数量@ info: whatsthis'),
            6979: GenericQuery('1048565', '扮演塞巴斯蒂安·米切利斯'),
        })
        self._test_queries('mmarco/zh/dev/v1.1', count=101093, items={
            0: GenericQuery('1048578', '無止境的池塘/游泳垃圾邮件的成本'),
            1: GenericQuery('1048579', ' . '),
            9: GenericQuery('1048587', '什么是赞助人?'),
            101092: GenericQuery('524285', '踢踏机嵌入的含義Name'),
        })
        self._test_queries('mmarco/zh/dev/small/v1.1', count=6980, items={
            0: GenericQuery('1048585', '{\\fn黑体\\fs22\\bord1\\shad0\\3aHBE\\4aH00\\fscx67\\fscy66\\2cHFFFFFF\\3cH808080}Paula Deen的弟弟是什么 {\\fn黑体\\fs22\\bord1\\shad0\\3aHBE\\4aH00\\fscx67\\fscy66\\2cHFFFFFF\\3cH808080}'),
            9: GenericQuery('524699', '三角护理服务数量@ info: whatsthis'),
            6979: GenericQuery('1048565', '扮演塞巴斯蒂安·米切利斯'),
        })

    def test_qrels(self):
        self._test_qrels('mmarco/es/train', count=532761, items={
            0: TrecQrel('1185869', '0', 1, '0'),
            9: TrecQrel('186154', '1160', 1, '0'),
            532760: TrecQrel('405466', '8841735', 1, '0'),
        })
        self._test_qrels('mmarco/es/dev', count=59273, items={
            0: TrecQrel('1102432', '2026790', 1, '0'),
            9: TrecQrel('300674', '7067032', 1, '0'),
            59272: TrecQrel('371455', '8009476', 1, '0'),
        })
        self._test_qrels('mmarco/es/dev/small', count=7437, items={
            0: TrecQrel('300674', '7067032', 1, '0'),
            9: TrecQrel('54544', '7068203', 1, '0'),
            7436: TrecQrel('195199', '8009377', 1, '0'),
        })
        self._test_qrels('mmarco/fr/train', count=532761, items={
            0: TrecQrel('1185869', '0', 1, '0'),
            9: TrecQrel('186154', '1160', 1, '0'),
            532760: TrecQrel('405466', '8841735', 1, '0'),
        })
        self._test_qrels('mmarco/fr/dev', count=59273, items={
            0: TrecQrel('1102432', '2026790', 1, '0'),
            9: TrecQrel('300674', '7067032', 1, '0'),
            59272: TrecQrel('371455', '8009476', 1, '0'),
        })
        self._test_qrels('mmarco/fr/dev/small', count=7437, items={
            0: TrecQrel('300674', '7067032', 1, '0'),
            9: TrecQrel('54544', '7068203', 1, '0'),
            7436: TrecQrel('195199', '8009377', 1, '0'),
        })
        self._test_qrels('mmarco/it/train', count=532761, items={
            0: TrecQrel('1185869', '0', 1, '0'),
            9: TrecQrel('186154', '1160', 1, '0'),
            532760: TrecQrel('405466', '8841735', 1, '0'),
        })
        self._test_qrels('mmarco/it/dev', count=59273, items={
            0: TrecQrel('1102432', '2026790', 1, '0'),
            9: TrecQrel('300674', '7067032', 1, '0'),
            59272: TrecQrel('371455', '8009476', 1, '0'),
        })
        self._test_qrels('mmarco/it/dev/small', count=7437, items={
            0: TrecQrel('300674', '7067032', 1, '0'),
            9: TrecQrel('54544', '7068203', 1, '0'),
            7436: TrecQrel('195199', '8009377', 1, '0'),
        })
        self._test_qrels('mmarco/id/train', count=532761, items={
            0: TrecQrel('1185869', '0', 1, '0'),
            9: TrecQrel('186154', '1160', 1, '0'),
            532760: TrecQrel('405466', '8841735', 1, '0'),
        })
        self._test_qrels('mmarco/id/dev', count=59273, items={
            0: TrecQrel('1102432', '2026790', 1, '0'),
            9: TrecQrel('300674', '7067032', 1, '0'),
            59272: TrecQrel('371455', '8009476', 1, '0'),
        })
        self._test_qrels('mmarco/id/dev/small', count=7437, items={
            0: TrecQrel('300674', '7067032', 1, '0'),
            9: TrecQrel('54544', '7068203', 1, '0'),
            7436: TrecQrel('195199', '8009377', 1, '0'),
        })
        self._test_qrels('mmarco/pt/train', count=532761, items={
            0: TrecQrel('1185869', '0', 1, '0'),
            9: TrecQrel('186154', '1160', 1, '0'),
            532760: TrecQrel('405466', '8841735', 1, '0'),
        })
        self._test_qrels('mmarco/pt/dev', count=59273, items={
            0: TrecQrel('1102432', '2026790', 1, '0'),
            9: TrecQrel('300674', '7067032', 1, '0'),
            59272: TrecQrel('371455', '8009476', 1, '0'),
        })
        self._test_qrels('mmarco/pt/dev/small', count=7437, items={
            0: TrecQrel('300674', '7067032', 1, '0'),
            9: TrecQrel('54544', '7068203', 1, '0'),
            7436: TrecQrel('195199', '8009377', 1, '0'),
        })
        self._test_qrels('mmarco/pt/train/v1.1', count=532761, items={
            0: TrecQrel('1185869', '0', 1, '0'),
            9: TrecQrel('186154', '1160', 1, '0'),
            532760: TrecQrel('405466', '8841735', 1, '0'),
        })
        self._test_qrels('mmarco/pt/dev/v1.1', count=59273, items={
            0: TrecQrel('1102432', '2026790', 1, '0'),
            9: TrecQrel('300674', '7067032', 1, '0'),
            59272: TrecQrel('371455', '8009476', 1, '0'),
        })
        self._test_qrels('mmarco/pt/dev/small/v1.1', count=7437, items={
            0: TrecQrel('300674', '7067032', 1, '0'),
            9: TrecQrel('54544', '7068203', 1, '0'),
            7436: TrecQrel('195199', '8009377', 1, '0'),
        })
        self._test_qrels('mmarco/de/train', count=532761, items={
            0: TrecQrel('1185869', '0', 1, '0'),
            9: TrecQrel('186154', '1160', 1, '0'),
            532760: TrecQrel('405466', '8841735', 1, '0'),
        })
        self._test_qrels('mmarco/de/dev', count=59273, items={
            0: TrecQrel('1102432', '2026790', 1, '0'),
            9: TrecQrel('300674', '7067032', 1, '0'),
            59272: TrecQrel('371455', '8009476', 1, '0'),
        })
        self._test_qrels('mmarco/de/dev/small', count=7437, items={
            0: TrecQrel('300674', '7067032', 1, '0'),
            9: TrecQrel('54544', '7068203', 1, '0'),
            7436: TrecQrel('195199', '8009377', 1, '0'),
        })
        self._test_qrels('mmarco/ru/train', count=532761, items={
            0: TrecQrel('1185869', '0', 1, '0'),
            9: TrecQrel('186154', '1160', 1, '0'),
            532760: TrecQrel('405466', '8841735', 1, '0'),
        })
        self._test_qrels('mmarco/ru/dev', count=59273, items={
            0: TrecQrel('1102432', '2026790', 1, '0'),
            9: TrecQrel('300674', '7067032', 1, '0'),
            59272: TrecQrel('371455', '8009476', 1, '0'),
        })
        self._test_qrels('mmarco/ru/dev/small', count=7437, items={
            0: TrecQrel('300674', '7067032', 1, '0'),
            9: TrecQrel('54544', '7068203', 1, '0'),
            7436: TrecQrel('195199', '8009377', 1, '0'),
        })
        self._test_qrels('mmarco/zh/train', count=532761, items={
            0: TrecQrel('1185869', '0', 1, '0'),
            9: TrecQrel('186154', '1160', 1, '0'),
            532760: TrecQrel('405466', '8841735', 1, '0'),
        })
        self._test_qrels('mmarco/zh/dev', count=59273, items={
            0: TrecQrel('1102432', '2026790', 1, '0'),
            9: TrecQrel('300674', '7067032', 1, '0'),
            59272: TrecQrel('371455', '8009476', 1, '0'),
        })
        self._test_qrels('mmarco/zh/dev/small', count=7437, items={
            0: TrecQrel('300674', '7067032', 1, '0'),
            9: TrecQrel('54544', '7068203', 1, '0'),
            7436: TrecQrel('195199', '8009377', 1, '0'),
        })
        self._test_qrels('mmarco/zh/dev/v1.1', count=59273, items={
            0: TrecQrel('1102432', '2026790', 1, '0'),
            9: TrecQrel('300674', '7067032', 1, '0'),
            59272: TrecQrel('371455', '8009476', 1, '0'),
        })
        self._test_qrels('mmarco/zh/dev/small/v1.1', count=7437, items={
            0: TrecQrel('300674', '7067032', 1, '0'),
            9: TrecQrel('54544', '7068203', 1, '0'),
            7436: TrecQrel('195199', '8009377', 1, '0'),
        })

    def test_scoreddocs(self):
        self._test_scoreddocs('mmarco/es/dev/small', count=6786720, items={
            0: GenericScoredDoc('2', '1782337', 12.7035),
            9: GenericScoredDoc('2', '4339073', 11.2667),
            6786719: GenericScoredDoc('1102400', '533560', 5.598184),
        })
        self._test_scoreddocs('mmarco/fr/dev/small', count=6785763, items={
            0: GenericScoredDoc('2', '1001873', 12.766),
            9: GenericScoredDoc('2', '6285819', 11.793),
            6785762: GenericScoredDoc('1102400', '1123059', 5.5288),
        })
        self._test_scoreddocs('mmarco/pt/dev/small/v1.1', count=6976324, items={
            0: GenericScoredDoc('2', '1782337', 13.6133),
            9: GenericScoredDoc('2', '3996546', 11.7405),
            6976323: GenericScoredDoc('1102400', '8613556', 6.592),
        })
        self._test_scoreddocs('mmarco/it/dev/small', count=6966491, items={
            0: GenericScoredDoc('2', '1782337', 13.0295),
            9: GenericScoredDoc('2', '2022784', 11.7611),
            6966490: GenericScoredDoc('1102400', '4411180', 5.498484),
        })
        self._test_scoreddocs('mmarco/id/dev/small', count=6841990, items={
            0: GenericScoredDoc('2', '1782337', 13.2769),
            9: GenericScoredDoc('2', '7496504', 11.857),
            6841989: GenericScoredDoc('1102400', '8018446', 4.710481),
        })
        self._test_scoreddocs('mmarco/de/dev/small', count=6594126, items={
            0: GenericScoredDoc('2', '2022779', 9.7118),
            9: GenericScoredDoc('2', '2022785', 7.9781),
            6594125: GenericScoredDoc('1102400', '1954600', 5.524396),
        })
        self._test_scoreddocs('mmarco/ru/dev/small', count=6958739, items={
            0: GenericScoredDoc('2', '6285817', 12.2203),
            9: GenericScoredDoc('2', '513654', 10.9697),
            6958738: GenericScoredDoc('1102400', '3863095', 5.499489),
        })
        self._test_scoreddocs('mmarco/zh/dev/small/v1.1', count=1034597, items={
            0: GenericScoredDoc('1215', '6593209', 2.2469),
            9: GenericScoredDoc('1215', '2815463', 2.237397),
            1034596: GenericScoredDoc('1102393', '3789102', 5.4537),
        })

    def test_docpairs(self):
        self._test_docpairs('mmarco/es/train', count=39780811, items={
            0: GenericDocPair('400296', '1540783', '3518497'),
            9: GenericDocPair('189845', '1051356', '4238671'),
            39780810: GenericDocPair('749547', '394235', '7655192'),
        })
        self._test_docpairs('mmarco/fr/train', count=39780811, items={
            0: GenericDocPair('400296', '1540783', '3518497'),
            9: GenericDocPair('189845', '1051356', '4238671'),
            39780810: GenericDocPair('749547', '394235', '7655192'),
        })
        self._test_docpairs('mmarco/pt/train', count=39780811, items={
            0: GenericDocPair('400296', '1540783', '3518497'),
            9: GenericDocPair('189845', '1051356', '4238671'),
            39780810: GenericDocPair('749547', '394235', '7655192'),
        })
        self._test_docpairs('mmarco/it/train', count=39780811, items={
            0: GenericDocPair('400296', '1540783', '3518497'),
            9: GenericDocPair('189845', '1051356', '4238671'),
            39780810: GenericDocPair('749547', '394235', '7655192'),
        })
        self._test_docpairs('mmarco/id/train', count=39780811, items={
            0: GenericDocPair('400296', '1540783', '3518497'),
            9: GenericDocPair('189845', '1051356', '4238671'),
            39780810: GenericDocPair('749547', '394235', '7655192'),
        })
        self._test_docpairs('mmarco/de/train', count=39780811, items={
            0: GenericDocPair('400296', '1540783', '3518497'),
            9: GenericDocPair('189845', '1051356', '4238671'),
            39780810: GenericDocPair('749547', '394235', '7655192'),
        })
        self._test_docpairs('mmarco/ru/train', count=39780811, items={
            0: GenericDocPair('400296', '1540783', '3518497'),
            9: GenericDocPair('189845', '1051356', '4238671'),
            39780810: GenericDocPair('749547', '394235', '7655192'),
        })
        self._test_docpairs('mmarco/zh/train', count=39780811, items={
            0: GenericDocPair('400296', '1540783', '3518497'),
            9: GenericDocPair('189845', '1051356', '4238671'),
            39780810: GenericDocPair('749547', '394235', '7655192'),
        })


if __name__ == '__main__':
    unittest.main()
