import re
import unittest
import ir_datasets
from ir_datasets.formats import GenericDoc, GenericQuery, TrecQrel
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
        self._test_queries('mmarco/zh/dev/v1.1', count=101093, items={
            0: GenericQuery('1048578', '無止境的池塘/游泳垃圾邮件的成本'),
            1: GenericQuery('1048579', ' . '),
            9: GenericQuery('1048587', '什么是赞助人?'),
            101092: GenericQuery('524285', '踢踏机嵌入的含義Name'),
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
        self._test_qrels('mmarco/zh/dev/v1.1', count=59273, items={
            0: TrecQrel('1102432', '2026790', 1, '0'),
            9: TrecQrel('300674', '7067032', 1, '0'),
            59272: TrecQrel('371455', '8009476', 1, '0'),
        })

if __name__ == '__main__':
    unittest.main()
