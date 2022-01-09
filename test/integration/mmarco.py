import re
import unittest
import ir_datasets
from ir_datasets.formats import GenericDoc, GenericQuery, TrecQrel, GenericScoredDoc, GenericDocPair
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

        self._test_docs('mmarco/v2/ar', count=8841823, items={
            0: GenericDoc('0', re.compile('^كان وجود التواصل وسط العقول العلمية مهمًا بنفس القدر لنجاح مشروع مانهاتن مثل الفكر العلمي\\. السحابة ا.{18} الإنجاز الرائع للباحثين والمهندسين الذريين هو ما يعنيه نجاحهم حقًا ؛ مئات الآلاف من الأبرياء طمسوا\\.$', flags=48)),
            9: GenericDoc('9', 'كان أحد الأسباب الرئيسية لاختيار هانفورد كموقع لمشروع مانهاتن B Reactor هو قربه من نهر كولومبيا ، أكبر نهر يتدفق إلى المحيط الهادئ من ساحل أمريكا الشمالية.'),
            8841822: GenericDoc('8841822', re.compile('^عرض الصورة بالحجم الكامل\\. خلف الكواليس ، يظهر الضوء المبهر أن المتفرجين في الرابع من يوليو ، هم ألعا.{226}بمواد كيميائية خاصة ، خاصة الأملاح المعدنية وأكاسيد المعادن ، والتي تتفاعل لإنتاج مجموعة من الألوان\\.$', flags=48)),
        })
        self._test_docs('mmarco/v2/zh', count=8841823, items={
            0: GenericDoc('0', '科学头脑中的交流对于曼哈顿计划的成功与科学智力同等重要。笼罩着原子研究人员和工程师令人印象深刻的成就的唯一乌云是他们的成功真正意味着什么；数十万无辜的生命被抹杀。'),
            9: GenericDoc('9', '汉福德被选为曼哈顿项目 B 反应堆选址的主要原因之一是它靠近哥伦比亚河，哥伦比亚河是从北美海岸流入太平洋的最大河流。'),
            8841822: GenericDoc('8841822', '查看全尺寸图像。 在耀眼的灯光背后，七月四日的观众哦和啊，都是精心制作的烟花。 无论是红色、白色和蓝色的喷泉，还是紫色的烟花，每个烟花都含有恰到好处的化学物质组合，以创造出这些五颜六色的灯光。 每个手工制作的烟花里面都有小包，里面装满了特殊的化学物质，主要是金属盐和金属氧化物，它们会发生反应，产生一系列颜色。'),
        })
        self._test_docs('mmarco/v2/dt', count=8841823, items={
            0: GenericDoc('0', re.compile('^De aanwezigheid van communicatie te midden van wetenschappelijke geesten was even belangrijk voor he.{157}enieurs hangt, is wat hun succes werkelijk betekende; honderdduizenden onschuldige levens uitgewist\\.$', flags=48)),
            9: GenericDoc('9', re.compile('^Een van de belangrijkste redenen waarom Hanford werd geselecteerd als locatie voor de B Reactor van .{46} Columbia River, de grootste rivier die vanaf de Noord\\-Amerikaanse kust in de Stille Oceaan stroomt\\.$', flags=48)),
            8841822: GenericDoc('8841822', re.compile('^Bekijk de afbeelding op volledige grootte\\. Achter de schermen van het verblindende licht is te zien .{329}aliën, voornamelijk metaalzouten en metaaloxiden, die reageren om een \u200b\u200breeks kleuren te produceren\\.$', flags=48)),
        })
        self._test_docs('mmarco/v2/fr', count=8841823, items={
            0: GenericDoc('0', re.compile('^La présence de la communication parmi les esprits scientifiques était tout aussi importante pour le .{158}est ce que leur succès signifiait vraiment ; des centaines de milliers de vies innocentes anéanties\\.$', flags=48)),
            9: GenericDoc('9', re.compile("^L'une des principales raisons pour lesquelles Hanford a été choisi comme site pour le réacteur B du .{46}euve Columbia, le plus grand fleuve se jetant dans l'océan Pacifique depuis la côte nord\\-américaine\\.$", flags=48)),
            8841822: GenericDoc('8841822', re.compile("^Voir l'image en taille réelle\\. Dans les coulisses des spectacles de lumière éblouissante que les spe.{414} des sels métalliques et des oxydes métalliques, qui réagissent pour produire une gamme de couleurs\\.$", flags=48)),
        })
        self._test_docs('mmarco/v2/de', count=8841823, items={
            0: GenericDoc('0', re.compile('^Die Präsenz der Kommunikation unter wissenschaftlichen Köpfen war für den Erfolg des Manhattan\\-Proje.{118}eure schwebt nur, was ihr Erfolg wirklich bedeutete; Hunderttausende unschuldiger Leben ausgelöscht\\.$', flags=48)),
            9: GenericDoc('9', re.compile('^Einer der Hauptgründe, warum Hanford als Standort für den B\\-Reaktor des Manhattan\\-Projekts ausgewähl.{30}mbia River, dem größten Fluss, der von der nordamerikanischen Küste in den Pazifischen Ozean mündet\\.$', flags=48)),
            8841822: GenericDoc('8841822', re.compile('^Bild in voller Größe anzeigen\\. Hinter den Kulissen der gleißenden Lichtershows, die die Zuschauer am.{349}d, hauptsächlich Metallsalzen und Metalloxiden, die reagieren, um eine Reihe von Farben zu erzeugen\\.$', flags=48)),
        })
        self._test_docs('mmarco/v2/hi', count=8841823, items={
            0: GenericDoc('0', re.compile('^वैज्ञानिक बुद्धि के रूप में मैनहट्टन परियोजना की सफलता के लिए वैज्ञानिक दिमाग के बीच संचार की उपस्थि.{111}का हुआ एकमात्र बादल उनकी सफलता का सही मायने में मतलब है; सैकड़ों हजारों निर्दोष लोगों की जान चली गई।$', flags=48)),
            9: GenericDoc('9', re.compile('^मैनहट्टन प्रोजेक्ट के बी रिएक्टर के लिए हनफोर्ड को एक साइट के रूप में चुने जाने के मुख्य कारणों में .{9}ंबिया नदी से इसकी निकटता थी, जो उत्तरी अमेरिकी तट से प्रशांत महासागर में बहने वाली सबसे बड़ी नदी थी।$', flags=48)),
            8841822: GenericDoc('8841822', re.compile('^पूर्ण आकार की छवि देखें। चकाचौंध रोशनी के दृश्यों के पीछे से पता चलता है कि चार जुलाई को दर्शक ऊह और.{284} से धातु के लवण और धातु के आक्साइड, जो रंगों की एक सरणी का उत्पादन करने के लिए प्रतिक्रिया करते हैं।$', flags=48)),
        })
        self._test_docs('mmarco/v2/id', count=8841823, items={
            0: GenericDoc('0', re.compile('^Kehadiran komunikasi di tengah pikiran ilmiah sama pentingnya dengan keberhasilan Proyek Manhattan s.{130} atom adalah apa arti kesuksesan mereka yang sebenarnya; ratusan ribu nyawa tak berdosa dilenyapkan\\.$', flags=48)),
            9: GenericDoc('9', re.compile('^Salah satu alasan utama Hanford dipilih sebagai lokasi Reaktor B Proyek Manhattan adalah kedekatanny.{1} dengan Sungai Columbia, sungai terbesar yang mengalir ke Samudra Pasifik dari pantai Amerika Utara\\.$', flags=48)),
            8841822: GenericDoc('8841822', re.compile('^Lihat gambar ukuran penuh\\. Di balik layar cahaya yang menyilaukan menunjukkan bahwa penonton ooh dan.{326}imia khusus, terutama garam logam dan oksida logam, yang bereaksi untuk menghasilkan berbagai warna\\.$', flags=48)),
        })
        self._test_docs('mmarco/v2/it', count=8841823, items={
            0: GenericDoc('0', re.compile('^La presenza della comunicazione tra le menti scientifiche era altrettanto importante per il successo.{164} ciò che significava veramente il loro successo; centinaia di migliaia di vite innocenti cancellate\\.$', flags=48)),
            9: GenericDoc('9', re.compile("^Uno dei motivi principali per cui Hanford è stata scelta come sito per il reattore B del Progetto Ma.{30}za al fiume Columbia, il più grande fiume che scorre nell'Oceano Pacifico dalla costa nordamericana\\.$", flags=48)),
            8841822: GenericDoc('8841822', re.compile("^Visualizza l'immagine a dimensione intera\\. Dietro le quinte degli spettacoli di luce abbagliante che.{380}, principalmente sali metallici e ossidi metallici, che reagiscono per produrre una serie di colori\\.$", flags=48)),
        })
        self._test_docs('mmarco/v2/ja', count=8841823, items={
            0: GenericDoc('0', 'マンハッタン計画の成功には、科学的知性と同様に、科学的精神の中でのコミュニケーションの存在も同様に重要でした。原子研究者とエンジニアの印象的な業績にぶら下がっている唯一の雲は、彼らの成功が本当に意味したことです。何十万もの罪のない命が失われました。'),
            9: GenericDoc('9', 'ハンフォードがマンハッタン計画のB原子炉の場所として選ばれた主な理由のひとつは、北米沿岸から太平洋に流れ込む最大の川であるコロンビア川に近いことでした。'),
            8841822: GenericDoc('8841822', re.compile('^フルサイズの画像を表示します。 まばゆいばかりの光の舞台裏では、7月4日の観客が慎重に作られた花火であることがわかります。 赤、白、青の噴水でも紫の線香花火でも、各花火にはこれらのカラフルなライトを作.{2}るための化学物質の適切な組み合わせが詰め込まれています。 それぞれの手作りの花火の中には、特殊な化学物質、主に金属塩と金属酸化物で満たされた小さなパケットがあり、これらは反応して一連の色を生成します。$', flags=48)),
        })
        self._test_docs('mmarco/v2/pt', count=8841823, items={
            0: GenericDoc('0', re.compile('^A presença de comunicação entre mentes científicas foi tão importante para o sucesso do Projeto Manh.{128}ômicos é o que seu sucesso realmente significou; centenas de milhares de vidas inocentes destruídas\\.$', flags=48)),
            9: GenericDoc('9', re.compile('^Um dos principais motivos pelos quais Hanford foi selecionado como local para o Reator B do Projeto .{24}idade com o Rio Columbia, o maior rio que deságua no Oceano Pacífico vindo da costa norte\\-americana\\.$', flags=48)),
            8841822: GenericDoc('8841822', re.compile('^Veja a imagem em tamanho grande\\. Nos bastidores da luz deslumbrante mostra que os espectadores ooh e.{329}s especiais, principalmente sais e óxidos de metal, que reagem para produzir uma variedade de cores\\.$', flags=48)),
        })
        self._test_docs('mmarco/v2/ru', count=8841823, items={
            0: GenericDoc('0', re.compile('^Наличие связи между научными умами было столь же важно для успеха Манхэттенского проекта, как и науч.{100}ов\\-атомщиков, \\- это то, что на самом деле означало их успех; уничтожены сотни тысяч невинных жизней\\.$', flags=48)),
            9: GenericDoc('9', re.compile('^Одной из основных причин, по которой Хэнфорд был выбран в качестве площадки для реактора B Манхэттен.{20}его близость к реке Колумбия, крупнейшей реке, впадающей в Тихий океан с побережья Северной Америки\\.$', flags=48)),
            8841822: GenericDoc('8841822', re.compile('^Посмотреть полноразмерное изображение\\. За кулисами ослепительный свет показывает, что зрители ох и о.{334}, в основном солями металлов и оксидами металлов, которые реагируют с образованием множества цветов\\.$', flags=48)),
        })
        self._test_docs('mmarco/v2/es', count=8841823, items={
            0: GenericDoc('0', re.compile('^La presencia de comunicación entre mentes científicas fue tan importante para el éxito del Proyecto .{135}ros atómicos es lo que realmente significó su éxito; cientos de miles de vidas inocentes destruidas\\.$', flags=48)),
            9: GenericDoc('9', re.compile('^Una de las principales razones por las que Hanford fue seleccionado como sitio para el Reactor B del.{45}Columbia, el río más grande que desemboca en el Océano Pacífico desde la costa de América del Norte\\.$', flags=48)),
            8841822: GenericDoc('8841822', re.compile('^Ver imagen a tamaño completo\\. Detrás de las escenas de los espectáculos de luces deslumbrantes que l.{390}cipalmente sales metálicas y óxidos metálicos, que reaccionan para producir una variedad de colores\\.$', flags=48)),
        })
        self._test_docs('mmarco/v2/vi', count=8841823, items={
            0: GenericDoc('0', re.compile('^Sự hiện diện của giao tiếp giữa những bộ óc khoa học cũng quan trọng không kém đối với sự thành công.{139}n tử là thành công của họ thực sự có ý nghĩa như thế nào; hàng trăm ngàn sinh mạng vô tội bị xóa sổ\\.$', flags=48)),
            9: GenericDoc('9', 'Một trong những lý do chính khiến Hanford được chọn làm nơi đặt Lò phản ứng B của Dự án Manhattan là nó nằm gần sông Columbia, con sông lớn nhất đổ ra Thái Bình Dương từ bờ biển Bắc Mỹ.'),
            8841822: GenericDoc('8841822', re.compile('^Xem hình ảnh kích thước đầy đủ\\. Phía sau hậu trường của ánh sáng rực rỡ cho thấy khán giả ooh và ahh.{283} đầy hóa chất đặc biệt, chủ yếu là muối kim loại và oxit kim loại, phản ứng tạo ra một loạt màu sắc\\.$', flags=48)),
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

        self._test_queries('mmarco/v2/ar/train', count=808731, items={
            0: GenericQuery('1', 'يعتبر بوتلاتش مثالاً على'),
            9: GenericQuery('14', ' كانت سامانثا وجوزفين يستعدان لبدء تجارة الملابس الخاصة بهما ؛ اتصلوا'),
            808730: GenericQuery('1185869', ') ما هو الأثر المباشر لنجاح مشروع مانهاتن؟'),
        })
        self._test_queries('mmarco/v2/ar/dev', count=101093, items={
            0: GenericQuery('2', 'تحديد مستقبلات الاندروجين'),
            9: GenericQuery('106', 'تعريف تقسيم "كوم متعدد الوحدات"'),
            101092: GenericQuery('1102432', '. ما هي الشركة؟'),
        })
        self._test_queries('mmarco/v2/ar/dev/small', count=6980, items={
            0: GenericQuery('2', 'تحديد مستقبلات الاندروجين'),
            9: GenericQuery('5925', 'سوني PS-LX300USB كيفية الاتصال بجهاز الكمبيوتر'),
            6979: GenericQuery('1102400', 'لماذا الدببة السبات'),
        })
        self._test_queries('mmarco/v2/zh/train', count=808731, items={
            0: GenericQuery('1', '一个potlatch被认为是一个例子'),
            9: GenericQuery('14', ' 萨曼莎和约瑟芬正准备开始自己的服装生意；他们联系了'),
            808730: GenericQuery('1185869', ') 曼哈顿项目成功的直接影响是什么？'),
        })
        self._test_queries('mmarco/v2/zh/dev', count=101093, items={
            0: GenericQuery('2', '雄激素受体定义'),
            9: GenericQuery('106', '“com 多单元”分区定义'),
            101092: GenericQuery('1102432', '.什么是公司？'),
        })
        self._test_queries('mmarco/v2/zh/dev/small', count=6980, items={
            0: GenericQuery('2', '雄激素受体定义'),
            9: GenericQuery('5925', '索尼 PS-LX300USB 如何连接电脑'),
            6979: GenericQuery('1102400', '熊为什么要冬眠'),
        })
        self._test_queries('mmarco/v2/dt/train', count=808731, items={
            0: GenericQuery('1', 'Een potlatch wordt beschouwd als een voorbeeld van:'),
            9: GenericQuery('14', ' Samantha en Josephine waren zich aan het voorbereiden om hun eigen kledingzaak te beginnen; ze namen contact op'),
            808730: GenericQuery('1185869', ')wat was de onmiddellijke impact van het succes van het Manhattan-project?'),
        })
        self._test_queries('mmarco/v2/dt/dev', count=101093, items={
            0: GenericQuery('2', 'Androgeenreceptor definiëren'),
            9: GenericQuery('106', "'com multi unit' bestemmingsdefinitie"),
            101092: GenericQuery('1102432', '. wat is een corporatie?'),
        })
        self._test_queries('mmarco/v2/dt/dev/small', count=6980, items={
            0: GenericQuery('2', 'Androgeenreceptor definiëren'),
            9: GenericQuery('5925', 'Sony PS-LX300USB hoe te verbinden met pc'),
            6979: GenericQuery('1102400', 'waarom overwinteren beren?'),
        })
        self._test_queries('mmarco/v2/fr/train', count=808731, items={
            0: GenericQuery('121352', "définir l'extrême"),
            9: GenericQuery('492875', 'température du désinfectant'),
            808730: GenericQuery('50393', 'avantages de faire bouillir des citrons et de boire du jus.'),
        })
        self._test_queries('mmarco/v2/fr/dev', count=101093, items={
            0: GenericQuery('1048578', 'coût des piscines sans fin / spa de nage'),
            9: GenericQuery('1048587', "qu'est-ce que le patron"),
            101092: GenericQuery('524285', "signification de l'inclinaison du tapis roulant"),
        })
        self._test_queries('mmarco/v2/fr/dev/small', count=6980, items={
            0: GenericQuery('1048585', 'quel est le frère de paula deen'),
            9: GenericQuery('524699', 'numéro de service tricare'),
            6979: GenericQuery('1048565', 'qui joue sebastian michaelis'),
        })
        self._test_queries('mmarco/v2/de/train', count=808731, items={
            0: GenericQuery('121352', 'Extrem definieren'),
            9: GenericQuery('492875', 'Desinfektionsmitteltemperatur'),
            808730: GenericQuery('50393', 'Vorteile von Zitronen kochen und Saft trinken.'),
        })
        self._test_queries('mmarco/v2/de/dev', count=101093, items={
            0: GenericQuery('1048578', 'Kosten für endlose Pools/Swimming Spa'),
            9: GenericQuery('1048587', 'was ist Patron'),
            101092: GenericQuery('524285', 'Laufbandneigung bedeutung'),
        })
        self._test_queries('mmarco/v2/de/dev/small', count=6980, items={
            0: GenericQuery('1048585', 'was ist paula deens bruder'),
            9: GenericQuery('524699', 'Tricare-Servicenummer'),
            6979: GenericQuery('1048565', 'wer spielt sebastian michaelis'),
        })
        self._test_queries('mmarco/v2/hi/train', count=808731, items={
            0: GenericQuery('1', 'पॉटलैच को का एक उदाहरण माना जाता है'),
            9: GenericQuery('14', ' सामंथा और जोसफीन अपना खुद का कपड़ों का व्यवसाय शुरू करने की तैयारी कर रहे थे; उन्होंने संपर्क किया'),
            808730: GenericQuery('1185869', ') मैनहट्टन परियोजना की सफलता का तत्काल प्रभाव क्या था?'),
        })
        self._test_queries('mmarco/v2/hi/dev', count=101093, items={
            0: GenericQuery('2', 'एण्ड्रोजन रिसेप्टर परिभाषित'),
            9: GenericQuery('106', "'कॉम मल्टी यूनिट' ज़ोनिंग परिभाषा"),
            101092: GenericQuery('1102432', '. एक निगम क्या है?'),
        })
        self._test_queries('mmarco/v2/hi/dev/small', count=6980, items={
            0: GenericQuery('2', 'एण्ड्रोजन रिसेप्टर परिभाषित'),
            9: GenericQuery('5925', 'Sony PS-LX300USB पीसी से कैसे कनेक्ट करें'),
            6979: GenericQuery('1102400', 'भालू हाइबरनेट क्यों करते हैं?'),
        })
        self._test_queries('mmarco/v2/id/train', count=808731, items={
            0: GenericQuery('1', 'Potlatch dianggap sebagai contoh dari'),
            9: GenericQuery('14', ' Samantha dan Josephine sedang bersiap untuk memulai bisnis pakaian mereka sendiri; mereka menghubungi'),
            808730: GenericQuery('1185869', ') apa dampak langsung dari keberhasilan proyek manhattan?'),
        })
        self._test_queries('mmarco/v2/id/dev', count=101093, items={
            0: GenericQuery('2', 'Reseptor androgen menentukan'),
            9: GenericQuery('106', "Definisi zonasi 'com multi unit'"),
            101092: GenericQuery('1102432', '. apa itu korporasi?'),
        })
        self._test_queries('mmarco/v2/id/dev/small', count=6980, items={
            0: GenericQuery('2', 'Reseptor androgen menentukan'),
            9: GenericQuery('5925', 'Sony PS-LX300USB cara menghubungkan ke pc'),
            6979: GenericQuery('1102400', 'mengapa beruang hibernasi'),
        })
        self._test_queries('mmarco/v2/it/train', count=808731, items={
            0: GenericQuery('121352', 'definire estremo'),
            9: GenericQuery('492875', 'temperatura del disinfettante'),
            808730: GenericQuery('50393', 'benefici di bollire i limoni e bere succo.'),
        })
        self._test_queries('mmarco/v2/it/dev', count=101093, items={
            0: GenericQuery('1048578', 'costo di piscine infinite/spa'),
            9: GenericQuery('1048587', "cos'è il patrono?"),
            101092: GenericQuery('524285', 'significato di inclinazione del tapis roulant'),
        })
        self._test_queries('mmarco/v2/it/dev/small', count=6980, items={
            0: GenericQuery('1048585', "cos'è il fratello di paula deen?"),
            9: GenericQuery('524699', 'numero di servizio tricare'),
            6979: GenericQuery('1048565', 'chi interpreta sebastian michaelis'),
        })
        self._test_queries('mmarco/v2/ja/train', count=808731, items={
            0: GenericQuery('1', 'ポトラッチはの例と見なされます'),
            9: GenericQuery('14', ' サマンサとジョセフィンは、独自の衣料品事業を始める準備をしていました。彼らは連絡した'),
            808730: GenericQuery('1185869', '）マンハッタン計画の成功の直接の影響は何でしたか？'),
        })
        self._test_queries('mmarco/v2/ja/dev', count=101093, items={
            0: GenericQuery('2', 'アンドロゲン受容体の定義'),
            9: GenericQuery('106', '「commultiunit」ゾーニング定義'),
            101092: GenericQuery('1102432', '。法人とは？'),
        })
        self._test_queries('mmarco/v2/ja/dev/small', count=6980, items={
            0: GenericQuery('2', 'アンドロゲン受容体の定義'),
            9: GenericQuery('5925', 'ソニーPS-LX300USBPCへの接続方法'),
            6979: GenericQuery('1102400', 'なぜクマは冬眠するのですか'),
        })
        self._test_queries('mmarco/v2/pt/train', count=808731, items={
            0: GenericQuery('121352', 'definir extremo'),
            9: GenericQuery('492875', 'temperatura do desinfetante'),
            808730: GenericQuery('50393', 'benefícios de ferver limões e beber suco.'),
        })
        self._test_queries('mmarco/v2/pt/dev', count=101093, items={
            0: GenericQuery('1048578', 'custo de piscinas infinitas / spa de natação'),
            9: GenericQuery('1048587', 'o que é patrono'),
            101092: GenericQuery('524285', 'significado de inclinação da esteira'),
        })
        self._test_queries('mmarco/v2/pt/dev/small', count=6980, items={
            0: GenericQuery('1048585', 'o que é irmão de paula deen'),
            9: GenericQuery('524699', 'número do serviço tricare'),
            6979: GenericQuery('1048565', 'quem joga sebastian michaelis'),
        })
        self._test_queries('mmarco/v2/ru/train', count=808731, items={
            0: GenericQuery('1', 'Потлач считается примером'),
            9: GenericQuery('14', ' Саманта и Жозефина готовились открыть собственный бизнес по производству одежды; они связались'),
            808730: GenericQuery('1185869', ') каково было непосредственное влияние успеха манхэттенского проекта?'),
        })
        self._test_queries('mmarco/v2/ru/dev', count=101093, items={
            0: GenericQuery('2', 'Рецепторы андрогенов определяют'),
            9: GenericQuery('106', 'Определение зонирования com multi unit'),
            101092: GenericQuery('1102432', '. что такое корпорация?'),
        })
        self._test_queries('mmarco/v2/ru/dev/small', count=6980, items={
            0: GenericQuery('2', 'Рецепторы андрогенов определяют'),
            9: GenericQuery('5925', 'Sony PS-LX300USB как подключить к ПК'),
            6979: GenericQuery('1102400', 'почему медведи впадают в спячку'),
        })
        self._test_queries('mmarco/v2/es/train', count=808731, items={
            0: GenericQuery('121352', 'definir extremo'),
            9: GenericQuery('492875', 'temperatura del desinfectante'),
            808730: GenericQuery('50393', 'Beneficios de hervir limones y beber jugo.'),
        })
        self._test_queries('mmarco/v2/es/dev', count=101093, items={
            0: GenericQuery('1048578', 'costo de piscinas infinitas / spa de natación'),
            9: GenericQuery('1048587', 'que es patron'),
            101092: GenericQuery('524285', 'significado de la inclinación de la cinta de correr'),
        })
        self._test_queries('mmarco/v2/es/dev/small', count=6980, items={
            0: GenericQuery('1048585', 'que es el hermano de paula deen'),
            9: GenericQuery('524699', 'número de servicio tricare'),
            6979: GenericQuery('1048565', 'quien interpreta a sebastian michaelis'),
        })
        self._test_queries('mmarco/v2/vi/train', count=808731, items={
            0: GenericQuery('1', 'Một potlatch được coi là một ví dụ của'),
            9: GenericQuery('14', ' Samantha và Josephine đang chuẩn bị bắt đầu kinh doanh quần áo của riêng họ; họ đã liên hệ'),
            808730: GenericQuery('1185869', ') tác động tức thời đến sự thành công của dự án manhattan là gì?'),
        })
        self._test_queries('mmarco/v2/vi/dev', count=101093, items={
            0: GenericQuery('2', 'Xác định thụ thể androgen'),
            9: GenericQuery('106', "định nghĩa phân vùng 'com multi unit'"),
            101092: GenericQuery('1102432', '. một công ty là gì?'),
        })
        self._test_queries('mmarco/v2/vi/dev/small', count=6980, items={
            0: GenericQuery('2', 'Xác định thụ thể androgen'),
            9: GenericQuery('5925', 'Cách kết nối Sony PS-LX300USB với PC'),
            6979: GenericQuery('1102400', 'tại sao gấu ngủ đông'),
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
        for ds in ['mmarco/v2/ar', 'mmarco/v2/zh', 'mmarco/v2/dt', 'mmarco/v2/fr', 'mmarco/v2/de', 'mmarco/v2/hi', 'mmarco/v2/id', 'mmarco/v2/it', 'mmarco/v2/ja', 'mmarco/v2/pt', 'mmarco/v2/ru', 'mmarco/v2/es', 'mmarco/v2/vi']:
            self._test_qrels(f'{ds}/train', count=532761, items={
                0: TrecQrel('1185869', '0', 1, '0'),
                9: TrecQrel('186154', '1160', 1, '0'),
                532760: TrecQrel('405466', '8841735', 1, '0'),
            })
            self._test_qrels(f'{ds}/dev', count=59273, items={
                0: TrecQrel('1102432', '2026790', 1, '0'),
                9: TrecQrel('300674', '7067032', 1, '0'),
                59272: TrecQrel('371455', '8009476', 1, '0'),
            })
            self._test_qrels(f'{ds}/dev/small', count=7437, items={
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

        self._test_scoreddocs('mmarco/v2/ar/dev/small', count=6848687, items={
            0: GenericScoredDoc('2', '1001873', -1.0),
            9: GenericScoredDoc('2', '2022779', -10.0),
            6848686: GenericScoredDoc('1102400', '8183767', -1000.0),
        })
        self._test_scoreddocs('mmarco/v2/zh/dev/small', count=6979520, items={
            0: GenericScoredDoc('2', '6285817', -1.0),
            9: GenericScoredDoc('2', '1782337', -10.0),
            6979519: GenericScoredDoc('1102400', '8662465', -1000.0),
        })
        self._test_scoreddocs('mmarco/v2/dt/dev/small', count=6608183, items={
            0: GenericScoredDoc('2', '2022779', -1.0),
            9: GenericScoredDoc('2', '6285817', -10.0),
            6608182: GenericScoredDoc('1102400', '424880', -1000.0),
        })
        self._test_scoreddocs('mmarco/v2/fr/dev/small', count=6831783, items={
            0: GenericScoredDoc('2', '1782337', -1.0),
            9: GenericScoredDoc('2', '6285819', -10.0),
            6831782: GenericScoredDoc('1102400', '3586980', -1000.0),
        })
        self._test_scoreddocs('mmarco/v2/de/dev/small', count=6586918, items={
            0: GenericScoredDoc('2', '2022779', -1.0),
            9: GenericScoredDoc('2', '5414414', -10.0),
            6586917: GenericScoredDoc('1102400', '2607361', -1000.0),
        })
        self._test_scoreddocs('mmarco/v2/hi/dev/small', count=6961912, items={
            0: GenericScoredDoc('2', '1782337', -1.0),
            9: GenericScoredDoc('2', '5762719', -10.0),
            6961911: GenericScoredDoc('1102400', '5426691', -1000.0),
        })
        self._test_scoreddocs('mmarco/v2/id/dev/small', count=6791487, items={
            0: GenericScoredDoc('2', '1782337', -1.0),
            9: GenericScoredDoc('2', '2022782', -10.0),
            6791486: GenericScoredDoc('1102400', '2744256', -1000.0),
        })
        self._test_scoreddocs('mmarco/v2/it/dev/small', count=6952771, items={
            0: GenericScoredDoc('2', '1782337', -1.0),
            9: GenericScoredDoc('2', '2022782', -10.0),
            6952770: GenericScoredDoc('1102400', '7352536', -1000.0),
        })
        self._test_scoreddocs('mmarco/v2/ja/dev/small', count=6817446, items={
            0: GenericScoredDoc('2', '3214931', -1.0),
            9: GenericScoredDoc('2', '3634076', -10.0),
            6817445: GenericScoredDoc('1102400', '1776192', -1000.0),
        })
        self._test_scoreddocs('mmarco/v2/pt/dev/small', count=6975268, items={
            0: GenericScoredDoc('2', '1782337', -1.0),
            9: GenericScoredDoc('2', '6285819', -10.0),
            6975267: GenericScoredDoc('1102400', '3981631', -1000.0),
        })
        self._test_scoreddocs('mmarco/v2/ru/dev/small', count=6931773, items={
            0: GenericScoredDoc('2', '1782337', -1.0),
            9: GenericScoredDoc('2', '112127', -10.0),
            6931772: GenericScoredDoc('1102400', '6819581', -1000.0),
        })
        self._test_scoreddocs('mmarco/v2/es/dev/small', count=6777044, items={
            0: GenericScoredDoc('2', '3214931', -1.0),
            9: GenericScoredDoc('2', '4339072', -10.0),
            6777043: GenericScoredDoc('1102400', '2729917', -1000.0),
        })
        self._test_scoreddocs('mmarco/v2/vi/dev/small', count=6976219, items={
            0: GenericScoredDoc('2', '2022779', -1.0),
            9: GenericScoredDoc('2', '3634076', -10.0),
            6976218: GenericScoredDoc('1102400', '7485193', -1000.0),
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

        for ds in ['mmarco/v2/ar/train', 'mmarco/v2/zh/train', 'mmarco/v2/dt/train', 'mmarco/v2/fr/train', 'mmarco/v2/de/train', 'mmarco/v2/hi/train', 'mmarco/v2/id/train', 'mmarco/v2/it/train', 'mmarco/v2/ja/train', 'mmarco/v2/pt/train', 'mmarco/v2/ru/train', 'mmarco/v2/es/train', 'mmarco/v2/vi/train']:
            self._test_docpairs(ds, count=39780811, items={
                0: GenericDocPair('400296', '1540783', '3518497'),
                9: GenericDocPair('189845', '1051356', '4238671'),
                39780810: GenericDocPair('749547', '394235', '7655192'),
            })


if __name__ == '__main__':
    unittest.main()
