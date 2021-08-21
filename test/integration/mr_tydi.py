import re
import unittest
import ir_datasets
from ir_datasets.formats import GenericQuery, GenericDoc, TrecQrel
from .base import DatasetIntegrationTest


class TestMrTydi(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('mr-tydi/ar', count=2106586, items={
            0: GenericDoc('7#0', re.compile('^الماء مادةٌ شفافةٌ عديمة اللون والرائحة، وهو المكوّن الأساسي للجداول والبحيرات والبحار والمحيطات وكذ.{231} يكون الماء سائلاً، ولكنّ حالاته الأخرى شائعة الوجود أيضاً؛ وهي حالة الجليد الصلبة والبخار الغازيّة\\.$', flags=48)),
            9: GenericDoc('7#9', re.compile('^يصنّف الماء كيميائيّاً على أنّه أكسيد للهيدروجين، وهو يتشكّل عندما يحترق الهيدروجين أو أيّ مركّب حاو.{1410} كما يمكن أن يكون على الشكل ماء فائق الثقل عندما يحلّ التريتيوم مكان الهيدروجين في جزيء الماء \\(T2O\\)\\.$', flags=48)),
            2106585: GenericDoc('5272574#0', re.compile('^الشاهيه او الشوهيه هو مرادف للكافيه وهو مكان لتقديم الشاي بدلاً من القهوة \\. الشاهيه غير منتشرة عالمي.{208}ث العرش و والي الممالك السبعة ايمن بن فهد اول من وضع لبنة لهذا النوع من المحلات و من مهد الطريق لهم\\.$', flags=48)),
        })
        self._test_docs('mr-tydi/bn', count=304059, items={
            0: GenericDoc('608#0', re.compile('^বাংলা ভাষা \\(/bɑːŋlɑː/; pronunciation\\) দক্ষিণ এশিয়ার বঙ্গ অঞ্চলের মানুষের স্থানীয় ভাষা, এই অঞ্চলটি .{444}, এবং ভারতের জাতীয় স্তোত্র এই ভাষাতেই রচিত এবং তা থেকেই দক্ষিণ এশিয়ায় এই ভাষার গুরুত্ব বোঝা যায়।$', flags=48)),
            9: GenericDoc('608#9', re.compile('^১৯৫১–৫২ সালে পূর্ব পাকিস্তানে বাঙালি জনগণের প্রবল ভাষা সচেতনতার ফলস্বরূপ বাংলা ভাষা আন্দোলন নামক একট.{264}। ১৯৯৯ খ্রিস্টাব্দের ১৭ই নভেম্বর ইউনেস্কো এই দিনটিকে আন্তর্জাতিক মাতৃভাষা দিবসের মর্যাদা প্রদান করে।$', flags=48)),
            304058: GenericDoc('719190#0', re.compile('^কোনরাড এলস্ট \\(জন্ম 7 অগাস্ট 1959\\) একজন বেলজীয় প্রাচ্যবিদ এবং ভারতবিদ যিনি তুলনামূলক ধর্মতত্ত্ব, হিন.{454}ভূত। এলস্ট হিন্দু জাতীয়তাবাদের বিষয়ে ডক্টরেট করেছেন, এবং হিন্দু জাতীয়তাবাদ আন্দোলনের সমর্থন করেন।$', flags=48)),
        })
        self._test_docs('mr-tydi/en', count=32907100, items={
            0: GenericDoc('12#0', re.compile('^Anarchism is a political philosophy that advocates self\\-governed societies based on voluntary, coope.{285}lds capitalism, the state, and representative democracy to be undesirable, unnecessary, and harmful\\.$', flags=48)),
            9: GenericDoc('12#9', re.compile('^The French Pierre\\-Joseph Proudhon is regarded as the first self\\-proclaimed anarchist, a label he ado.{1030}ractices inspired subsequent anarchists and made him one of the leading social thinkers of his time\\.$', flags=48)),
            32907099: GenericDoc('59828278#1', 'Casalegno died on 23 January 2019 at the age of 93.'),
        })
        self._test_docs('mr-tydi/fi', count=1908757, items={
            0: GenericDoc('1#0', re.compile('^Amsterdam on Alankomaiden pääkaupunki\\. Amsterdam on väkiluvultaan Alankomaiden suurin kaupunki, huht.{399} pääkaupunki, sijaitsevat niin kuningashuone, hallitus, parlamentti kuin korkein oikeuskin Haagissa\\.$', flags=48)),
            9: GenericDoc('1#9', re.compile('^Amsterdamia johtaa muiden Alankomaiden kuntien tapaan valtuusto\\. Amsterdamin kaupunginvaltuustoon va.{281}ginvaltuuston että raatimieskollegion puheenjohtaja, mutta hänellä ei ole äänioikeutta valtuustossa\\.$', flags=48)),
            1908756: GenericDoc('1494441#5', 'Vuoden valmentajaksi valittiin Päivi Alafrantin valmentaja Eino Maksimainen.'),
        })
        self._test_docs('mr-tydi/id', count=1469399, items={
            0: GenericDoc('1#0', re.compile('^Asam deoksiribonukleat, lebih dikenal dengan singkatan DNA \\(bahasa Inggris: d</b>eoxyribo<b data\\-par.{1133}n G\\), ikatan hidrogen mengikat basa\\-basa dari kedua unting polinukleotida membentuk DNA unting ganda$', flags=48)),
            9: GenericDoc('1#9', re.compile('^Nukleobasa diklasifikasikan ke dalam dua jenis: purina \\(A dan G\\) yang berupa fusi senyawa heteroling.{378} yang telah disintesis untuk mengkaji sifat\\-sifat asam nukleat dan digunakan dalam bioteknologi\\.\\[15\\]$', flags=48)),
            1469398: GenericDoc('2733009#0', re.compile('^Poggio a Caiano merupakan sebuah munisipalitas di provinsi Prato, Toskana \\(Italia\\)\\. Di wilayah kotam.{10}erdapat salah satu vila Medici yang disebut Villa di Poggio a Caiano yang mendominasi kota tersebut\\.$', flags=48)),
        })
        self._test_docs('mr-tydi/ja', count=7000027, items={
            0: GenericDoc('5#0', 'アンパサンド (&、英語名：) とは並立助詞「…と…」を意味する記号である。ラテン語の の合字で、Trebuchet MSフォントでは、と表示され "et" の合字であることが容易にわかる。ampersa、すなわち "and per se and"、その意味は"and [the symbol which] by itself [is] and"である。'),
            9: GenericDoc('10#0', 'この記事では言語（げんご）、特に自然言語について述べる。'),
            7000026: GenericDoc('3899088#4', '2017年 音楽プロデューサーとして始動。'),
        })
        self._test_docs('mr-tydi/ko', count=1496126, items={
            0: GenericDoc('5#0', '제임스 얼 "지미" 카터 주니어(, 1924년 10월 1일 ~ )는 민주당 출신 미국 39번째 대통령 (1977년 ~ 1981년)이다.'),
            9: GenericDoc('5#9', '퇴임 이후 민간 자원을 적극 활용한 비영리 기구인 카터 재단을 설립한 뒤 민주주의 실현을 위해 제 3세계의 선거 감시 활동 및 기니 벌레에 의한 드라쿤쿠르스 질병 방재를 위해 힘썼다. 미국의 빈곤층 지원 활동, 사랑의 집짓기 운동, 국제 분쟁 중재 등의 활동도 했다.'),
            1496125: GenericDoc('2431764#0', '갤러거의 다른 뜻은 다음과 같다.'),
        })
        self._test_docs('mr-tydi/ru', count=9597504, items={
            0: GenericDoc('7#0', 'Литва́ (), официальное название\xa0— Лито́вская Респу́блика ()\xa0— государство, расположенное в северо-восточной части Европы. Столица страны\xa0— Вильнюс.'),
            9: GenericDoc('7#9', re.compile('^Традиционно считается, что этническая основа Литвы сформирована носителями археологочической культур.{94}ной Литвы и Северо\\-Западной Белоруссии\\. Около VII века н\\.\xa0э\\. литовский язык отделился от латышского\\.$', flags=48)),
            9597503: GenericDoc('7761282#4', 'Скончался Роберт Грин 5 октября 1946 года.'),
        })
        self._test_docs('mr-tydi/sw', count=136689, items={
            0: GenericDoc('2#0', re.compile('^Akiolojia \\(kutoka Kiyunani αρχαίος = "zamani" na λόγος = "neno, usemi"\\) ni somo linalohusu mabaki ya.{95}wa kuchimba ardhi na kutafuta mabaki ya majengo, makaburi, silaha, vifaa, vyombo na mifupa ya watu\\. $', flags=48)),
            9: GenericDoc('10#2', '2) Kwa kufuata tabia za lugha nyingine "daktari" hutumiwa pia kama jina la heshima kwa mtu aliyepata shahada ya uzamivu au "PhD" ambayo ni shahada ya juu kabisa.'),
            136688: GenericDoc('108712#3', 'Instagram: @nafanikiwa_inspiration'),
        })
        self._test_docs('mr-tydi/te', count=548224, items={
            0: GenericDoc('786#0', re.compile('^గుంటూరు జిల్లా \\[1\\] 11,391 చ\\.కి\\.మీ\\. ల విస్తీర్ణములో వ్యాపించి, 48,89,230 \\(2011 గణన\\) జనాభా కలిగిఉన్నది.{55}ాన మహబూబ్ నగర్ జిల్లా, మరియు వాయువ్యాన నల్గొండ జిల్లా సరిహద్దులుగా ఉన్నాయి\\. దీని ముఖ్యపట్టణం గుంటూరు$', flags=48)),
            9: GenericDoc('786#9', 'నల్లమలై, వెంకటాయపాలెం శ్రేణులు మరియు కొండవీడు కొండలు'),
            548223: GenericDoc('273584#1', '2011 భారత జనగణన గణాంకాల ప్రకారం - మొత్తం 34,739 - పురుషులు 17,515 - స్త్రీలు 19,224'),
        })
        self._test_docs('mr-tydi/th', count=568855, items={
            0: GenericDoc('1#0', 'วิกิพีเดียดำเนินการโดยมูลนิธิวิกิมีเดีย องค์กรไม่แสวงผลกำไร ผู้ดำเนินการอีกหลาย ได้แก่'),
            9: GenericDoc('545#8', re.compile('^มีการค้นพบทางดาราศาสตร์ที่สำคัญไม่มากนักก่อนการประดิษฐ์กล้องโทรทรรศน์ ตัวอย่างการค้นพบเช่น ชาวจีนสาม.{151}วงสองร้อยปีก่อนคริสตกาล ฮิปปาร์คัส นักดาราศาสตร์ชาวกรีก สามารถคำนวณขนาดและระยะห่างของดวงจันทร์ได้\\[7\\]$', flags=48)),
            568854: GenericDoc('1001591#3', "การต่อสู้ยังเป็นส่วนหนึ่งของหนังสือเล่มแรกของ Brezhnev's trilogy ซึ่งเป็นการที่ทำให้เกินจริงต่อบทบาทของเลโอนิด เบรจเนฟ ในช่วงมหาสงครามของผู้รักชาติ"),
        })

    def test_queries(self):
        self._test_queries('mr-tydi/ar', count=16595, items={
            0: GenericQuery('0', 'هل عدم القيام بجهد جسماني ممكن ان يسبب الأرق؟'),
            9: GenericQuery('9', 'من هي المغنية العربية التي قامت بأداء شارة مسلسل الرسوم المتحركة (حكايات عالمية) لأول مرة؟'),
            16594: GenericQuery('16594', 'ماهى أول مؤلفة لي ج. ك. رولينغ ؟'),
        })
        self._test_queries('mr-tydi/ar/train', count=12377, items={
            0: GenericQuery('1', 'ما هي المسألة الشرقية ؟'),
            9: GenericQuery('11', 'متى تولى البابا فرنسيس المنصب؟'),
            12376: GenericQuery('15512', 'هل مرض أكل اللحم مرض معدي؟'),
        })
        self._test_queries('mr-tydi/ar/dev', count=3115, items={
            0: GenericQuery('0', 'هل عدم القيام بجهد جسماني ممكن ان يسبب الأرق؟'),
            9: GenericQuery('57', 'هل تعد النعامة أكبر الطيورحجماً في العالم؟'),
            3114: GenericQuery('15511', 'من ابتكر المثلجات؟'),
        })
        self._test_queries('mr-tydi/ar/test', count=1081, items={
            0: GenericQuery('15513', 'كم عدد مرات فوز الأوروغواي ببطولة كاس العالم لكرو القدم؟'),
            9: GenericQuery('15522', 'ما كان إسم حصان الرسول صلي الله عليه وسلم ؟'),
            1080: GenericQuery('16594', 'ماهى أول مؤلفة لي ج. ك. رولينغ ؟'),
        })
        self._test_queries('mr-tydi/bn', count=2264, items={
            0: GenericQuery('0', 'চেঙ্গিস খান কোন বংশের রাজা ছিলেন ?'),
            9: GenericQuery('9', 'অর্থনীতির জনক কাকে বলা হয় ?'),
            2263: GenericQuery('2263', 'চট্টগ্রাম কি কখনো ধর্মপালের অধীনে ছিল ?'),
        })
        self._test_queries('mr-tydi/bn/train', count=1713, items={
            0: GenericQuery('0', 'চেঙ্গিস খান কোন বংশের রাজা ছিলেন ?'),
            9: GenericQuery('11', 'দ্য লাঞ্চবক্স চলচ্চিত্রটির নির্মাতা কে ?'),
            1712: GenericQuery('2151', 'জামাতি ইসলাম দলটির প্রতিষ্ঠাতা কে ?'),
        })
        self._test_queries('mr-tydi/bn/dev', count=440, items={
            0: GenericQuery('4', 'ইংরেজ আন্তর্জাতিক ক্রিকেট তারকা জর্জ গিবসন ম্যাকাউলি কি একজন ডানহাতি ব্যাটসম্যান ছিলেন ?'),
            9: GenericQuery('63', 'বাংলাদেশের অষ্টম রাষ্ট্রপতি জিয়াউর রহমান কবে জন্মগ্রহণ করেন ?'),
            439: GenericQuery('2152', 'বাংলা ব্যাকরণ মতে বিশেষণ কয় প্রকার ?'),
        })
        self._test_queries('mr-tydi/bn/test', count=111, items={
            0: GenericQuery('2153', 'পশ্চিম ভারতের মহারাষ্ট্র রাজ্যের মুম্বাই শহরে নির্মিত গেটওয়ে অব ইন্ডিয়া স্থাপত্যটির ভিত্তিপ্রস্তর স্থাপন করেন কে ?'),
            9: GenericQuery('2162', 'মাওলানা ভাসানী বিজ্ঞান ও প্রযুক্তি বিশ্ববিদ্যালয়টির মোট আয়তন কত ?'),
            110: GenericQuery('2263', 'চট্টগ্রাম কি কখনো ধর্মপালের অধীনে ছিল ?'),
        })
        self._test_queries('mr-tydi/en', count=5194, items={
            0: GenericQuery('0', 'Is Creole a pidgin of French?'),
            9: GenericQuery('9', 'Who invented Hangul?'),
            5193: GenericQuery('5193', 'When did the Bundaberg Central State School become a heritage-listed site?'),
        })
        self._test_queries('mr-tydi/en/train', count=3547, items={
            0: GenericQuery('1', 'When was quantum field theory developed?'),
            9: GenericQuery('10', 'What do Grasshoppers eat?'),
            3546: GenericQuery('4446', 'What is the population of Mahwah, NJ?'),
        })
        self._test_queries('mr-tydi/en/dev', count=878, items={
            0: GenericQuery('0', 'Is Creole a pidgin of French?'),
            9: GenericQuery('44', 'What is the main version of Islam practiced in Iraq?'),
            877: GenericQuery('4443', 'Who was costume designer for the first Star Wars movie?'),
        })
        self._test_queries('mr-tydi/en/test', count=744, items={
            0: GenericQuery('4447', 'Do zebra finches have stripes?'),
            9: GenericQuery('4456', 'When was Quezon City founded?'),
            743: GenericQuery('5193', 'When did the Bundaberg Central State School become a heritage-listed site?'),
        })
        self._test_queries('mr-tydi/fi', count=9572, items={
            0: GenericQuery('0', 'Milloin Charles Fort syntyi?'),
            9: GenericQuery('9', 'Mitä tarkoittaa puritaani?'),
            9571: GenericQuery('9571', 'Milloin käytiin Persianlahden sota?'),
        })
        self._test_queries('mr-tydi/fi/train', count=6561, items={
            0: GenericQuery('0', 'Milloin Charles Fort syntyi?'),
            9: GenericQuery('10', 'Montako kuuta Saturnuksella on?'),
            6560: GenericQuery('8317', 'Milloin Lewis Hamilton ajoi ensimmäisen F1 osakilpailunsa?'),
        })
        self._test_queries('mr-tydi/fi/dev', count=1738, items={
            0: GenericQuery('1', 'Mitä on altruismi?'),
            9: GenericQuery('52', 'Mistä nimitys markka tulee?'),
            1737: GenericQuery('8308', 'Kuinka leveä Iso-Kukkanen on?'),
        })
        self._test_queries('mr-tydi/fi/test', count=1254, items={
            0: GenericQuery('8318', 'Milloin Kokemäki on perustettu?'),
            9: GenericQuery('8327', 'Minä vuonna Charles Darwin julkaisi kuuluisan teoriansa lajien synnystä?'),
            1253: GenericQuery('9571', 'Milloin käytiin Persianlahden sota?'),
        })
        self._test_queries('mr-tydi/id', count=6977, items={
            0: GenericQuery('0', 'dimanakah  Dr. Ernest François Eugène Douwes Dekker meninggal?'),
            9: GenericQuery('9', 'Siapa yang mengembangkan video game Pokémon pertama kali ?'),
            6976: GenericQuery('6976', 'Berapakah berat Ikan pari manta yag terbesar?'),
        })
        self._test_queries('mr-tydi/id/train', count=4902, items={
            0: GenericQuery('0', 'dimanakah  Dr. Ernest François Eugène Douwes Dekker meninggal?'),
            9: GenericQuery('12', 'Apa kepercyaan dalam karya Mahabharata?'),
            4901: GenericQuery('6147', 'Kapan Kaisar Tang Gaozu mulai menjabat ?'),
        })
        self._test_queries('mr-tydi/id/dev', count=1224, items={
            0: GenericQuery('3', 'Dimana James Hepburn meninggal?'),
            9: GenericQuery('41', 'Kapan perdagangan melalui armada mulai dilakukan oleh bangsa Eropa ?'),
            1223: GenericQuery('6138', 'Kapan sepeda motor ditemukan ?'),
        })
        self._test_queries('mr-tydi/id/test', count=829, items={
            0: GenericQuery('6148', 'Siapakah yang menemuka benua Amerika ?'),
            9: GenericQuery('6157', 'Dimana kantor pusat General Motors?'),
            828: GenericQuery('6976', 'Berapakah berat Ikan pari manta yag terbesar?'),
        })
        self._test_queries('mr-tydi/ja', count=5353, items={
            0: GenericQuery('0', '“ダン” ダニエル・ジャドソン・キャラハンの出身はどこ'),
            9: GenericQuery('9', '発見された中で最大のクロトガリザメの大きさは？'),
            5352: GenericQuery('5352', 'クイズ番組『アメリカ横断ウルトラクイズ』の出場者はどのようなひとでしたか？'),
        })
        self._test_queries('mr-tydi/ja/train', count=3697, items={
            0: GenericQuery('1', 'サー・マイケル・フィリップ・ジャガーの出身は？'),
            9: GenericQuery('18', 'ニューヨーク市の面積は？'),
            3696: GenericQuery('4630', 'ブラームスの出身はどこ？'),
        })
        self._test_queries('mr-tydi/ja/dev', count=928, items={
            0: GenericQuery('0', '“ダン” ダニエル・ジャドソン・キャラハンの出身はどこ'),
            9: GenericQuery('22', 'スポーツにおけるドーピング検査が始まったのはいつ'),
            927: GenericQuery('4629', '彭 徳懐はいつ生まれた？'),
        })
        self._test_queries('mr-tydi/ja/test', count=720, items={
            0: GenericQuery('4631', '化学兵器禁止条約はどこで採択された？'),
            9: GenericQuery('4640', 'ダニエル・レイ・エインジはどこのプロ野球チームに所属した？'),
            719: GenericQuery('5352', 'クイズ番組『アメリカ横断ウルトラクイズ』の出場者はどのようなひとでしたか？'),
        })
        self._test_queries('mr-tydi/ko', count=2019, items={
            0: GenericQuery('0', '2019년까지 월드컵은 몇개국에서 개최되었는가?'),
            9: GenericQuery('9', '최초의 원자력 발전소는 무엇인가?'),
            2018: GenericQuery('2018', '월드컵이 처음 개최된 나라는 어디인가?'),
        })
        self._test_queries('mr-tydi/ko/train', count=1295, items={
            0: GenericQuery('1', '로마의 면적은 서울시의 2배인가요?'),
            9: GenericQuery('12', '지구 대기권에서 지표면에 가장 인접한 대기의 층은 무엇인가요?'),
            1294: GenericQuery('1597', '오행이라는 것은 목·화·토·금·수의 오원소를 말하나요?'),
        })
        self._test_queries('mr-tydi/ko/dev', count=303, items={
            0: GenericQuery('0', '2019년까지 월드컵은 몇개국에서 개최되었는가?'),
            9: GenericQuery('40', '우크라이나에서 가장 큰 도시는 어디인가?'),
            302: GenericQuery('1593', '몽골은 언제 현행 체제 수립을 했나요?'),
        })
        self._test_queries('mr-tydi/ko/test', count=421, items={
            0: GenericQuery('1598', '한니발 바르카의 최종 계급은 무엇인가요?'),
            9: GenericQuery('1607', '상자 속 소년의 매장지는 어디인가요?'),
            420: GenericQuery('2018', '월드컵이 처음 개최된 나라는 어디인가?'),
        })
        self._test_queries('mr-tydi/ru', count=7763, items={
            0: GenericQuery('0', 'Когда начался Кари́бский кризис?'),
            9: GenericQuery('9', 'Можно излечиться от наркотической зависимости полностью?'),
            7762: GenericQuery('7762', 'Когда впервые начали применять компьютерную графику в кинематографе?'),
        })
        self._test_queries('mr-tydi/ru/train', count=5366, items={
            0: GenericQuery('1', 'Когда был спущен на воду первый миноносец «Спокойный»?'),
            9: GenericQuery('19', 'Какой самый распространенный элемент на Земле?'),
            5365: GenericQuery('6761', 'Какая площадь Аппенинского полуострова?'),
        })
        self._test_queries('mr-tydi/ru/dev', count=1375, items={
            0: GenericQuery('0', 'Когда начался Кари́бский кризис?'),
            9: GenericQuery('23', 'Когда в Китае начали есть палочками?'),
            1374: GenericQuery('6762', 'Кто убил Зорана Джинджича?'),
        })
        self._test_queries('mr-tydi/ru/test', count=995, items={
            0: GenericQuery('6763', 'Когда появился термин правово́е госуда́рство?'),
            9: GenericQuery('6772', 'Какая максимальная скорость ЭД9?'),
            994: GenericQuery('7762', 'Когда впервые начали применять компьютерную графику в кинематографе?'),
        })
        self._test_queries('mr-tydi/sw', count=3271, items={
            0: GenericQuery('0', 'Je,nchi gani yenye kuzalisha chungwa kwa wingi zaidi duniani?'),
            9: GenericQuery('9', 'Je mto mkubwa sana Afrika ni gani?'),
            3270: GenericQuery('3270', 'Michael Wamalwa Kijana alifariki akiwa na miaka mingapi?'),
        })
        self._test_queries('mr-tydi/sw/train', count=2072, items={
            0: GenericQuery('0', 'Je,nchi gani yenye kuzalisha chungwa kwa wingi zaidi duniani?'),
            9: GenericQuery('11', 'Je,Juan Carlos Tedesco alisomea katika chuo kikuu gani?'),
            2071: GenericQuery('2600', 'Nini maana ya data?'),
        })
        self._test_queries('mr-tydi/sw/dev', count=526, items={
            0: GenericQuery('6', 'Bandari kubwa nchini Kenya iko wapi?'),
            9: GenericQuery('35', 'Je,makao makuu ya kaunti ya Samburu ni wapi?'),
            525: GenericQuery('2597', 'Paul Schulze alizaliwa mwaka upi?'),
        })
        self._test_queries('mr-tydi/sw/test', count=670, items={
            0: GenericQuery('2601', 'Je,Sarah Wayne Callies ana mume?'),
            9: GenericQuery('2610', 'Lucas Vázquez Iglesias ana miaka mingapi?'),
            669: GenericQuery('3270', 'Michael Wamalwa Kijana alifariki akiwa na miaka mingapi?'),
        })
        self._test_queries('mr-tydi/te', count=5517, items={
            0: GenericQuery('0', 'వేప చెట్టు యొక్క శాస్త్రీయ నామం ఏమిటి?'),
            9: GenericQuery('9', 'నందమూరి తారక రామారావు నటించిన మొదటి చిత్రం ఏది?'),
            5516: GenericQuery('5516', 'తాడేపల్లి రాఘవ నారాయణ శాస్త్రి ఎక్కడ జన్మించాడు?'),
        })
        self._test_queries('mr-tydi/te/train', count=3880, items={
            0: GenericQuery('0', 'వేప చెట్టు యొక్క శాస్త్రీయ నామం ఏమిటి?'),
            9: GenericQuery('10', 'జింకును మొదటగా ఎప్పుడు కనుగొన్నారు?'),
            3879: GenericQuery('4868', 'ఆది చిత్ర కథానాయకుడు ఎవరు?'),
        })
        self._test_queries('mr-tydi/te/dev', count=983, items={
            0: GenericQuery('2', 'ఆర్మేనియా దేశంలో మొత్తం జిల్లాలు ఉన్నాయి?'),
            9: GenericQuery('41', '2011లో గొడ్లవీడు గ్రామంలో ఎంత మంది పురుషులు ఉన్నారు?'),
            982: GenericQuery('4869', 'తమన్నా భాటియా నటించిన మొదటి తెలుగు సినిమా ఏది?'),
        })
        self._test_queries('mr-tydi/te/test', count=646, items={
            0: GenericQuery('4870', 'మహా సముద్రాలు ఎన్ని ఉన్నాయి?'),
            9: GenericQuery('4879', 'నండవ గ్రామ విస్తీర్ణత ఎంత?'),
            645: GenericQuery('5516', 'తాడేపల్లి రాఘవ నారాయణ శాస్త్రి ఎక్కడ జన్మించాడు?'),
        })
        self._test_queries('mr-tydi/th', count=5322, items={
            0: GenericQuery('0', 'มหาวิทยาลัยมหาสารคาม เปิดสอนกี่สาขาวิชา?'),
            9: GenericQuery('9', 'ดิ อะเมซิ่ง เรซ เป็นเรียลลิตี้โชว์จากประเทศอะไร?'),
            5321: GenericQuery('5321', 'หม่อมราชวงศ์สุขุมพันธุ์ บริพัตร เกิดเมื่อไหร่?'),
        })
        self._test_queries('mr-tydi/th/train', count=3319, items={
            0: GenericQuery('0', 'มหาวิทยาลัยมหาสารคาม เปิดสอนกี่สาขาวิชา?'),
            9: GenericQuery('14', 'ขนมปังขิงมีส่วนผสมของขิงหรือไม่?'),
            3318: GenericQuery('4130', 'ใครเป็นผู้ก่อตั้ง โรงเรียนสาธิตแห่งมหาวิทยาลัยเกษตรศาสตร์?'),
        })
        self._test_queries('mr-tydi/th/dev', count=807, items={
            0: GenericQuery('3', 'ซาเอบะ เรียวจากเรื่องซิตี้ฮันเตอร์มีความสูงเท่าไหร่?'),
            9: GenericQuery('42', 'ราชวงศ์หมิงมีประมุของค์แรกคือใคร ?'),
            806: GenericQuery('4113', 'กรุงเทพมหานครมีกี่เขต?'),
        })
        self._test_queries('mr-tydi/th/test', count=1190, items={
            0: GenericQuery('4131', 'อยุธยามีกี่อำเภอ ?'),
            9: GenericQuery('4140', 'เชฟกระทะเหล็ก ประเทศไทย ออกอากาศครั้งแรกเมื่อใด ?'),
            1189: GenericQuery('5321', 'หม่อมราชวงศ์สุขุมพันธุ์ บริพัตร เกิดเมื่อไหร่?'),
        })

    def test_qrels(self):
        self._test_qrels('mr-tydi/ar', count=16749, items={
            0: TrecQrel('0', '151236#12', 1, 'Q0'),
            9: TrecQrel('9', '401329#2', 1, 'Q0'),
            16748: TrecQrel('16594', '6108#5', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ar/train', count=12377, items={
            0: TrecQrel('1', '26569#0', 1, 'Q0'),
            9: TrecQrel('11', '1596701#0', 1, 'Q0'),
            12376: TrecQrel('15512', '2473000#5', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ar/dev', count=3115, items={
            0: TrecQrel('0', '151236#12', 1, 'Q0'),
            9: TrecQrel('57', '1547903#8', 1, 'Q0'),
            3114: TrecQrel('15511', '166973#1', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ar/test', count=1257, items={
            0: TrecQrel('15513', '1266754#1', 1, 'Q0'),
            9: TrecQrel('15520', '104530#0', 1, 'Q0'),
            1256: TrecQrel('16594', '6108#5', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/bn', count=2292, items={
            0: TrecQrel('0', '3287#0', 1, 'Q0'),
            9: TrecQrel('9', '1218#4', 1, 'Q0'),
            2291: TrecQrel('2263', '42588#1', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/bn/train', count=1719, items={
            0: TrecQrel('0', '3287#0', 1, 'Q0'),
            9: TrecQrel('11', '551087#0', 1, 'Q0'),
            1718: TrecQrel('2151', '335652#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/bn/dev', count=443, items={
            0: TrecQrel('4', '717942#0', 1, 'Q0'),
            9: TrecQrel('63', '5531#0', 1, 'Q0'),
            442: TrecQrel('2152', '342398#2', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/bn/test', count=130, items={
            0: TrecQrel('2153', '469063#1', 1, 'Q0'),
            9: TrecQrel('2160', '5695#2', 1, 'Q0'),
            129: TrecQrel('2263', '42588#1', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/en', count=5360, items={
            0: TrecQrel('0', '3716905#1', 1, 'Q0'),
            9: TrecQrel('9', '17470560#2', 1, 'Q0'),
            5359: TrecQrel('5193', '56062743#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/en/train', count=3547, items={
            0: TrecQrel('1', '25267#12', 1, 'Q0'),
            9: TrecQrel('10', '1021764#2', 1, 'Q0'),
            3546: TrecQrel('4446', '124989#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/en/dev', count=878, items={
            0: TrecQrel('0', '3716905#1', 1, 'Q0'),
            9: TrecQrel('44', '5054631#1', 1, 'Q0'),
            877: TrecQrel('4443', '15218018#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/en/test', count=935, items={
            0: TrecQrel('4447', '424730#5', 1, 'Q0'),
            9: TrecQrel('4453', '42869687#42', 1, 'Q0'),
            934: TrecQrel('5193', '56062743#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/fi', count=9750, items={
            0: TrecQrel('0', '254561#0', 1, 'Q0'),
            9: TrecQrel('9', '37218#0', 1, 'Q0'),
            9749: TrecQrel('9571', '5511#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/fi/train', count=6561, items={
            0: TrecQrel('0', '254561#0', 1, 'Q0'),
            9: TrecQrel('10', '357229#0', 1, 'Q0'),
            6560: TrecQrel('8317', '259582#10', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/fi/dev', count=1738, items={
            0: TrecQrel('1', '18044#0', 1, 'Q0'),
            9: TrecQrel('52', '65183#1', 1, 'Q0'),
            1737: TrecQrel('8308', '1490320#1', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/fi/test', count=1451, items={
            0: TrecQrel('8318', '8625#16', 1, 'Q0'),
            9: TrecQrel('8326', '124577#0', 1, 'Q0'),
            1450: TrecQrel('9571', '5511#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/id', count=7087, items={
            0: TrecQrel('0', '7080#33', 1, 'Q0'),
            9: TrecQrel('9', '462208#0', 1, 'Q0'),
            7086: TrecQrel('6976', '413950#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/id/train', count=4902, items={
            0: TrecQrel('0', '7080#33', 1, 'Q0'),
            9: TrecQrel('12', '817#2', 1, 'Q0'),
            4901: TrecQrel('6147', '224992#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/id/dev', count=1224, items={
            0: TrecQrel('3', '2386357#15', 1, 'Q0'),
            9: TrecQrel('41', '13153#3', 1, 'Q0'),
            1223: TrecQrel('6138', '18525#1', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/id/test', count=961, items={
            0: TrecQrel('6148', '6874#1', 1, 'Q0'),
            9: TrecQrel('6157', '17046#0', 1, 'Q0'),
            960: TrecQrel('6976', '413950#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ja', count=5548, items={
            0: TrecQrel('0', '2681119#1', 1, 'Q0'),
            9: TrecQrel('9', '1214905#10', 1, 'Q0'),
            5547: TrecQrel('5352', '340021#6', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ja/train', count=3697, items={
            0: TrecQrel('1', '119071#3', 1, 'Q0'),
            9: TrecQrel('18', '507175#4', 1, 'Q0'),
            3696: TrecQrel('4630', '28677#2', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ja/dev', count=928, items={
            0: TrecQrel('0', '2681119#1', 1, 'Q0'),
            9: TrecQrel('22', '54592#4', 1, 'Q0'),
            927: TrecQrel('4629', '3255343#1', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ja/test', count=923, items={
            0: TrecQrel('4631', '88104#2', 1, 'Q0'),
            9: TrecQrel('4637', '3218420#1', 1, 'Q0'),
            922: TrecQrel('5352', '340021#6', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ko', count=2116, items={
            0: TrecQrel('0', '1471178#0', 1, 'Q0'),
            9: TrecQrel('9', '78656#4', 1, 'Q0'),
            2115: TrecQrel('2018', '7686#10', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ko/train', count=1317, items={
            0: TrecQrel('1', '3228#0', 1, 'Q0'),
            9: TrecQrel('12', '3727#2', 1, 'Q0'),
            1316: TrecQrel('1597', '108520#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ko/dev', count=307, items={
            0: TrecQrel('0', '1471178#0', 1, 'Q0'),
            9: TrecQrel('40', '44#0', 1, 'Q0'),
            306: TrecQrel('1593', '9279#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ko/test', count=492, items={
            0: TrecQrel('1598', '7207#0', 1, 'Q0'),
            9: TrecQrel('1607', '1418110#16', 1, 'Q0'),
            491: TrecQrel('2018', '7686#10', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ru', count=7909, items={
            0: TrecQrel('0', '105156#0', 1, 'Q0'),
            9: TrecQrel('10', '389080#7', 1, 'Q0'),
            7908: TrecQrel('7762', '2444778#2', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ru/train', count=5366, items={
            0: TrecQrel('1', '2183682#1', 1, 'Q0'),
            9: TrecQrel('19', '298318#2', 1, 'Q0'),
            5365: TrecQrel('6761', '89740#1', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ru/dev', count=1375, items={
            0: TrecQrel('0', '105156#0', 1, 'Q0'),
            9: TrecQrel('23', '10475#1', 1, 'Q0'),
            1374: TrecQrel('6762', '5244374#1', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/ru/test', count=1168, items={
            0: TrecQrel('6763', '104578#46', 1, 'Q0'),
            9: TrecQrel('6769', '55439#6', 1, 'Q0'),
            1167: TrecQrel('7762', '2444778#2', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/sw', count=3767, items={
            0: TrecQrel('0', '18032#12', 1, 'Q0'),
            9: TrecQrel('6', '1676#23', 1, 'Q0'),
            3766: TrecQrel('3270', '32664#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/sw/train', count=2401, items={
            0: TrecQrel('0', '18032#12', 1, 'Q0'),
            9: TrecQrel('9', '3036#0', 1, 'Q0'),
            2400: TrecQrel('2600', '33987#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/sw/dev', count=623, items={
            0: TrecQrel('6', '8408#2', 1, 'Q0'),
            9: TrecQrel('29', '8390#3', 1, 'Q0'),
            622: TrecQrel('2597', '22883#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/sw/test', count=743, items={
            0: TrecQrel('2601', '15607#5', 1, 'Q0'),
            9: TrecQrel('2609', '75425#0', 1, 'Q0'),
            742: TrecQrel('3270', '32664#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/te', count=5540, items={
            0: TrecQrel('0', '59628#1', 1, 'Q0'),
            9: TrecQrel('9', '2475#5', 1, 'Q0'),
            5539: TrecQrel('5516', '104096#1', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/te/train', count=3880, items={
            0: TrecQrel('0', '59628#1', 1, 'Q0'),
            9: TrecQrel('10', '64977#1', 1, 'Q0'),
            3879: TrecQrel('4868', '84762#1', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/te/dev', count=983, items={
            0: TrecQrel('2', '259006#0', 1, 'Q0'),
            9: TrecQrel('41', '26144#1', 1, 'Q0'),
            982: TrecQrel('4869', '132304#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/te/test', count=677, items={
            0: TrecQrel('4870', '40068#0', 1, 'Q0'),
            9: TrecQrel('4878', '248481#1', 1, 'Q0'),
            676: TrecQrel('5516', '104096#1', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/th', count=5545, items={
            0: TrecQrel('0', '12146#1', 1, 'Q0'),
            9: TrecQrel('9', '73009#1', 1, 'Q0'),
            5544: TrecQrel('5321', '46316#0', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/th/train', count=3360, items={
            0: TrecQrel('0', '12146#1', 1, 'Q0'),
            9: TrecQrel('14', '26348#2', 1, 'Q0'),
            3359: TrecQrel('4130', '32375#1', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/th/dev', count=817, items={
            0: TrecQrel('3', '122780#4', 1, 'Q0'),
            9: TrecQrel('42', '21447#2', 1, 'Q0'),
            816: TrecQrel('4113', '9672#1', 1, 'Q0'),
        })
        self._test_qrels('mr-tydi/th/test', count=1368, items={
            0: TrecQrel('4131', '4203#14', 1, 'Q0'),
            9: TrecQrel('4139', '614750#1', 1, 'Q0'),
            1367: TrecQrel('5321', '46316#0', 1, 'Q0'),
        })


if __name__ == '__main__':
    unittest.main()
