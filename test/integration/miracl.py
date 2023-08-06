import re
import unittest
import ir_datasets
from ir_datasets.datasets.miracl import MiraclDoc
from ir_datasets.formats import GenericQuery, TrecQrel
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestMiracl(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('miracl/ar', count=2061414, items={
            0: MiraclDoc('7#0', 'ماء', re.compile('^الماء مادةٌ شفافةٌ عديمة اللون والرائحة، وهو المكوّن الأساسي للجداول والبحيرات والبحار والمحيطات وكذ.{230} يكون الماء سائلاً، ولكنّ حالاته الأخرى شائعة الوجود أيضاً؛ وهي حالة الجليد الصلبة والبخار الغازيّة\\.$', flags=48)),
            9: MiraclDoc('7#9', 'ماء', 'كما يوجد الجليد على شكل صفائح جليديّة في الأرض وفي الفوّهات والصخور البركانيّة في القمر، وفي أقمار أخرى مثل قمر شارون.'),
            2061413: MiraclDoc('5272574#0', 'شاهيه', re.compile('^الشاهيه او الشوهيه هو مرادف للكافيه وهو مكان لتقديم الشاي بدلاً من القهوة \\. الشاهيه غير منتشرة عالمي.{208}ث العرش و والي الممالك السبعة ايمن بن فهد اول من وضع لبنة لهذا النوع من المحلات و من مهد الطريق لهم\\.$', flags=48)),
        })
        self._test_docs('miracl/bn', count=297265, items={
            0: MiraclDoc('608#0', 'বাংলা ভাষা', re.compile('^বাংলা ভাষা \\(; \\) দক্ষিণ এশিয়ার বঙ্গ অঞ্চলের মানুষের স্থানীয় ভাষা, এই অঞ্চলটি বর্তমানে রাজনৈতিকভাবে .{410}, এবং ভারতের জাতীয় স্তোত্র এই ভাষাতেই রচিত এবং তা থেকেই দক্ষিণ এশিয়ায় এই ভাষার গুরুত্ব বোঝা যায়।$', flags=48)),
            9: MiraclDoc('608#9', 'বাংলা ভাষা', re.compile('^বাংলাদেশ রাষ্ট্রের রাষ্ট্রভাষা ও সরকারি ভাষা হলো বাংলা। এছাড়াও ভারতীয় সংবিধান দ্বারা স্বীকৃত ২৩টি .{481} বাহিনীর ৫,৩০০ বাংলাদেশি সৈনিকের সেবার স্বীকৃতিস্বরূপ বাংলা ভাষাকে সরকারি ভাষার মর্যাদা প্রদান করেন।$', flags=48)),
            297264: MiraclDoc('719190#0', 'কোনরাড এলস্ট', re.compile('^কোনরাড এলস্ট \\(জন্ম 7 অগাস্ট 1959\\) একজন বেলজীয় প্রাচ্যবিদ এবং ভারতবিদ যিনি তুলনামূলক ধর্মতত্ত্ব, হিন.{454}ভূত। এলস্ট হিন্দু জাতীয়তাবাদের বিষয়ে ডক্টরেট করেছেন, এবং হিন্দু জাতীয়তাবাদ আন্দোলনের সমর্থন করেন।$', flags=48)),
        })
        self._test_docs('miracl/de', count=15866222, items={
            0: MiraclDoc('1#0', 'Alan Smithee', re.compile('^Alan Smithee steht als Pseudonym für einen fiktiven Regisseur, der Filme verantwortet, bei denen der.{93}on 1968 bis 2000 wurde es von der Directors Guild of America \\(DGA\\) für solche Situationen empfohlen\\.$', flags=48)),
            9: MiraclDoc('1#9', 'Alan Smithee', 'Zu den Drehbuchautoren, die das Pseudonym benutzt haben, gehören Sam Raimi und Ivan Raimi, die das Drehbuch zu "Die total beknackte Nuß" als "Alan Smithee, Jr." und "Alan Smithee, Sr." schrieben.'),
            15866221: MiraclDoc('12150081#3', 'Marcel Lotka', re.compile('^Nach seinem Vertragsende bei Hertha BSC kehrt Lotka zur Saison 2022/23 in das Ruhrgebiet zurück und .{7}t zur zweiten Mannschaft von Borussia Dortmund\\. Er unterschrieb einen Vertrag bis zum 30\\. Juni 2024\\.$', flags=48)),
        })
        self._test_docs('miracl/en', count=32893221, items={
            0: MiraclDoc('12#0', 'Anarchism', re.compile('^Anarchism is a political philosophy that advocates self\\-governed societies based on voluntary, coope.{285}lds capitalism, the state, and representative democracy to be undesirable, unnecessary, and harmful\\.$', flags=48)),
            9: MiraclDoc('12#9', 'Anarchism', re.compile('^The French Pierre\\-Joseph Proudhon is regarded as the first self\\-proclaimed anarchist, a label he ado.{1030}ractices inspired subsequent anarchists and made him one of the leading social thinkers of his time\\.$', flags=48)),
            32893220: MiraclDoc('59828278#1', 'Jacqueline Casalegno', 'Casalegno died on 23 January 2019 at the age of 93.'),
        })
        self._test_docs('miracl/es', count=10373953, items={
            0: MiraclDoc('7#0', 'Andorra', re.compile('^Andorra, oficialmente Principado de Andorra \\(\\), es un micro\\-Estado soberano sin litoral ubicado en e.{573}a —con los departamentos de Ariège y Pirineos Orientales \\(Occitania\\)—\\. Pertenece a la Europa latina\\.$', flags=48)),
            9: MiraclDoc('7#9', 'Andorra', re.compile('^Una hipótesis relaciona a la palabra Andorra con "andurrial", palabra cuyo origen sigue siendo incie.{91}rrales", o bien, según Francisco Martínez Marina, con el árabe "al darra"h, que significa "boscosa"\\.$', flags=48)),
            10373952: MiraclDoc('10172220#4', 'Diario de SSR', re.compile('^A día de hoy ella gana su propio dinero, y con tan solo 16 años colabora con algunas fundaciones con.{11}er y niños sin fronteras\\. También es apadrinadora de chimpancés del país natal de su madre, Senegal\\.$', flags=48)),
        })
        self._test_docs('miracl/fa', count=2207172, items={
            0: MiraclDoc('594#0', 'ویکی\u200cپدیا', re.compile('^ویکی\u200cپدیا \\(کوته\u200cنوشت به\u200cصورت «وپ» و «WP»\\) یک دانشنامه برخط چندزبانه مبتنی بر وب با محتوای آزاد و همک.{277}ست\\. هدف ویکی\u200cپدیا آفرینش و انتشار جهانی یک دانشنامه با محتوای آزاد به تمامی زبان\u200cهای زندهٔ دنیا است\\.$', flags=48)),
            9: MiraclDoc('594#9', 'ویکی\u200cپدیا', re.compile('^کاربران اسپانیایی در فوریه ۲۰۰۲ از ترس تبلیغات تجاری و نبود کنترل در یک ویکی\u200cپدیای انگلیسی\u200cمحور از ".{208}ص به جاهای دیگری نظیر ویکی\u200cاینفو انتقال داده شده\u200cاند\\. در ویکی\u200cاینفو خبری از «دیدگاه بی\u200cطرفانه» نیست\\.$', flags=48)),
            2207171: MiraclDoc('5934583#0', 'ادوکسودین', 'ادوکسودین () یک داروی ضد ویروسی است. این دارو یک آنالوگ نوکلئوزیدی از تیمیدین است، این دارو در برابر ویروس هرپس سیمپلکس اثربخشی نشان داده است.'),
        })
        self._test_docs('miracl/fi', count=1883509, items={
            0: MiraclDoc('1#0', 'Amsterdam', re.compile('^Amsterdam on Alankomaiden pääkaupunki\\. Amsterdam on väkiluvultaan Alankomaiden suurin kaupunki, huht.{399} pääkaupunki, sijaitsevat niin kuningashuone, hallitus, parlamentti kuin korkein oikeuskin Haagissa\\.$', flags=48)),
            9: MiraclDoc('1#9', 'Amsterdam', re.compile('^Amsterdamia johtaa muiden Alankomaiden kuntien tapaan valtuusto\\. Amsterdamin kaupunginvaltuustoon va.{281}ginvaltuuston että raatimieskollegion puheenjohtaja, mutta hänellä ei ole äänioikeutta valtuustossa\\.$', flags=48)),
            1883508: MiraclDoc('1494441#5', 'Suomen urheilu 1990', 'Vuoden valmentajaksi valittiin Päivi Alafrantin valmentaja Eino Maksimainen.'),
        })
        self._test_docs('miracl/fr', count=14636953, items={
            0: MiraclDoc('3#0', 'Antoine Meillet', 'Paul Jules Antoine Meillet, né le à Moulins (Allier) et mort le à Châteaumeillant (Cher), est le principal linguiste français des premières décennies du . Il est aussi philologue.'),
            9: MiraclDoc('3#9', 'Antoine Meillet', re.compile("^Selon le linguiste allemand Walter Porzig, Meillet est un « grand précurseur »\\. Il montre, par exemp.{26}s indo\\-européens, les groupes indo\\-européens sont le résultat historique d'une variation diatopique\\.$", flags=48)),
            14636952: MiraclDoc('14821800#8', 'Exploitation aurifère au Tchad', re.compile("^La dangerosité de la prospection est telle que les employeurs recourent à la torture pour forcer les.{107}rs recrutés vivent en outre une situation d'esclavage, sans paie, avec insuffisamment de nourriture\\.$", flags=48)),
        })
        self._test_docs('miracl/hi', count=506264, items={
            0: MiraclDoc('10#0', 'हम होंगे कामयाब', re.compile('^हम होंगे कामयाब \\( का गिरिजा कुमार माथुर द्वारा किया गया हिंदी भावानुवाद\\) एक प्रतिरोध गीत है। यह गीत .{144}ाता है, जो चार्ल्स अल्बर्ट टिंडले द्वारा गाया गया था और जिसे 1900 में पहली बार प्रकाशित किया गया था।$', flags=48)),
            9: MiraclDoc('14#7', 'दैनिक पूजा', 'अगर सम्भव हो तो गणेश के 108 नाम जपें :'),
            506263: MiraclDoc('1355429#0', 'चिर्रावूरु यज्ञेश्वर चिन्तामणि', re.compile('^चिर्रावूरु यज्ञेश्वर चिन्तामणि \\(10 अप्रैल 1880 – 1 जुलाई 1941\\) भारत के एक सम्पादक, पत्रकार, उदार राज.{16}। उनका जन्म युगादि \\(तेलुगु नववर्ष दिवस\\) को विजयनगरम् में हुआ था जो वर्तमान में आन्ध्र प्रदेश में है।$', flags=48)),
        })
        self._test_docs('miracl/id', count=1446315, items={
            0: MiraclDoc('1#0', 'Asam deoksiribonukleat', re.compile('^Asam deoksiribonukleat, lebih dikenal dengan singkatan DNA \\(bahasa Inggris: "deoxyribonucleic acid"\\).{1083}n G\\), ikatan hidrogen mengikat basa\\-basa dari kedua unting polinukleotida membentuk DNA unting ganda$', flags=48)),
            9: MiraclDoc('1#9', 'Asam deoksiribonukleat', re.compile('^Nukleobasa diklasifikasikan ke dalam dua jenis: purina \\(A dan G\\) yang berupa fusi senyawa heteroling.{371}atan yang telah disintesis untuk mengkaji sifat\\-sifat asam nukleat dan digunakan dalam bioteknologi\\.$', flags=48)),
            1446314: MiraclDoc('2733009#0', 'Poggio a Caiano', re.compile('^Poggio a Caiano merupakan sebuah munisipalitas di provinsi Prato, Toskana \\(Italia\\)\\. Di wilayah kotam.{10}erdapat salah satu vila Medici yang disebut Villa di Poggio a Caiano yang mendominasi kota tersebut\\.$', flags=48)),
        })
        self._test_docs('miracl/ja', count=6953614, items={
            0: MiraclDoc('5#0', 'アンパサンド', 'アンパサンド (&、英語名：) とは並立助詞「…と…」を意味する記号である。ラテン語の の合字で、Trebuchet MSフォントでは、と表示され "et" の合字であることが容易にわかる。ampersa、すなわち "and per se and"、その意味は"and [the symbol which] by itself [is] and"である。'),
            9: MiraclDoc('10#0', '言語', 'この記事では言語（げんご）、特に自然言語について述べる。'),
            6953613: MiraclDoc('3899088#4', 'DJ SAAT', '2017年 音楽プロデューサーとして始動。'),
        })
        self._test_docs('miracl/ko', count=1486752, items={
            0: MiraclDoc('5#0', '지미 카터', '제임스 얼 "지미" 카터 주니어(, 1924년 10월 1일 ~ )는 민주당 출신 미국 39번째 대통령 (1977년 ~ 1981년)이다.'),
            9: MiraclDoc('5#9', '지미 카터', '퇴임 이후 민간 자원을 적극 활용한 비영리 기구인 카터 재단을 설립한 뒤 민주주의 실현을 위해 제 3세계의 선거 감시 활동 및 기니 벌레에 의한 드라쿤쿠르스 질병 방재를 위해 힘썼다. 미국의 빈곤층 지원 활동, 사랑의 집짓기 운동, 국제 분쟁 중재 등의 활동도 했다.'),
            1486751: MiraclDoc('2431764#0', '갤러거', '갤러거의 다른 뜻은 다음과 같다.'),
        })
        self._test_docs('miracl/ru', count=9543918, items={
            0: MiraclDoc('7#0', 'Литва', 'Литва́ (), официальное название\xa0— Лито́вская Респу́блика ()\xa0— государство, расположенное в северо-восточной части Европы. Столица страны\xa0— Вильнюс.'),
            9: MiraclDoc('7#9', 'Литва', re.compile('^Традиционно считается, что этническая основа Литвы сформирована носителями археологочической культур.{94}ной Литвы и Северо\\-Западной Белоруссии\\. Около VII века н\\.\xa0э\\. литовский язык отделился от латышского\\.$', flags=48)),
            9543917: MiraclDoc('7761282#4', 'Грин, Роберт Фрэнсис', 'Скончался Роберт Грин 5 октября 1946 года.'),
        })
        self._test_docs('miracl/sw', count=131924, items={
            0: MiraclDoc('2#0', 'Akiolojia', re.compile('^Akiolojia \\(kutoka Kiyunani αρχαίος = "zamani" na λόγος = "neno, usemi"\\) ni somo linalohusu mabaki ya.{94}kwa kuchimba ardhi na kutafuta mabaki ya majengo, makaburi, silaha, vifaa, vyombo na mifupa ya watu\\.$', flags=48)),
            9: MiraclDoc('10#2', 'Daktari', '2) Kwa kufuata tabia za lugha nyingine "daktari" hutumiwa pia kama jina la heshima kwa mtu aliyepata shahada ya uzamivu au "PhD" ambayo ni shahada ya juu kabisa.'),
            131923: MiraclDoc('108712#3', 'Nafanikiwa Inspiration', 'Instagram: @nafanikiwa_inspiration'),
        })
        self._test_docs('miracl/te', count=518079, items={
            0: MiraclDoc('1#0', 'మొదటి పేజీ', '__NOEDITSECTION__'),
            9: MiraclDoc('786#8', 'గుంటూరు జిల్లా', re.compile('^గుంటూరు జిల్లా సగటున 33 మీటర్లు ఎత్తులో ఉంది\\. చాలవరకు సమతల ప్రదేశం\\. కొన్ని కొండలు కూడా ఉన్నాయి\\. కృష్.{408}ో ఎత్తిపోతల అనబడే జలపాతం నల్లమలై కొండలపై చంద్రవంక నదిపై ఉంది\\. దీనిలో 21మీ ఎత్తునుండి నీరు పారుతుంది\\.$', flags=48)),
            518078: MiraclDoc('273584#1', 'లక్ష్మణ్\u200cచాందా మండలం', '2011 భారత జనగణన గణాంకాల ప్రకారం - మొత్తం 34,739 - పురుషులు 17,515 - స్త్రీలు 19,224'),
        })
        self._test_docs('miracl/th', count=542166, items={
            0: MiraclDoc('1#0', 'หน้าหลัก', 'วิกิพีเดียดำเนินการโดยมูลนิธิวิกิมีเดีย องค์กรไม่แสวงผลกำไร ผู้ดำเนินการอีกหลาย ได้แก่'),
            9: MiraclDoc('545#7', 'ดาราศาสตร์', re.compile('^เมื่อสังคมมีวิวัฒนาการขึ้นในดินแดนต่าง ๆ การสังเกตการณ์ทางดาราศาสตร์ก็ซับซ้อนมากขึ้น โดยเฉพาะอย่างยิ.{553}ดาวต่าง ๆ เคลื่อนที่ไปโดยรอบ แนวคิดนี้เรียกว่า แบบจำลองแบบโลกเป็นศูนย์กลางจักรวาล \\(geocentric model\\)$', flags=48)),
            542165: MiraclDoc('1001591#3', 'มาเลยาเซมเลีย', "การต่อสู้ยังเป็นส่วนหนึ่งของหนังสือเล่มแรกของ Brezhnev's trilogy ซึ่งเป็นการที่ทำให้เกินจริงต่อบทบาทของเลโอนิด เบรจเนฟ ในช่วงมหาสงครามของผู้รักชาติ"),
        })
        self._test_docs('miracl/yo', count=49043, items={
            0: MiraclDoc('598#0', 'A', 'aa ab ac ad ae af ag ah ai aj ak al am an ao ap aq ar as at au av aw ax ay az'),
            9: MiraclDoc('604#0', 'G', 'ga gb gc gd ge gf gg gh gi gj gk gl gm gn go gp gq gr gs gt gu gv gw gx gy gz'),
            49042: MiraclDoc('71570#11', 'Federal University of Technology', '• Uche Jombo Rodriguez'),
        })
        self._test_docs('miracl/zh', count=4934368, items={
            0: MiraclDoc('13#0', '数学', '数学，是研究數量、结构以及空间等概念及其变化的一門学科，从某种角度看屬於形式科學的一種。數學利用抽象化和邏輯推理，從計數、計算、量度、對物體形狀及運動的觀察發展而成。數學家們拓展這些概念，以公式化新的猜想，以及從選定的公理及定義出發，嚴謹地推導出一些定理。'),
            9: MiraclDoc('13#9', '数学', '到了16世纪，算术、初等代数以及三角学等初等数学已大体完备。17世纪变量概念的产生使人们开始研究变化中的量与量的互相关系和图形间的互相变换，微积分的概念也在此時形成。随着數學轉向形式化，为研究数学基础而产生的集合论和数理逻辑等也开始发展。数学的重心从求解实际问题转变到对一般形式上的思考。'),
            4934367: MiraclDoc('7887526#10', '嘉寶潭之役', '三日之期已到，陳耀見部署已定，收下紅旗之後，仍豎立白旗，叛軍黨怒不可遏，攻勢洶洶而來，大有剿滅陳耀部隊的態勢，唯剛交戰沒多久，柑仔井林家、湳仔阮家收拾餘兵，再度集結而來，高舉白旗夾殺叛軍部隊。首領陳耀親上戰場，奮勇殺敵，陳姓家丁亦不落人後。待到自鹿港商船運來的四尊大炮，炮擊敵軍，賊黨鑒於頹勢已現，倉促收兵，嘉寶潭之危遂解。'),
        })

    def test_queries(self):
        self._test_queries('miracl/ar/train', count=3495, items={
            0: GenericQuery('1', 'ما هي المسألة الشرقية ؟'),
            9: GenericQuery('12', 'ماهو الأمن البشري ؟'),
            3494: GenericQuery('15481', 'من قاد الثورة العرابية في مصر؟'),
        })
        self._test_queries('miracl/ar/dev', count=2896, items={
            0: GenericQuery('0', 'هل عدم القيام بجهد جسماني ممكن ان يسبب الأرق؟'),
            9: GenericQuery('60', 'من هو مؤلف سلسلة حرب النجوم؟'),
            2895: GenericQuery('15511', 'من ابتكر المثلجات؟'),
        })
        self._test_queries('miracl/ar/test-a', count=936, items={
            0: GenericQuery('15513', 'كم عدد مرات فوز الأوروغواي ببطولة كاس العالم لكرو القدم؟'),
            9: GenericQuery('15523', 'من رئيس ألمانيا النازية في الحرب العالمية الثانية؟'),
            935: GenericQuery('16594', 'ماهى أول مؤلفة لي ج. ك. رولينغ ؟'),
        })
        self._test_queries('miracl/ar/test-b', count=1405, items={
            0: GenericQuery('1002902#0', 'ما هي قوة انتشار الدمار الذي تحدثه القنبلة النووية؟'),
            9: GenericQuery('1034420#0', 'أين تقع دولة تونس في أفريقيا ؟'),
            1404: GenericQuery('999091#0', 'من هو الحاكم في السعوديه؟'),
        })
        self._test_queries('miracl/bn/train', count=1631, items={
            0: GenericQuery('0', 'চেঙ্গিস খান কোন বংশের রাজা ছিলেন ?'),
            9: GenericQuery('12', 'বাংলাদেশের অষ্টম রাষ্ট্রপতি জিয়াউর রহমানের বাবার নাম কী ?'),
            1630: GenericQuery('2151', 'জামাতি ইসলাম দলটির প্রতিষ্ঠাতা কে ?'),
        })
        self._test_queries('miracl/bn/dev', count=411, items={
            0: GenericQuery('4', 'ইংরেজ আন্তর্জাতিক ক্রিকেট তারকা জর্জ গিবসন ম্যাকাউলি কি একজন ডানহাতি ব্যাটসম্যান ছিলেন ?'),
            9: GenericQuery('63', 'বাংলাদেশের অষ্টম রাষ্ট্রপতি জিয়াউর রহমান কবে জন্মগ্রহণ করেন ?'),
            410: GenericQuery('2152', 'বাংলা ব্যাকরণ মতে বিশেষণ কয় প্রকার ?'),
        })
        self._test_queries('miracl/bn/test-a', count=102, items={
            0: GenericQuery('2153', 'পশ্চিম ভারতের মহারাষ্ট্র রাজ্যের মুম্বাই শহরে নির্মিত গেটওয়ে অব ইন্ডিয়া স্থাপত্যটির ভিত্তিপ্রস্তর স্থাপন করেন কে ?'),
            9: GenericQuery('2162', 'মাওলানা ভাসানী বিজ্ঞান ও প্রযুক্তি বিশ্ববিদ্যালয়টির মোট আয়তন কত ?'),
            101: GenericQuery('2263', 'চট্টগ্রাম কি কখনো ধর্মপালের অধীনে ছিল ?'),
        })
        self._test_queries('miracl/bn/test-b', count=1130, items={
            0: GenericQuery('597183#0', 'বর্তমান সালে বাংলাদেশের জনসংখ্যা কত?'),
            9: GenericQuery('572931#0', 'এরিস্টটল কোন দেশের দার্শনিক ছিলেন?'),
            1129: GenericQuery('78447#0', 'কোন সাল থেকে কোন সাল পর্যন্ত ব্রিটিশ রাজ চলেছিল?'),
        })
        self._test_queries('miracl/de/dev', count=305, items={
            0: GenericQuery('81674#0', 'Wo ist das Gebiet der Irokesen-Indianer in Kanada?'),
            9: GenericQuery('7484600#0', 'Welcher Bahnhof ist der älteste in den USA?'),
            304: GenericQuery('3223301#0', 'Wie viele Sprachen gibt es insgesamt auf der Welt?'),
        })
        self._test_queries('miracl/de/test-b', count=712, items={
            0: GenericQuery('11531728#0', 'Wie lange muss man ein Polizist sein bevor man ein Detektiv werden kann in Deutschland?'),
            9: GenericQuery('11700929#0', 'Welche Religion ist am größten in Japan?'),
            711: GenericQuery('6333890#0', 'In welcher Sprache kommunizierten russische und norwegische Fischer miteinander?'),
        })
        self._test_queries('miracl/en/train', count=2863, items={
            0: GenericQuery('1', 'When was quantum field theory developed?'),
            9: GenericQuery('13', 'When were bluebonnets named the state flower of Texas?'),
            2862: GenericQuery('4446', 'What is the population of Mahwah, NJ?'),
        })
        self._test_queries('miracl/en/dev', count=799, items={
            0: GenericQuery('0', 'Is Creole a pidgin of French?'),
            9: GenericQuery('47', 'When did Aristagoras become leader of Miletus?'),
            798: GenericQuery('4443', 'Who was costume designer for the first Star Wars movie?'),
        })
        self._test_queries('miracl/en/test-a', count=734, items={
            0: GenericQuery('4447', 'Do zebra finches have stripes?'),
            9: GenericQuery('4456', 'When was Quezon City founded?'),
            733: GenericQuery('5193', 'When did the Bundaberg Central State School become a heritage-listed site?'),
        })
        self._test_queries('miracl/en/test-b', count=1790, items={
            0: GenericQuery('23819476#0', "Which summer dessert is often eaten during Wimbledon's tennis matches?"),
            9: GenericQuery('1765998#0', 'John F. Kennedy was assassinated in which city?'),
            1789: GenericQuery('56426977#0', 'When did Théodore Chabert enlisted in Frencg Royal Army?'),
        })
        self._test_queries('miracl/es/train', count=2162, items={
            0: GenericQuery('1769696#0', '¿Cuáles son las principales plantas fanerógamas?'),
            9: GenericQuery('5810974#0', '¿Cómo calcular el vértice de un triángulo?'),
            2161: GenericQuery('508901#0', '¿Qué trata de explicar los conceptos de la filosofía?'),
        })
        self._test_queries('miracl/es/dev', count=648, items={
            0: GenericQuery('1177652#0', '¿Qué es Anónimo de Rávena?'),
            9: GenericQuery('188191#0', '¿Es Rumanía una monarquía?'),
            647: GenericQuery('7523671#0', '¿En qué categoría fue campeón el piloto Ricardo Tormo?'),
        })
        self._test_queries('miracl/es/test-b', count=1515, items={
            0: GenericQuery('8541928#0', '¿En qué país se encuentra Santa Cruz de la Sierra?'),
            9: GenericQuery('8324682#0', '¿En qué continente está Azerbaiyán?'),
            1514: GenericQuery('2534161#0', '¿En qué países se publica la revista ¡HOLA!?'),
        })
        self._test_queries('miracl/fa/train', count=2107, items={
            0: GenericQuery('5682911#0', 'خواننده سنتی در چه دستگاهی آواز می خواتد؟'),
            9: GenericQuery('4334823#0', 'کاربرد برنامه نویسی در کامپیوتر چیست؟'),
            2106: GenericQuery('2528634#0', 'گیاه اسطخودوس از چه گونه ای است؟'),
        })
        self._test_queries('miracl/fa/dev', count=632, items={
            0: GenericQuery('5428682#0', 'سیاستمداران آمریکایی را چه کسی انتخاب می کند؟'),
            9: GenericQuery('4809078#0', 'از فناوری\u200cهای فیلمبرداری در کجا استفاده می شود؟'),
            631: GenericQuery('5174269#0', 'سینمای میانماربه چه فیلم هایی شهرت دارد؟'),
        })
        self._test_queries('miracl/fa/test-b', count=1476, items={
            0: GenericQuery('239796#0', 'کاربرد آمار در ریاضیات چیست؟'),
            9: GenericQuery('5090616#0', 'چرا سن قانونی برای اجرای حکم مهم است؟'),
            1475: GenericQuery('58872#0', 'از فتوشاپ در چه مواردی استفاده میشود؟'),
        })
        self._test_queries('miracl/fi/train', count=2897, items={
            0: GenericQuery('0', 'Milloin Charles Fort syntyi?'),
            9: GenericQuery('10', 'Montako kuuta Saturnuksella on?'),
            2896: GenericQuery('8271', 'Montako kaupunkia Suomessa on?'),
        })
        self._test_queries('miracl/fi/dev', count=1271, items={
            0: GenericQuery('1', 'Mitä on altruismi?'),
            9: GenericQuery('52', 'Mistä nimitys markka tulee?'),
            1270: GenericQuery('8305', 'Onko Bowser Marion arkkivihollinen?'),
        })
        self._test_queries('miracl/fi/test-a', count=1060, items={
            0: GenericQuery('8318', 'Milloin Kokemäki on perustettu?'),
            9: GenericQuery('8327', 'Minä vuonna Charles Darwin julkaisi kuuluisan teoriansa lajien synnystä?'),
            1059: GenericQuery('9571', 'Milloin käytiin Persianlahden sota?'),
        })
        self._test_queries('miracl/fi/test-b', count=711, items={
            0: GenericQuery('870432#0', 'Mitä ovat tilannekomediat?'),
            9: GenericQuery('347723#0', 'Mitä kieltä käytettiin tuohi kirjeissä?'),
            710: GenericQuery('3896#0', 'Mitä tarkoittaa käyttää strategista silmää?'),
        })
        self._test_queries('miracl/fr/train', count=1143, items={
            0: GenericQuery('10756354#0', 'Qui était Princesse Diana?'),
            9: GenericQuery('5873198#0', 'Qui est Walt Disney?'),
            1142: GenericQuery('8564179#0', 'Quelles sont les caractéristiques d’un vrai dictateur?'),
        })
        self._test_queries('miracl/fr/dev', count=343, items={
            0: GenericQuery('5607241#0', "Qu'est-ce que c'est une famille patricienne?"),
            9: GenericQuery('13910718#0', 'Où est situé Chūgoku au Japon?'),
            342: GenericQuery('10579479#0', "Combien y a-t-il d'éléments chimiques au total?"),
        })
        self._test_queries('miracl/fr/test-b', count=801, items={
            0: GenericQuery('4716921#0', "Qu'est-ce que c'est un explosif?"),
            9: GenericQuery('13260096#0', 'Qui est le président de Brésil?'),
            800: GenericQuery('11003060#0', 'Qui est la personne mongole historique la plus célèbre?'),
        })
        self._test_queries('miracl/hi/train', count=1169, items={
            0: GenericQuery('231591#0', 'भारत में कुल कितने राज्य है?'),
            9: GenericQuery('5138#0', 'विश्व में सबसे ज़ादा बोली जाने वाली भाषा क्या है ?'),
            1168: GenericQuery('1058385#0', 'संयुक्त राष्ट्र ने किस अमेरिकी अभिनेत्री को अपना विशेष दूत बनाया है?'),
        })
        self._test_queries('miracl/hi/dev', count=350, items={
            0: GenericQuery('1033752#0', 'कांग्रेस दल का नेता कौन है ?'),
            9: GenericQuery('229394#0', 'ईसा मसीह ने किस देश में जन्म लिया था?'),
            349: GenericQuery('727434#0', 'हाल ही में पद्म श्री पुरस्कार से सम्मानित किस भोजपुरी गायक का निधन हो गया है?'),
        })
        self._test_queries('miracl/hi/test-b', count=819, items={
            0: GenericQuery('217614#0', 'अमेरिका में कितने राज्य है?'),
            9: GenericQuery('1343627#0', 'अमेरिका के सबसे ऊँचे पर्वत का नाम क्या है?'),
            818: GenericQuery('575512#0', 'सबसे पहली मोटर गाढ़ी किस देश ने बनाई थी?'),
        })
        self._test_queries('miracl/id/train', count=4071, items={
            0: GenericQuery('5', 'siapakah orang tua John Fitzgerald Kennedy?'),
            9: GenericQuery('23', 'Kapan  Operasi hari Kiamat terjadi ?'),
            4070: GenericQuery('6147', 'Kapan Kaisar Tang Gaozu mulai menjabat ?'),
        })
        self._test_queries('miracl/id/dev', count=960, items={
            0: GenericQuery('3', 'Dimana James Hepburn meninggal?'),
            9: GenericQuery('41', 'Kapan perdagangan melalui armada mulai dilakukan oleh bangsa Eropa ?'),
            959: GenericQuery('6138', 'Kapan sepeda motor ditemukan ?'),
        })
        self._test_queries('miracl/id/test-a', count=731, items={
            0: GenericQuery('6148', 'Siapakah yang menemuka benua Amerika ?'),
            9: GenericQuery('6158', 'Berapa luas kota Blitar?'),
            730: GenericQuery('6976', 'Berapakah berat Ikan pari manta yag terbesar?'),
        })
        self._test_queries('miracl/id/test-b', count=611, items={
            0: GenericQuery('2684524#0', 'Gimaba cara membuat atap?'),
            9: GenericQuery('1044017#0', 'Bagiamana cara mesin bekerja?'),
            610: GenericQuery('1200317#0', 'Pada tahun berapa Amerika Serikat memiliki presiden pertama mereka?'),
        })
        self._test_queries('miracl/ja/train', count=3477, items={
            0: GenericQuery('1', 'サー・マイケル・フィリップ・ジャガーの出身は？'),
            9: GenericQuery('19', '桂 銀淑のデビュー曲は何？'),
            3476: GenericQuery('4630', 'ブラームスの出身はどこ？'),
        })
        self._test_queries('miracl/ja/dev', count=860, items={
            0: GenericQuery('0', '“ダン” ダニエル・ジャドソン・キャラハンの出身はどこ'),
            9: GenericQuery('26', 'パメラ・コールマン・スミスはいつ生まれた？'),
            859: GenericQuery('4629', '彭 徳懐はいつ生まれた？'),
        })
        self._test_queries('miracl/ja/test-a', count=650, items={
            0: GenericQuery('4631', '化学兵器禁止条約はどこで採択された？'),
            9: GenericQuery('4640', 'ダニエル・レイ・エインジはどこのプロ野球チームに所属した？'),
            649: GenericQuery('5352', 'クイズ番組『アメリカ横断ウルトラクイズ』の出場者はどのようなひとでしたか？'),
        })
        self._test_queries('miracl/ja/test-b', count=1141, items={
            0: GenericQuery('832900#0', 'シカゴの市長は誰ですか？'),
            9: GenericQuery('1213346#0', '飛行機を発明したのは誰ですか。'),
            1140: GenericQuery('900297#0', '名古屋市長は誰ですか。'),
        })
        self._test_queries('miracl/ko/train', count=868, items={
            0: GenericQuery('1', '로마의 면적은 서울시의 2배인가요?'),
            9: GenericQuery('16', '오버워치 출시일은 언제인가요?'),
            867: GenericQuery('1597', '오행이라는 것은 목·화·토·금·수의 오원소를 말하나요?'),
        })
        self._test_queries('miracl/ko/dev', count=213, items={
            0: GenericQuery('2', '합성생물학을 연구하는 방식은 탑다운 외 다른 방식은 무엇이 있나요?'),
            9: GenericQuery('58', '나폴레옹 1세는 언제 황제로 즉위하나요?'),
            212: GenericQuery('1582', '인체에는 몇 개의 세포가 있는가?'),
        })
        self._test_queries('miracl/ko/test-a', count=263, items={
            0: GenericQuery('1602', '유비 출생일은 언제인가요?'),
            9: GenericQuery('1615', '매슬로우의 5단계 욕구 중 제 1수준은 생리적 욕구인가요?'),
            262: GenericQuery('2016', '광주 비엔날레는 몇년마다 열리나요?'),
        })
        self._test_queries('miracl/ko/test-b', count=1417, items={
            0: GenericQuery('2386699#0', '일본은 어떤 언어를 사용하나요?'),
            9: GenericQuery('1229231#0', '어느 나라 소금이 유명한가요?'),
            1416: GenericQuery('279678#0', '털 제거는 어떻게 하나?'),
        })
        self._test_queries('miracl/ru/train', count=4683, items={
            0: GenericQuery('1', 'Когда был спущен на воду первый миноносец «Спокойный»?'),
            9: GenericQuery('21', 'Какой процент населения Земли ездит на правостороннем движении?'),
            4682: GenericQuery('6761', 'Какая площадь Аппенинского полуострова?'),
        })
        self._test_queries('miracl/ru/dev', count=1252, items={
            0: GenericQuery('0', 'Когда начался Кари́бский кризис?'),
            9: GenericQuery('33', 'Возле какой реки произошло Мианкальское восстание?'),
            1251: GenericQuery('6762', 'Кто убил Зорана Джинджича?'),
        })
        self._test_queries('miracl/ru/test-a', count=911, items={
            0: GenericQuery('6763', 'Когда появился термин правово́е госуда́рство?'),
            9: GenericQuery('6772', 'Какая максимальная скорость ЭД9?'),
            910: GenericQuery('7762', 'Когда впервые начали применять компьютерную графику в кинематографе?'),
        })
        self._test_queries('miracl/ru/test-b', count=718, items={
            0: GenericQuery('2514517#0', 'Когда вышел мультфильм “Король Лев”?'),
            9: GenericQuery('4130787#0', 'Как юридически оформить наследство?'),
            717: GenericQuery('269810#0', 'Сколько лет прожил Лев Гумилёв?'),
        })
        self._test_queries('miracl/sw/train', count=1901, items={
            0: GenericQuery('0', 'Je,nchi gani yenye kuzalisha chungwa kwa wingi zaidi duniani?'),
            9: GenericQuery('15', 'Je,nani mwanzilishi wa kampuni ya Visa Inc?'),
            1900: GenericQuery('2600', 'Nini maana ya data?'),
        })
        self._test_queries('miracl/sw/dev', count=482, items={
            0: GenericQuery('6', 'Bandari kubwa nchini Kenya iko wapi?'),
            9: GenericQuery('38', 'Jina kamili la Pelé ni lipi?'),
            481: GenericQuery('2597', 'Paul Schulze alizaliwa mwaka upi?'),
        })
        self._test_queries('miracl/sw/test-a', count=638, items={
            0: GenericQuery('2601', 'Je,Sarah Wayne Callies ana mume?'),
            9: GenericQuery('2611', 'Kuna kundinyota ngapi?'),
            637: GenericQuery('3270', 'Michael Wamalwa Kijana alifariki akiwa na miaka mingapi?'),
        })
        self._test_queries('miracl/sw/test-b', count=465, items={
            0: GenericQuery('20328#0', 'Dar-Es Salaam ni mji wa nchi gani?'),
            9: GenericQuery('39828#0', 'Helikopta ina gurudumu ngapi?'),
            464: GenericQuery('89888#0', 'Ni nini hutumiwa kupanga lugha kwa vikundi?'),
        })
        self._test_queries('miracl/te/train', count=3452, items={
            0: GenericQuery('0', 'వేప చెట్టు యొక్క శాస్త్రీయ నామం ఏమిటి?'),
            9: GenericQuery('11', 'తమరాం గ్రామ పిన్ కోడ్ ఏంటి?'),
            3451: GenericQuery('4867', 'ప్యూమా సంస్థని ఎప్పుడు స్థాపించారు?'),
        })
        self._test_queries('miracl/te/dev', count=828, items={
            0: GenericQuery('2', 'ఆర్మేనియా దేశంలో మొత్తం జిల్లాలు ఉన్నాయి?'),
            9: GenericQuery('50', 'మొట్టమొదటి పజెరో నమూనాను ఎక్కడ ప్రవేశపెట్టారు?'),
            827: GenericQuery('4869', 'తమన్నా భాటియా నటించిన మొదటి తెలుగు సినిమా ఏది?'),
        })
        self._test_queries('miracl/te/test-a', count=594, items={
            0: GenericQuery('4870', 'మహా సముద్రాలు ఎన్ని ఉన్నాయి?'),
            9: GenericQuery('4879', 'నండవ గ్రామ విస్తీర్ణత ఎంత?'),
            593: GenericQuery('5516', 'తాడేపల్లి రాఘవ నారాయణ శాస్త్రి ఎక్కడ జన్మించాడు?'),
        })
        self._test_queries('miracl/te/test-b', count=793, items={
            0: GenericQuery('28113#0', 'విశాఖపట్నం జిల్లా ఏ రాష్ట్రంలో ఉంది?'),
            9: GenericQuery('27851#0', 'అరకులోయ ఎత్తు ఏ ఎంత?'),
            792: GenericQuery('33638#0', 'పాలవంచ నుంచి హైదరాబాద్ కు గల దూరం ఎంత?'),
        })
        self._test_queries('miracl/th/train', count=2972, items={
            0: GenericQuery('0', 'มหาวิทยาลัยมหาสารคาม เปิดสอนกี่สาขาวิชา?'),
            9: GenericQuery('16', 'พระเจ้าบรมวงศ์เธอ กรมขุนสุพรรณภาควดี มีพี่น้องกี่คน ?'),
            2971: GenericQuery('4130', 'ใครเป็นผู้ก่อตั้ง โรงเรียนสาธิตแห่งมหาวิทยาลัยเกษตรศาสตร์?'),
        })
        self._test_queries('miracl/th/dev', count=733, items={
            0: GenericQuery('4', 'บันทึกเหตุการณ์และเรื่องราวต่าง ๆ ในยุคสามก๊กฉบับแรก ที่มีการบันทึกเป็นลายลักษณ์อักษรเรียกว่าอะไร?'),
            9: GenericQuery('51', 'สมาชิกสภาผู้แทนราษฎร มีหน้าที่หลักคืออะไร ?'),
            732: GenericQuery('4113', 'กรุงเทพมหานครมีกี่เขต?'),
        })
        self._test_queries('miracl/th/test-a', count=992, items={
            0: GenericQuery('4131', 'อยุธยามีกี่อำเภอ ?'),
            9: GenericQuery('4140', 'เชฟกระทะเหล็ก ประเทศไทย ออกอากาศครั้งแรกเมื่อใด ?'),
            991: GenericQuery('5321', 'หม่อมราชวงศ์สุขุมพันธุ์ บริพัตร เกิดเมื่อไหร่?'),
        })
        self._test_queries('miracl/th/test-b', count=650, items={
            0: GenericQuery('895001#0', 'วันชาติของประเทศไทยตรงกับวันที่เท่าไหร่'),
            9: GenericQuery('781364#0', 'วันวาเลนไทน์อยู่ในเดือนอะไร'),
            649: GenericQuery('140245#0', 'ปลานิล มีชื่อวิทยาศาตร์ว่าอย่างไร'),
        })
        self._test_queries('miracl/yo/dev', count=119, items={
            0: GenericQuery('10020#0', 'Odun wo ni wọn ṣe idije Olympiiki akọkọ?'),
            9: GenericQuery('13935#0', 'Awon orile ede wo ni won pe ni Balkani?'),
            118: GenericQuery('9593#0', 'Kí ni orúkọ bibi akọrin Naira Marley?'),
        })
        self._test_queries('miracl/yo/test-b', count=288, items={
            0: GenericQuery('10494#0', 'ọdun wo ni wọn pa funsho william?'),
            9: GenericQuery('12525#0', 'Ki ni oluilu orile ede Hungari?'),
            287: GenericQuery('8837#0', 'ẹgbẹ oloṣelu wo ni Boris Johnson wa pẹlu?'),
        })
        self._test_queries('miracl/zh/train', count=1312, items={
            0: GenericQuery('5455987#0', '月球到地球的距离是多少？'),
            9: GenericQuery('986470#0', '四川美食有哪些？'),
            1311: GenericQuery('1885930#0', '世界上哪些国家信天主教？'),
        })
        self._test_queries('miracl/zh/dev', count=393, items={
            0: GenericQuery('1719936#0', '二战是什么时候开始的？'),
            9: GenericQuery('67347#0', '电视剧《继承者们》的女主角是谁？'),
            392: GenericQuery('5517620#0', '中国女足有哪些成绩？'),
        })
        self._test_queries('miracl/zh/test-b', count=920, items={
            0: GenericQuery('59350#0', '最后一任法国国王是谁？'),
            9: GenericQuery('596861#0', '非洲有多少个国家？'),
            919: GenericQuery('3534377#0', '浙江有哪些旅游景点？'),
        })

    def test_qrels(self):
        self._test_qrels('miracl/ar/train', count=25382, items={
            0: TrecQrel('1', '26569#0', 1, 'Q0'),
            9: TrecQrel('1', '26569#4', 0, 'Q0'),
            25381: TrecQrel('15481', '120888#3', 0, 'Q0'),
        })
        self._test_qrels('miracl/ar/dev', count=29197, items={
            0: TrecQrel('0', '151236#1', 1, 'Q0'),
            9: TrecQrel('0', '3645940#4', 0, 'Q0'),
            29196: TrecQrel('15511', '1295397#0', 0, 'Q0'),
        })
        self._test_qrels('miracl/bn/train', count=16754, items={
            0: TrecQrel('0', '59523#1', 1, 'Q0'),
            9: TrecQrel('0', '296930#46', 0, 'Q0'),
            16753: TrecQrel('2151', '483238#12', 0, 'Q0'),
        })
        self._test_qrels('miracl/bn/dev', count=4206, items={
            0: TrecQrel('4', '717942#0', 1, 'Q0'),
            9: TrecQrel('4', '713734#0', 0, 'Q0'),
            4205: TrecQrel('2152', '14254#2', 0, 'Q0'),
        })
        self._test_qrels('miracl/de/dev', count=3144, items={
            0: TrecQrel('10055536#0', '2653#14', 1, 'Q0'),
            9: TrecQrel('10055536#0', '7006449#14', 0, 'Q0'),
            3143: TrecQrel('9943409#0', '173950#12', 0, 'Q0'),
        })
        self._test_qrels('miracl/en/train', count=29416, items={
            0: TrecQrel('1', '2078963#10', 1, 'Q0'),
            9: TrecQrel('1', '25312#23', 0, 'Q0'),
            29415: TrecQrel('4446', '124989#21', 0, 'Q0'),
        })
        self._test_qrels('miracl/en/dev', count=8350, items={
            0: TrecQrel('0', '462221#4', 1, 'Q0'),
            9: TrecQrel('0', '11236157#2', 0, 'Q0'),
            8349: TrecQrel('4443', '8132416#3', 0, 'Q0'),
        })
        self._test_qrels('miracl/es/train', count=21531, items={
            0: TrecQrel('10000698#0', '541735#4', 1, 'Q0'),
            9: TrecQrel('10000698#0', '6383339#7', 0, 'Q0'),
            21530: TrecQrel('9996500#0', '39277#14', 0, 'Q0'),
        })
        self._test_qrels('miracl/es/dev', count=6443, items={
            0: TrecQrel('10036600#0', '8156619#0', 1, 'Q0'),
            9: TrecQrel('10036600#0', '4779177#0', 0, 'Q0'),
            6442: TrecQrel('9997781#0', '8493995#1', 0, 'Q0'),
        })
        self._test_qrels('miracl/fa/train', count=21844, items={
            0: TrecQrel('1030103#0', '88822#0', 1, 'Q0'),
            9: TrecQrel('1030103#0', '910079#32', 0, 'Q0'),
            21843: TrecQrel('998155#0', '521645#12', 0, 'Q0'),
        })
        self._test_qrels('miracl/fa/dev', count=6571, items={
            0: TrecQrel('10163#0', '10162#31', 1, 'Q0'),
            9: TrecQrel('10163#0', '5352636#11', 0, 'Q0'),
            6570: TrecQrel('981101#0', '1206494#0', 0, 'Q0'),
        })
        self._test_qrels('miracl/fi/train', count=20350, items={
            0: TrecQrel('0', '254561#0', 1, 'Q0'),
            9: TrecQrel('2', '34783#36', 0, 'Q0'),
            20349: TrecQrel('8271', '16787#1', 0, 'Q0'),
        })
        self._test_qrels('miracl/fi/dev', count=12008, items={
            0: TrecQrel('1', '18044#1', 1, 'Q0'),
            9: TrecQrel('1', '18044#8', 0, 'Q0'),
            12007: TrecQrel('8305', '122347#4', 0, 'Q0'),
        })
        self._test_qrels('miracl/fr/train', count=11426, items={
            0: TrecQrel('1000816#0', '2797090#3', 1, 'Q0'),
            9: TrecQrel('1000816#0', '2797090#1', 0, 'Q0'),
            11425: TrecQrel('998997#0', '71269#10', 0, 'Q0'),
        })
        self._test_qrels('miracl/fr/dev', count=3429, items={
            0: TrecQrel('10037625#0', '148020#0', 1, 'Q0'),
            9: TrecQrel('10037625#0', '148020#6', 0, 'Q0'),
            3428: TrecQrel('9970463#0', '936867#66', 0, 'Q0'),
        })
        self._test_qrels('miracl/hi/train', count=11668, items={
            0: TrecQrel('100443#0', '1051327#15', 1, 'Q0'),
            9: TrecQrel('100443#0', '91720#8', 0, 'Q0'),
            11667: TrecQrel('997148#0', '10866#1', 0, 'Q0'),
        })
        self._test_qrels('miracl/hi/dev', count=3494, items={
            0: TrecQrel('1004960#0', '69380#11', 1, 'Q0'),
            9: TrecQrel('1004960#0', '538245#7', 0, 'Q0'),
            3493: TrecQrel('9978#0', '221542#9', 0, 'Q0'),
        })
        self._test_qrels('miracl/id/train', count=41358, items={
            0: TrecQrel('5', '12980#4', 1, 'Q0'),
            9: TrecQrel('5', '12980#9', 0, 'Q0'),
            41357: TrecQrel('6147', '436242#3', 0, 'Q0'),
        })
        self._test_qrels('miracl/id/dev', count=9668, items={
            0: TrecQrel('3', '115796#6', 1, 'Q0'),
            9: TrecQrel('3', '279066#2', 0, 'Q0'),
            9667: TrecQrel('6138', '41402#28', 0, 'Q0'),
        })
        self._test_qrels('miracl/ja/train', count=34387, items={
            0: TrecQrel('1', '119071#0', 1, 'Q0'),
            9: TrecQrel('1', '3317224#0', 0, 'Q0'),
            34386: TrecQrel('4630', '3846925#1', 0, 'Q0'),
        })
        self._test_qrels('miracl/ja/dev', count=8354, items={
            0: TrecQrel('0', '2681119#1', 1, 'Q0'),
            9: TrecQrel('0', '2681119#4', 0, 'Q0'),
            8353: TrecQrel('4629', '197987#3', 0, 'Q0'),
        })
        self._test_qrels('miracl/ko/train', count=12767, items={
            0: TrecQrel('1', '3228#0', 1, 'Q0'),
            9: TrecQrel('1', '128525#10', 0, 'Q0'),
            12766: TrecQrel('1597', '108520#9', 0, 'Q0'),
        })
        self._test_qrels('miracl/ko/dev', count=3057, items={
            0: TrecQrel('2', '317339#6', 1, 'Q0'),
            9: TrecQrel('2', '317339#3', 0, 'Q0'),
            3056: TrecQrel('1582', '76034#30', 0, 'Q0'),
        })
        self._test_qrels('miracl/ru/train', count=33921, items={
            0: TrecQrel('1', '2183682#1', 1, 'Q0'),
            9: TrecQrel('1', '1937636#2', 0, 'Q0'),
            33920: TrecQrel('6761', '89740#1', 1, 'Q0'),
        })
        self._test_qrels('miracl/ru/dev', count=13100, items={
            0: TrecQrel('0', '105156#0', 1, 'Q0'),
            9: TrecQrel('0', '6560940#1', 0, 'Q0'),
            13099: TrecQrel('6762', '5244374#14', 0, 'Q0'),
        })
        self._test_qrels('miracl/sw/train', count=9359, items={
            0: TrecQrel('0', '18032#10', 1, 'Q0'),
            9: TrecQrel('0', '35063#11', 0, 'Q0'),
            9358: TrecQrel('2600', '33987#0', 1, 'Q0'),
        })
        self._test_qrels('miracl/sw/dev', count=5092, items={
            0: TrecQrel('6', '32589#0', 1, 'Q0'),
            9: TrecQrel('6', '32589#3', 0, 'Q0'),
            5091: TrecQrel('2597', '34586#1', 0, 'Q0'),
        })
        self._test_qrels('miracl/te/train', count=18608, items={
            0: TrecQrel('0', '59628#4', 1, 'Q0'),
            9: TrecQrel('0', '40216#0', 0, 'Q0'),
            18607: TrecQrel('4867', '109661#0', 1, 'Q0'),
        })
        self._test_qrels('miracl/te/dev', count=1606, items={
            0: TrecQrel('2', '259006#0', 1, 'Q0'),
            9: TrecQrel('50', '108202#2', 1, 'Q0'),
            1605: TrecQrel('4869', '132304#0', 1, 'Q0'),
        })
        self._test_qrels('miracl/th/train', count=21293, items={
            0: TrecQrel('0', '12146#1', 1, 'Q0'),
            9: TrecQrel('12', '548193#17', 0, 'Q0'),
            21292: TrecQrel('4130', '32375#1', 1, 'Q0'),
        })
        self._test_qrels('miracl/th/dev', count=7573, items={
            0: TrecQrel('4', '9800#4', 1, 'Q0'),
            9: TrecQrel('4', '928787#0', 0, 'Q0'),
            7572: TrecQrel('4113', '235995#23', 0, 'Q0'),
        })
        self._test_qrels('miracl/yo/dev', count=1188, items={
            0: TrecQrel('10020#0', '10020#1', 1, 'Q0'),
            9: TrecQrel('10020#0', '68760#255', 0, 'Q0'),
            1187: TrecQrel('9593#0', '36891#0', 0, 'Q0'),
        })
        self._test_qrels('miracl/zh/train', count=13113, items={
            0: TrecQrel('1000222#0', '453#50', 1, 'Q0'),
            9: TrecQrel('1000222#0', '7345501#3', 0, 'Q0'),
            13112: TrecQrel('997826#0', '7757109#2', 0, 'Q0'),
        })
        self._test_qrels('miracl/zh/dev', count=3928, items={
            0: TrecQrel('1009493#0', '1016228#22', 1, 'Q0'),
            9: TrecQrel('1009493#0', '66771#5', 0, 'Q0'),
            3927: TrecQrel('992843#0', '293786#0', 0, 'Q0'),
        })


if __name__ == '__main__':
    unittest.main()
