import re
import unittest
from ir_datasets.datasets.wikiclir import WikiClirDoc, WikiClirQuery
from ir_datasets.formats import TrecQrel
from .base import DatasetIntegrationTest


class TestWikiclir(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('wikiclir/ar', count=535118, items={
            0: WikiClirDoc('7', 'ماء', re.compile('^ الماء هو سائل شفاف لا لون لهُ ولا رائحة، ويوجد في الكرة الأرضية في المسطّحات المائيّة من الجداول وا.{959}يّة في المناطق القطبيّة، في حين تتواجد 0\\.3 % من الماء العذب في الأنهار والبحيرات وفي الغلاف الجوّي \\.$', flags=48)),
            9: WikiClirDoc('90', 'عنكبوت', re.compile('^ الرُتَيْلاوات رتبة من صف العنكبيات، وهي أكبر رتبة في هذا الصف، إذا تشمل أكثر من 40,000 نوع في 3700 .{1001}نها تقضي معظم وقتها في اصطياد الحشرات والفتك بها \\. فلولاها لتكاثرت الحشرات وأتت على الأخضر واليابس \\.$', flags=48)),
            535117: WikiClirDoc('3769734', 'ريجينا سنيفر', re.compile("^ ريجينا سنيفرهي كاتبة ولدت في لبنان في عام 1962 \\. كتبت العديد من الكتب، ونشر آخرها في عام 2013، '' ب.{341} الحرب '' في لبنان، كتاب ترجم إلى اللغة العربية وحرره الفارابي في يوليو / تموز 2008 وقبله جورج قرم \\.$", flags=48)),
        })
        self._test_docs('wikiclir/ca', count=548722, items={
            0: WikiClirDoc('1', 'Àbac', re.compile("^àbac l'àbac \\( del llatí `` abăcus '' , i grec άβαξ\\-ακος , que significa `` taula '' \\) és una eina pe.{962}e napier i permeten llegir directament el resultat de la multiplicació sense fer sumes intermèdies \\.$", flags=48)),
            9: WikiClirDoc('18', 'Aeròbic', re.compile("^ laeròbic és una modalitat de gimnàstica sueca amb acompanyament musical que consisteix en una sèrie.{931} la combinació d'exercicis aeròbics amb tonificació dels músculs , també anomenat `` body power \\. ''$", flags=48)),
            548721: WikiClirDoc('1514683', 'Bepink-Cogeas', re.compile('^ el bepink\\-cogeas \\( codi uci : bpk \\) és un equip ciclista femení italià \\. creat al 2012 , té categor.{367}\\- web oficial \\- plantilles i resultats a cyclebase\\.nl \\- plantilles i resultats a procyclingstats\\.com$', flags=48)),
        })
        self._test_docs('wikiclir/zh', count=951480, items={
            0: WikiClirDoc('13', '数学', re.compile('^ 数学是利用符号语言研究数量、结构、变化以及空间等概念的一门学科，从某种角度看属于形式科学的一种。数学透过抽象化和逻辑推理的使用，由计数、计算、量度和对物体形状及运动的观察而产生。数学家们拓展这些概念.{939}问 ” ）。 史前的人类就已尝试用自然的法则来衡量物质的多少、时间的长短等抽象的数量关系，比如时间单位有日、季节和年等。算术（加减乘除）也自然而然地产生了。古代的石碑及泥版亦证实了当时已有几何的知识。$', flags=48)),
            9: WikiClirDoc('53', '经济学', re.compile('^ 经济学是一门对产品和服务的生产、分配以及消费进行研究的社会科学。西方语言中的 “ 经济学 ” 一词源于古希腊的。起初这一领域被称为政治经济学，但19世纪经济学家採用简短的「经济学」一词来代表「经济科.{881}过度广泛，而且无法将分析的范围侷限在对于市场的研究上。然而，自从1960年代起，由于理性选择理论和其引发的赛局理论不断将经济学的研究领域扩张，这个定义已经获得广泛认同，尽管仍有一些对此定义的批评存在。$', flags=48)),
            951479: WikiClirDoc('5795145', '族群', re.compile('^ 族群（），是指一群人，他们认为彼此共享了相同的祖先、血缘、外貌、历史、文化、习俗、语言、地域、宗教、生活习惯与国家体验等，因此形成一个共同的群体。为区分我族及「他者」的分类方式之一。族群含义在20世.{689}或方言分类，如客家人、闽南人。 宗教层面 \\. \\- 按宗教信仰对人群分类，如信仰伊斯兰教的群体穆斯林。 参见 \\. \\- 国民（nation） \\- 人种（race） \\- 民系 \\- 氏族 \\- 部落 \\- 原住民$', flags=48)),
        })
        self._test_docs('wikiclir/cs', count=386906, items={
            0: WikiClirDoc('10', 'Astronomie', re.compile('^ astronomie , řecky αστρονομία z άστρον \\( astron \\) hvězda a νόμος \\( nomos \\) zákon , česky též hvězdá.{845} , například planet ve sluneční soustavě \\. základem nebeské mechaniky jsou práce keplera a newtona \\.$', flags=48)),
            9: WikiClirDoc('21', 'Matematika', re.compile("^ matematika \\( z řeckého \\( `` mathematikós '' \\) = `` milující poznání '' ; \\( `` máthema '' \\) = `` věd.{1089}kem v reálném světě \\. některé obory čisté matematiky se nacházejí na pomezí s logikou či filozofií \\.$", flags=48)),
            386905: WikiClirDoc('1319204', 'Helena Rubinsteinová', re.compile("^ helena rubinsteinová , rodným jménem chaja rubinsteinová \\( 25\\. prosince 1872 , krakov – 1\\. dubna 19.{1121}aci `` helena rubinstein foundation '' , kterou založila \\. byla sestřenicí filozofa martina bubera \\.$", flags=48)),
        })
        self._test_docs('wikiclir/nl', count=1908260, items={
            0: WikiClirDoc('1', 'Albert Speer', re.compile('^ berthold konrad hermann albert speer \\( mannheim , 19 maart 1905 – londen , 1 september 1981 \\) was e.{1195}ald in 1927 bleef speer nog meerdere jaren , als tessenows assistent , aan de hogeschool verbonden \\.$', flags=48)),
            9: WikiClirDoc('13', 'Astronomie', re.compile('^ astronomie of sterrenkunde is de wetenschap die zich bezighoudt met de observatie en de studie van .{949}eten bijvoorbeeld zijn meestal ontleend aan amateurastronomen die deze komeet als eerste waarnamen \\.$', flags=48)),
            1908259: WikiClirDoc('4848272', 'Karen Briggs (judoka)', re.compile('^karen briggs \\( judoka \\) karen valerie briggs \\( kingston upon hull , 11 april 1963 \\) is een voormalig.{884}land \\( – 48 kg \\) \\- – 1991 praag , tsjecho\\-slowakije \\( – 48 kg \\) \\- – 1981 madrid , spanje \\( – 48 kg \\)$', flags=48)),
        })
        self._test_docs('wikiclir/fi', count=418677, items={
            0: WikiClirDoc('1', 'Amsterdam', re.compile('^ amsterdam on alankomaiden pääkaupunki \\. amsterdam on väkiluvultaan alankomaiden suurin kaupunki , h.{1173}voi , kun se pystyi viemään yhä useampaa tuotetta muualle eurooppaan vapaasti , esimerkiksi olutta \\.$', flags=48)),
            9: WikiClirDoc('14', 'Aleksis Kivi', re.compile('^ aleksis kivi \\( oikealta nimeltään alexis stenvall \\) \\( 10\\. lokakuuta 1834 nurmijärvi – 31\\. joulukuut.{1117}johan stenvall oli merimies \\. kirjailijan oma isä erik stenvall oli asunut lapsuutensa helsingissä \\.$', flags=48)),
            418676: WikiClirDoc('1401493', 'Jordan Rowley', ' jordan rowley ( s. 3. huhtikuuta 1990 edmonton ) on jääkiekkoilija , joka pelaa lahden pelicansissa .'),
        })
        self._test_docs('wikiclir/fr', count=1894397, items={
            0: WikiClirDoc('3', 'Antoine Meillet', re.compile("^ paul jules antoine meillet , né le à moulins \\( allier \\) et mort le à châteaumeillant \\( cher \\) , est.{884}a au linguiste auguste carrière à la tête de la chaire d'arménien à l'école des langues orientales \\.$", flags=48)),
            9: WikiClirDoc('19', 'Algorithme', re.compile("^ un algorithme est une suite finie et non ambiguë d ’ opérations ou d'instructions permettant de rés.{1688}nécessaire pour amener un algorithme à son terme , en fonction de la quantité de données à traiter \\.$", flags=48)),
            1894396: WikiClirDoc('11055655', 'Elisabeth Maxwell', re.compile("^ elisabeth `` betty '' maxwell , née meynard , née le et morte le , est une historienne d'origine fr.{866}mpire de la presse \\. elle donne naissance à 9 enfants , mais deux d ’ entre eux meurent en bas\\-âge \\.$", flags=48)),
        })
        self._test_docs('wikiclir/de', count=2091278, items={
            0: WikiClirDoc('1', 'Alan Smithee', re.compile("^ alan smithee steht als pseudonym für einen fiktiven regisseur , der filme verantwortet , bei denen .{1223}ariante `` alan smithee '' war das anagramm `` the alias men '' vermutlich kein entstehungsgrund \\) \\.$", flags=48)),
            9: WikiClirDoc('17', 'Liste von Autoren/K', ' kh . - yasmina khadra ( * 1955 )'),
            2091277: WikiClirDoc('10015849', 'Soli (BiH)', re.compile('^soli \\( bih \\) soli war ein bosnisches gebiet \\( oblast \\) und eine gespanschaft im mittelalter \\. das ze.{1109} teil des sandžaks zvornik \\( zvornički sandžak \\) und des kadiluk srebrenik \\( srebrenički kadiluk \\) \\.$', flags=48)),
        })
        self._test_docs('wikiclir/it', count=1347011, items={
            0: WikiClirDoc('2', 'Armonium', re.compile("^ l'armonium o armonio \\( in francese , `` harmonium '' \\) è un tipo di organo costituito da una tastie.{1119}quella più alta e , rispettivamente , svolgono l'azione verso l'ottava bassa e verso l'ottava alta \\.$", flags=48)),
            9: WikiClirDoc('20', 'Abbie Hoffman', re.compile('^ di origini ebraiche , dotato di una personalità sardonica e vulcanica , di orientamento anarchico e.{937}ers \\( degli attori diventati attivisti sociali \\) , distribuendo cibo gratis e organizzando alloggi \\.$', flags=48)),
            1347010: WikiClirDoc('6494686', 'Superflat', re.compile("^ il `` superflat '' è un movimento artistico postmoderno , influenzato dai manga e dagli anime , fon.{959}e boy : the arts of japan ’ s exploding subculture \\. new york : japan society \\. isbn 0\\-913304\\-57\\-3 \\.$", flags=48)),
        })
        self._test_docs('wikiclir/ja', count=1071292, items={
            0: WikiClirDoc('5', 'アンパサンド', re.compile("^ アンパサンド \\( , \\& \\) とは「…と…」を意味する記号である。ラテン語の の合字で、trebuchet msフォントでは、と表示され `` et '' の合字であることが容易にわかる。amper.{874}グル \\) （アンド）は、浜崎あゆみが2003年に発売した4曲入りマキシシングル。 \\- \\& \\( 一青窈のアルバム \\) （アンド）は、一青窈が2005年に発売したアルバム、及び同アルバムに収録された楽曲。$", flags=48)),
            9: WikiClirDoc('43', 'コケ植物', re.compile('^ コケ植物（コケしょくぶつ、）とは、陸上植物かつ非維管束植物であるような植物の総称、もしくはそこに含まれる植物のこと。コケ類（コケるい）や蘚苔類（せんたいるい）、蘚苔植物（せんたいしょくぶつ）などとも.{861}糸状の原糸体（げんしたい、protonema）というものを形成する。原糸体は葉緑体をもち、基質表面に伸びた後、その上に植物体が発達を始め配偶体となる。なお、一部に生涯にわたって原糸体を持つものがある。$', flags=48)),
            1071291: WikiClirDoc('3641139', '若杉明', re.compile('^ 若杉明（わかすぎ あきら、1929年11月19日\\- \\) は、日本の会計学者、横浜国立大学名誉教授。 横須賀市出身。1958年東京大学大学院経済学会計学博士課程満期退学。68年「実現概念の展開 その会.{814} \\& aの財務・会計戦略』編著 ビジネス教育出版社 1989 \\- 『ソフト化社会と会計』編著 ビジネス教育出版社 1989 \\- 『リストラクチャリングの財務・会計戦略』編 ビジネス教育出版社 1991$', flags=48)),
        })
        self._test_docs('wikiclir/ko', count=394177, items={
            0: WikiClirDoc('5', '지미 카터', re.compile("^ 제임스 얼 `` 지미 '' 카터 주니어 \\( , 1924년 10월 1일 \\~ \\) 는 민주당 출신 미국 39번째 대통령 \\( 1977년 \\~ 1981년 \\) 이다 \\. 지미 카터는 조지아 주.{1114}신 6,000명을 감축하는 데 그쳤다 \\. 또한 박정희 정권의 인권 문제 등과의 논란으로 불협화음을 냈으나 , 1979년 6월 하순 , 대한민국을 방문하여 관계가 다소 회복되었다 \\.$", flags=48)),
            9: WikiClirDoc('31', '음계', re.compile('^ 음계 \\( 音階 \\) 는 음악에서 음높이 \\( pitch \\) 순서로 된 음의 집합을 말한다 \\. 악곡을 주로 구성하는 음을 나타낸 것이며 음계의 종류에 따라 곡의 분위기가 달라진다 \\. .{888} 變徵 \\) \\-올림화 \\( fa \\) ·치\\-솔·우\\-라·변궁 \\( 變宮 \\) \\-시로 7음계를 많이 쓴다 \\. 한국 전통 음악에서는 5음계 외에도 3음계 또는 악계통에서는 7음계 등이 쓰인다 \\.$', flags=48)),
            394176: WikiClirDoc('1824675', '안세호', re.compile('^ 안세호 \\( 1981년 2월 17일 \\~ \\) 는 대한민국의 배우이다 \\. 학력 \\. \\- 대진대학교 연극영화학부 출연작 \\. 영화 \\. \\- 《골든슬럼버》 \\( 2017년 \\) \\- 《군함도》 \\( .{445}링》 \\( 2005년 \\) \\- 찌라시청년 역 드라마 \\. \\- 《나의 판타스틱한 장례식》 \\( 2015년 , sbs \\) \\- 동수네 작업반 반장 역 \\- 《삼총사》 \\( 2014년 , tvn \\)$', flags=48)),
        })
        self._test_docs('wikiclir/no', count=471420, items={
            0: WikiClirDoc('2', 'Akershus', re.compile("^ akershus \\( fra norrønt `` akr '' , åker , og `` hús '' , borg eller kastell \\) er et norsk fylke , s.{1272}lsen fylke \\. i 1948 ble aker herred overført fra akershus til å bli en del av oslo \\( by og fylke \\) \\.$", flags=48)),
            9: WikiClirDoc('18', 'Atalanta BC', re.compile('^ atalanta bergamasca calcio er en italiensk fotballklubb \\. den ble grunnlagt i 1907 i byen bergamo i.{383} 1978–79 \\) \\- glenn strömberg \\( 1984–85 \\) \\- filippo inzaghi \\( 1996–97 \\) \\- christian vieri \\( 2006–07 \\)$', flags=48)),
            471419: WikiClirDoc('1521098', 'VM i vektløfting 1910', ' vm i vektløfting 1910 ( verdensmesterskapet i vektløfting ) ble arrangert i düsseldorf og wien i to forskjellige turneringer i 1910 .'),
        })
        self._test_docs('wikiclir/nn', count=133290, items={
            0: WikiClirDoc('1', 'Hovudside', ' __ingainnhaldsliste__ __ingabolkredigering__'),
            9: WikiClirDoc('28', 'Jødedommen', re.compile('^ jødedommen er den religiøse kulturen åt det jødiske folket \\. han er ein av dei først dokumenterte m.{1106} utøvinga av desse lovane og boda slik dei blir tolka av dei ulike antikke og moderne autoritetane \\.$', flags=48)),
            133289: WikiClirDoc('341641', 'Harry Danielsen', re.compile('^ harry danielsen var ein norsk skulemann og politikar frå rødøy i nordland \\. han representerte nordl.{883} då medlem i forbrukar\\- og administrasjonskomitéen \\. sommaren 1987 melde danielsen seg ut av høgre \\.$', flags=48)),
        })
        self._test_docs('wikiclir/pl', count=1234316, items={
            0: WikiClirDoc('2', 'AWK', re.compile('^ awk – interpretowany język programowania , którego główną funkcją jest wyszukiwanie i przetwarzanie.{895}dice_2 \\) i wartości , które mają być udostępnione w predefiniowanych zmiennych codice_3 i codice_4 \\.$', flags=48)),
            9: WikiClirDoc('15', 'AmigaOS', re.compile('^ amigaos – system operacyjny opracowany przez firmę commodore international dla produkowanych przez .{942}ta implementacja systemu amigaos pod nazwą aros \\. dostępna jest ona między innymi na platformę x86 \\.$', flags=48)),
            1234315: WikiClirDoc('4059443', 'Sóweczka ekwadorska', re.compile("^ sóweczka ekwadorska \\( `` glaucidium nubicola '' \\) – gatunek małego ptaka z rodziny puszczykowatych .{1006}żej spokrewnione są z dwuplamistymi \\( `` g\\. gnoma '' \\) i kostarykańskimi \\( `` g\\. costaricanum '' \\) \\.$", flags=48)),
        })
        self._test_docs('wikiclir/pt', count=973057, items={
            0: WikiClirDoc('220', 'Astronomia', re.compile("^ astronomia é uma ciência natural que estuda corpos celestes \\( como estrelas , planetas , cometas , .{963}vium '' que , junto com o `` trivium '' , compunha a metodologia de ensino das sete artes liberais \\.$", flags=48)),
            9: WikiClirDoc('235', 'Lista de padrões de arquivo gráfico', re.compile('^ \\- amiga interchange file format \\( iff \\) \\- adobe photoshop image \\( psd \\) \\- compuserv graphics interc.{293}f/tiff \\) \\- truevision targa \\( tga \\) \\- windows and os/2 bitmap \\( bmp/dib \\) \\- zsoft paintbrush \\( pcx \\)$', flags=48)),
            973056: WikiClirDoc('5499216', 'Chaudhry Muhammad Ali', re.compile('^ chaudhry mohammad ali \\( punjabi , urdu : چوہدری محمد علی\u200e ; 15 de julho de 1905 – 2 de dezembro de .{189}e estado em 1958\\. ao longo da sua carreira , foi também ministro das finanças e ministro da defesa \\.$', flags=48)),
        })
        self._test_docs('wikiclir/ro', count=376655, items={
            0: WikiClirDoc('1', 'Rocarta', re.compile('^ rocarta este o enciclopedie în format electronic care conține articole legate de românia , republic.{1032}imilare tipărite , precum și albume de imagini în care s\\-au investit mai mulți sau mai puțini bani \\.$', flags=48)),
            9: WikiClirDoc('24', 'Romania (dezambiguizare)', re.compile('^romania \\( dezambiguizare \\) romania , în această grafie , se poate referi la : \\- capul , mina de baux.{948}ui roman de răsărit în secolele vi și vii , când restul italiei trecuse sub stăpânirea lombarzilor \\.$', flags=48)),
            376654: WikiClirDoc('2013894', 'Rezonanță (chimie)', re.compile('^rezonanță \\( chimie \\) în chimie , rezonanța sau mezomeria face referire la oscilarea structurii chimi.{805}r , și nu prin poziția nucleelor \\. vezi și \\. \\- aromaticitate \\- tautomerie \\- delocalizare electronică$', flags=48)),
        })
        self._test_docs('wikiclir/ru', count=1413945, items={
            0: WikiClirDoc('7', 'Литва', re.compile('^литва литва́ \\( \\) , официальное название — лито́вская респу́блика \\( \\) — государство , географически р.{867}олм аукштояс \\( \\) \\( или аукштасис калнас \\( \\) \\) в юго\\-восточной части страны , в 23,5 км от вильнюса \\.$', flags=48)),
            9: WikiClirDoc('27', 'Киевская Русь', re.compile("^киевская русь ки́евская русь , древнеру́сское госуда́рство , дре́вняя русь \\( ' , ' , , др\\.\\-сканд \\. `.{909}ической дезинтеграции , что впоследствии сыграло важную роль в процессе объединения русских земель \\.$", flags=48)),
            1413944: WikiClirDoc('7070375', 'Перекрёстки (телесериал, 1994)', re.compile('^перекрёстки \\( телесериал , 1994 \\) перекрёстки \\( \\) — мексиканский 68 серийный телесериал 1994 года те.{840}chir \\- orlando soles \\- isabel andrade \\- celia álvarez de soles \\- héctor cruz lara \\- reynaldo álvarez$', flags=48)),
        })
        self._test_docs('wikiclir/en-simple', count=127089, items={
            0: WikiClirDoc('1', 'April', re.compile('^ april is the fourth month of the year , and comes between march and may \\. it is one of four months .{1212}mmediately after that , april finishes on the same day of the week as january of the previous year \\.$', flags=48)),
            9: WikiClirDoc('18', 'Andouille', re.compile('^ andouille is a type of pork sausage \\. it is spicy \\( hot in taste \\) and smoked \\. there are different.{560}ane for a maximum of seven or eight hours , at about 175 degrees fahrenheit \\( 80 degrees celsius \\) \\.$', flags=48)),
            127088: WikiClirDoc('594702', 'Digital video', re.compile('^ digital video is a representation of moving visual images in the form of encoded \\. this is in contr.{831}include hdmi , displayport , digital visual interface \\( dvi \\) and serial digital interface \\( sdi \\) \\.$', flags=48)),
        })
        self._test_docs('wikiclir/es', count=1302958, items={
            0: WikiClirDoc('7', 'Andorra', re.compile('^ andorra , oficialmente principado de andorra \\( \\) , es un pequeño país soberano del suroeste de euro.{831}a oficial es el catalán que convive con el español y en menor medida con el francés y el portugués \\.$', flags=48)),
            9: WikiClirDoc('24', 'Arquitectura', re.compile("^ la arquitectura es el arte y la técnica de proyectar , diseñar , construir y modificar el hábitat h.{1568}ángulos y llevada a término por una mente y una inteligencia culta '' '' \\( del lib \\. i , cap \\. i \\) \\.$", flags=48)),
            1302957: WikiClirDoc('8045476', 'Inés Márquez Moreno', re.compile('^ poetisa cuencana nacida el 7 de junio de 1916 , hija del dr\\. ricardo márquez tapia y de la sra \\. ro.{1166}o vega , humberto mata , enrique noboa arízaga , rigoberto cordero león y jacinto cordero espinoza \\.$', flags=48)),
        })
        self._test_docs('wikiclir/sw', count=37079, items={
            0: WikiClirDoc('2', 'Akiolojia', re.compile("^ akiolojia \\( kutoka kiyunani αρχαίος = `` zamani '' na λόγος = `` neno , usemi '' \\) ni somo linalohu.{1003}i ya kiroma , lakini mji uliharibika kabisa na kufunikwa na majivu ya volkeno vesuvio mwaka 79 b\\.k \\.$", flags=48)),
            9: WikiClirDoc('33', 'Lugha asilia', re.compile("^ 1\\. lugha asilia ni lugha ambayo ilikua kama sehemu ya utamaduni wa umma fulani ambao watu wake wana.{131} kuzungumzwa na watu , na lugha za kompyuta na za kuandaa programu zinaitwa `` lugha za kuundwa '' \\.$", flags=48)),
            37078: WikiClirDoc('92114', 'Kaptura', re.compile('^ kaptura ni vazi lililovaliwa na wanaume na wanawake juu ya eneo la pelvic yao , wakizunguka kiuno n.{272}joto au katika mazingira ambapo faraja na mtiririko wa hewa ni muhimu zaidi kuliko ulinzi wa miguu \\.$', flags=48)),
        })
        self._test_docs('wikiclir/sv', count=3785412, items={
            0: WikiClirDoc('1', 'Amager', re.compile('^ amager är en dansk ö i öresund \\. öns norra och västra delar tillhör köpenhamn , medan övriga delar .{1153}i många köpenhamnsbors ögon , men inställningen håller på att ändras i takt med stigande huspriser \\.$', flags=48)),
            9: WikiClirDoc('12', '1 april', re.compile('^ 1 april är den 91 : a dagen på året i den gregorianska kalendern \\( 92 : a under skottår \\) \\. det åte.{923}en fransk abbot från 1100\\-talet , på dagens datum före 1747 , då det utgick till förmån för harald \\.$', flags=48)),
            3785411: WikiClirDoc('8048978', 'Surçina', ' surçina ( albanska : surçina , serbiska : svrčina ) är en by i kosovo . den ligger i kommunen ferizaj . enligt den senaste folkräkningen år 2011 fanns det 222 invånare .'),
        })
        self._test_docs('wikiclir/tl', count=79008, items={
            0: WikiClirDoc('5', 'Wikipedia', re.compile('^ ang wikipedia ay isang ensiklopedya na may basehang wiki at may malayang nilalaman \\. ito ay tinataw.{845}l , at apache \\) \\. ang mga kalahok sa wikipediang sumusunod , at pinagtitibay , ang ilang patakaran \\.$', flags=48)),
            9: WikiClirDoc('603', 'Astronomiya', re.compile("^ ang dalubtalaan \\( astronomiya \\) ay isang agham na kinapapalooban ng pagmamasid at pagpapaliwanag ng.{963}' = `` astron '' \\+ `` nomos '' , na mayroong literal na kahulugang `` '' batas ng mga bituin '' '' \\.$", flags=48)),
            79007: WikiClirDoc('267691', 'Nao Iwadate', ' si nao iwadate ( ipinaganak agosto 17 , 1988 ) ay isang manlalaro ng putbol sa hapon .'),
        })
        self._test_docs('wikiclir/tr', count=295593, items={
            0: WikiClirDoc('10', 'Cengiz Han', re.compile("^ cengiz han \\( `` cenghis khan '' , `` çinggis haan '' ya da doğum adıyla temuçin \\( anlamı : demirci .{1648}a çağırmış ve moğolca için uygur alfabesini uyarlatarak bunu çocuklarına da öğretmesini istemiştir \\.$", flags=48)),
            9: WikiClirDoc('40', 'Beşiktaş JK', re.compile("^ beşiktaş jimnastik kulübü , 1903 yılında istanbul'da kurulan spor kulübüdür \\. bereket jimnastik kul.{864}ndan biridir \\. armasında türk bayrağı amblemi taşıma hakkını elde etmiş az sayıda takımdan biridir \\.$", flags=48)),
            295592: WikiClirDoc('2268203', 'Prachatice İlçesi', re.compile("^prachatice ilçesi prachatice ilçesi , çek cumhuriyeti'nin güney bohemya bölgesinde bulunan ilçedir \\..{707}zí \\- volary \\- vrbice \\- záblatí \\- zábrdí \\- zálezly \\- zbytiny \\- zdíkov \\- žárovná \\- želnava \\- žernovice$", flags=48)),
        })
        self._test_docs('wikiclir/uk', count=704903, items={
            0: WikiClirDoc('3', 'Головна сторінка', 'головна сторінка'),
            9: WikiClirDoc('592', 'Біологія', re.compile('^біологія біоло́гія \\( — життя , — слово ; наука \\) — система наук , що вивчає життя в усіх його проява.{1135}авляють самостійні дисципліни — анатомія , фізіологія , гістологія , біохімія , мікробіологія тощо \\.$', flags=48)),
            704902: WikiClirDoc('2485891', 'Хліб, любов і фантазія', re.compile("^хліб , любов і фантазія « хліб .*, що не любить чужої жалості , розриває її .$", flags=48)),
        })
        self._test_docs('wikiclir/vi', count=1392152, items={
            0: WikiClirDoc('4', 'Internet Society', re.compile("^ internet society hay isoc là một tổ chức quốc tế hoạt động phi lợi nhuận , phi chính phủ và bao gồm.{820}d the internet society '' \\- về internet engineering task force và isoc , bài của vint cerf 18/6/1995$", flags=48)),
            9: WikiClirDoc('56', 'Lào', re.compile("^ lào \\( , , `` lāo '' \\) , tên chính thức là nước cộng hoà dân chủ nhân dân lào , \\( tiếng lào : ສາທາລະ.{922} kết quả là chấm dứt chế độ quân chủ , phong trào pathet lào theo chủ nghĩa cộng sản lên nắm quyền \\.$", flags=48)),
            1392151: WikiClirDoc('6111969', 'Sơn lục đậu', re.compile('^ còn gọi là vọng giang nam , cốt khí mồng , dương giác đậu , giang nam đậu , thạch quyết minh , dã b.{875} ta dùng toàn bộ cây , hay chỉ lá , hái hạt về phơi khô \\. ở việt nam người ta chưa chú ý khai thác \\.$', flags=48)),
        })

    def test_queries(self):
        self._test_queries('wikiclir/ar', count=324489, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('324', 'Academy Awards', 'the , now known officially as the oscars, is a set of twenty-four for artistic and technical merit in the american film industry, given annually by the of motion picture arts and sciences (ampas), to recognize excellence in cinematic achievements as assessed by the voting membership.'),
            324488: WikiClirQuery('54964051', 'Tal Afar offensive (2017)', 'the is an ongoing announced on 20 august 2017 by iraqi prime minister haider al-abadi in order to liberate the region from the islamic state of iraq and the levant (isil).'),
        })
        self._test_queries('wikiclir/ca', count=339586, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('316', 'Academy Award for Best Production Design', 'the recognizes achievement for art direction in film.'),
            339585: WikiClirQuery('54965687', 'Karl Heinrich Gräffe', '(1799-1873) was a german mathematician, who was professor at university of zurich.'),
        })
        self._test_queries('wikiclir/zh', count=463273, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('316', 'Academy Award for Best Production Design', 'the recognizes achievement for art direction in film.'),
            463272: WikiClirQuery('54967133', 'United Nations Security Council Resolution 2371', 'the unanimously adopted on august 5, 2017, with approval of all the five permanent members and the ten non-permanent members in response to north korea’s july 2017 missile tests.'),
        })
        self._test_queries('wikiclir/cs', count=233553, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('334', 'International Atomic Time', "(tai, from the french name ) is a high-precision coordinate standard based on the notional passage of proper on earth's geoid."),
            233552: WikiClirQuery('54961893', 'Vincenzo Legrenzio Ciampi', '(piacenza, 2 april 1719 – venice, 30 march 1762) was an italian composer.'),
        })
        self._test_queries('wikiclir/nl', count=687718, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('324', 'Academy Awards', 'the , now known officially as the oscars, is a set of twenty-four for artistic and technical merit in the american film industry, given annually by the of motion picture arts and sciences (ampas), to recognize excellence in cinematic achievements as assessed by the voting membership.'),
            687717: WikiClirQuery('54967572', 'SV Marken', 'sportvereniging (dutch for "sport club marken", commonly shortened to cv marken, or just marken) is an association football club from marken, netherlands.'),
        })
        self._test_queries('wikiclir/fi', count=273819, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('316', 'Academy Award for Best Production Design', 'the recognizes achievement for art direction in film.'),
            273818: WikiClirQuery('54966570', 'Nadezhda Babkina', 'georgieva (; born 19 march, 1950, chyorny yar, astrakhan oblast, soviet union) is а soviet and russian folk and pop singer.'),
        })
        self._test_queries('wikiclir/fr', count=1089179, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('316', 'Academy Award for Best Production Design', 'the recognizes achievement for art direction in film.'),
            1089178: WikiClirQuery('54967313', 'Lilly Wood and The Prick au Trianon', 'is a 2013 french musical movie directed by benjamin lemaire.'),
        })
        self._test_queries('wikiclir/de', count=938217, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('316', 'Academy Award for Best Production Design', 'the recognizes achievement for art direction in film.'),
            938216: WikiClirQuery('54967235', 'Journal of Risk and Uncertainty', 'the is a bimonthly peer-reviewed academic covering the study of analysis and decision-making under uncertainty.'),
        })
        self._test_queries('wikiclir/it', count=808605, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('316', 'Academy Award for Best Production Design', 'the recognizes achievement for art direction in film.'),
            808604: WikiClirQuery('54967555', '1999 Merano Open – Doubles', 'lucas arnold ker and jaime oncins win the title by defeating marc-kevin goellner and eric taino 6–4, 7–6 in the final.'),
        })
        self._test_queries('wikiclir/ja', count=426431, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('316', 'Academy Award for Best Production Design', 'the recognizes achievement for art direction in film.'),
            426430: WikiClirQuery('54966134', "Pu'an Signal Station", '() is a railway on the taiwan railways administration (tra) south-link line located in daren township, taitung county, taiwan.'),
        })
        self._test_queries('wikiclir/ko', count=224855, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('316', 'Academy Award for Best Production Design', 'the recognizes achievement for art direction in film.'),
            224854: WikiClirQuery('54965501', 'Lee Yoo-jin (actor)', '(hangul: ; born april 6, 1992) is a south korean actor.'),
        })
        self._test_queries('wikiclir/no', count=299897, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('324', 'Academy Awards', 'the , now known officially as the oscars, is a set of twenty-four for artistic and technical merit in the american film industry, given annually by the of motion picture arts and sciences (ampas), to recognize excellence in cinematic achievements as assessed by the voting membership.'),
            299896: WikiClirQuery('54967547', 'Wassana Panyapuek', '(born 14 december 1968) is a thai sprinter.'),
        })
        self._test_queries('wikiclir/nn', count=99493, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('339', 'Ayn Rand', "(; born alisa zinov'yevna rosenbaum, ; – march 6, 1982) was a russian-american novelist, philosopher, playwright, and screenwriter."),
            99492: WikiClirQuery('54952283', 'Lekamøya', 'is a mountain in the municipality of leka in nord-trøndelag, norway.'),
        })
        self._test_queries('wikiclir/pl', count=693656, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('324', 'Academy Awards', 'the , now known officially as the oscars, is a set of twenty-four for artistic and technical merit in the american film industry, given annually by the of motion picture arts and sciences (ampas), to recognize excellence in cinematic achievements as assessed by the voting membership.'),
            693655: WikiClirQuery('54966439', 'Top fermentation', 'or high is a brewing method for beer whereby the yeast floats on of the wort.'),
        })
        self._test_queries('wikiclir/pt', count=611732, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('324', 'Academy Awards', 'the , now known officially as the oscars, is a set of twenty-four for artistic and technical merit in the american film industry, given annually by the of motion picture arts and sciences (ampas), to recognize excellence in cinematic achievements as assessed by the voting membership.'),
            611731: WikiClirQuery('54964827', 'Monalysa Alcântara', '(born 26 january 1999) is a brazilian model and beauty pageant titleholder who won miss brasil 2017.'),
        })
        self._test_queries('wikiclir/ro', count=199264, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('316', 'Academy Award for Best Production Design', 'the recognizes achievement for art direction in film.'),
            199263: WikiClirQuery('54965687', 'Karl Heinrich Gräffe', '(1799-1873) was a german mathematician, who was professor at university of zurich.'),
        })
        self._test_queries('wikiclir/ru', count=664924, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('324', 'Academy Awards', 'the , now known officially as the oscars, is a set of twenty-four for artistic and technical merit in the american film industry, given annually by the of motion picture arts and sciences (ampas), to recognize excellence in cinematic achievements as assessed by the voting membership.'),
            664923: WikiClirQuery('54966570', 'Nadezhda Babkina', 'georgieva (; born 19 march, 1950, chyorny yar, astrakhan oblast, soviet union) is а soviet and russian folk and pop singer.'),
        })
        self._test_queries('wikiclir/en-simple', count=114572, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('324', 'Academy Awards', 'the , now known officially as the oscars, is a set of twenty-four for artistic and technical merit in the american film industry, given annually by the of motion picture arts and sciences (ampas), to recognize excellence in cinematic achievements as assessed by the voting membership.'),
            114571: WikiClirQuery('54964009', 'Pyotr Deynekin', 'stepanovich (14 december 1937 – 19 august 2017) was a russian military general.'),
        })
        self._test_queries('wikiclir/es', count=781642, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('324', 'Academy Awards', 'the , now known officially as the oscars, is a set of twenty-four for artistic and technical merit in the american film industry, given annually by the of motion picture arts and sciences (ampas), to recognize excellence in cinematic achievements as assessed by the voting membership.'),
            781641: WikiClirQuery('54966770', 'Selene Johnson', '(february 20, 1876-december 11, 1960) was an american stage and silent film actress born in philadelphia, pennsylvania (usa) as knapp johnson.'),
        })
        self._test_queries('wikiclir/sw', count=22860, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('580', 'Astronomer', 'an is a scientist in the field of astronomy who concentrates their studies on a specific question or field outside the scope of earth.'),
            22859: WikiClirQuery('54716724', 'Tirax language', 'is an oceanic spoken in north east malakula, vanuatu.'),
        })
        self._test_queries('wikiclir/sv', count=639073, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('316', 'Academy Award for Best Production Design', 'the recognizes achievement for art direction in film.'),
            639072: WikiClirQuery('54963595', 'Sirkka Selja', '(sirkka-liisa tulonen; 20 march 1920 – 17 august 2017) was a finnish poet and writer.'),
        })
        self._test_queries('wikiclir/tl', count=48930, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('358', 'Algeria', "( '; , '; ), officially the people's democratic republic of algeria, is a sovereign state in north africa on the mediterranean coast."),
            48929: WikiClirQuery('54959191', 'Miho Yoshioka (tarento)', 'she was born from higashiōsaka, osaka prefecture.'),
        })
        self._test_queries('wikiclir/tr', count=185388, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('324', 'Academy Awards', 'the , now known officially as the oscars, is a set of twenty-four for artistic and technical merit in the american film industry, given annually by the of motion picture arts and sciences (ampas), to recognize excellence in cinematic achievements as assessed by the voting membership.'),
            185387: WikiClirQuery('54965031', 'Himmet Karadağ', '(born 1974, denizli, turkey) is a turkish bureaucrat and chairman of borsa istanbul the sole exchange entity of turkey.'),
        })
        self._test_queries('wikiclir/uk', count=348222, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('324', 'Academy Awards', 'the , now known officially as the oscars, is a set of twenty-four for artistic and technical merit in the american film industry, given annually by the of motion picture arts and sciences (ampas), to recognize excellence in cinematic achievements as assessed by the voting membership.'),
            348221: WikiClirQuery('54966570', 'Nadezhda Babkina', 'georgieva (; born 19 march, 1950, chyorny yar, astrakhan oblast, soviet union) is а soviet and russian folk and pop singer.'),
        })
        self._test_queries('wikiclir/vi', count=354312, items={
            0: WikiClirQuery('12', 'Anarchism', 'is a political philosophy that advocates self-governed societies based on voluntary institutions.'),
            9: WikiClirQuery('324', 'Academy Awards', 'the , now known officially as the oscars, is a set of twenty-four for artistic and technical merit in the american film industry, given annually by the of motion picture arts and sciences (ampas), to recognize excellence in cinematic achievements as assessed by the voting membership.'),
            354311: WikiClirQuery('54960571', 'Dictyocaryum lamarckianum', 'is a species of flowering plant in the arecaceae family.'),
        })


    def test_qrels(self):
        self._test_qrels('wikiclir/ar', count=519269, items={
            0: TrecQrel('12', '23571', 2, 'Q0'),
            9: TrecQrel('12', '804785', 1, 'Q0'),
            519268: TrecQrel('54964051', '3769457', 2, 'Q0'),
        })
        self._test_qrels('wikiclir/ca', count=965233, items={
            0: TrecQrel('12', '15902', 2, 'Q0'),
            9: TrecQrel('12', '1010721', 1, 'Q0'),
            965232: TrecQrel('54965687', '1423451', 2, 'Q0'),
        })
        self._test_qrels('wikiclir/zh', count=926130, items={
            0: TrecQrel('12', '87200', 2, 'Q0'),
            9: TrecQrel('12', '16304', 1, 'Q0'),
            926129: TrecQrel('54967133', '5795012', 2, 'Q0'),
        })
        self._test_qrels('wikiclir/cs', count=954370, items={
            0: TrecQrel('12', '12682', 2, 'Q0'),
            9: TrecQrel('12', '430366', 1, 'Q0'),
            954369: TrecQrel('54961893', '2646', 1, 'Q0'),
        })
        self._test_qrels('wikiclir/nl', count=2334644, items={
            0: TrecQrel('12', '11036', 2, 'Q0'),
            9: TrecQrel('12', '134021', 1, 'Q0'),
            2334643: TrecQrel('54967572', '2716534', 2, 'Q0'),
        })
        self._test_qrels('wikiclir/fi', count=939613, items={
            0: TrecQrel('12', '7556', 2, 'Q0'),
            9: TrecQrel('12', '1101970', 1, 'Q0'),
            939612: TrecQrel('54966570', '529972', 2, 'Q0'),
        })
        self._test_qrels('wikiclir/fr', count=5137366, items={
            0: TrecQrel('12', '178', 2, 'Q0'),
            9: TrecQrel('12', '1312543', 1, 'Q0'),
            5137365: TrecQrel('54967313', '6378662', 1, 'Q0'),
        })
        self._test_qrels('wikiclir/de', count=5550454, items={
            0: TrecQrel('12', '24409', 2, 'Q0'),
            9: TrecQrel('12', '3103271', 1, 'Q0'),
            5550453: TrecQrel('54967235', '7427899', 1, 'Q0'),
        })
        self._test_qrels('wikiclir/it', count=3443633, items={
            0: TrecQrel('12', '22305', 2, 'Q0'),
            9: TrecQrel('12', '14627', 1, 'Q0'),
            3443632: TrecQrel('54967555', '3455512', 2, 'Q0'),
        })
        self._test_qrels('wikiclir/ja', count=3338667, items={
            0: TrecQrel('12', '1430709', 2, 'Q0'),
            9: TrecQrel('12', '2963727', 1, 'Q0'),
            3338666: TrecQrel('54966134', '1664146', 1, 'Q0'),
        })
        self._test_qrels('wikiclir/ko', count=568205, items={
            0: TrecQrel('12', '10071', 2, 'Q0'),
            9: TrecQrel('12', '86969', 1, 'Q0'),
            568204: TrecQrel('54965501', '1824430', 1, 'Q0'),
        })
        self._test_qrels('wikiclir/no', count=963514, items={
            0: TrecQrel('12', '31', 2, 'Q0'),
            9: TrecQrel('12', '285079', 1, 'Q0'),
            963513: TrecQrel('54967547', '1387292', 2, 'Q0'),
        })
        self._test_qrels('wikiclir/nn', count=250141, items={
            0: TrecQrel('12', '10770', 2, 'Q0'),
            9: TrecQrel('12', '130318', 1, 'Q0'),
            250140: TrecQrel('54952283', '2757', 1, 'Q0'),
        })
        self._test_qrels('wikiclir/pl', count=2471360, items={
            0: TrecQrel('12', '25', 2, 'Q0'),
            9: TrecQrel('12', '14226', 1, 'Q0'),
            2471359: TrecQrel('54966439', '1937710', 1, 'Q0'),
        })
        self._test_qrels('wikiclir/pt', count=1741889, items={
            0: TrecQrel('12', '230', 2, 'Q0'),
            9: TrecQrel('12', '2121768', 1, 'Q0'),
            1741888: TrecQrel('54964827', '1311522', 1, 'Q0'),
        })
        self._test_qrels('wikiclir/ro', count=451180, items={
            0: TrecQrel('12', '23210', 2, 'Q0'),
            9: TrecQrel('12', '226810', 1, 'Q0'),
            451179: TrecQrel('54965687', '1736377', 2, 'Q0'),
        })
        self._test_qrels('wikiclir/ru', count=2321384, items={
            0: TrecQrel('12', '3021', 2, 'Q0'),
            9: TrecQrel('12', '2051069', 1, 'Q0'),
            2321383: TrecQrel('54966570', '3117631', 1, 'Q0'),
        })
        self._test_qrels('wikiclir/en-simple', count=250380, items={
            0: TrecQrel('12', '4807', 2, 'Q0'),
            9: TrecQrel('25', '46790', 1, 'Q0'),
            250379: TrecQrel('54964009', '594669', 2, 'Q0'),
        })
        self._test_qrels('wikiclir/es', count=2894807, items={
            0: TrecQrel('12', '2190809', 2, 'Q0'),
            9: TrecQrel('12', '221716', 1, 'Q0'),
            2894806: TrecQrel('54966770', '8045048', 2, 'Q0'),
        })
        self._test_qrels('wikiclir/sw', count=57924, items={
            0: TrecQrel('12', '16420', 2, 'Q0'),
            9: TrecQrel('303', '6834', 1, 'Q0'),
            57923: TrecQrel('54716724', '74685', 2, 'Q0'),
        })
        self._test_qrels('wikiclir/sv', count=2069453, items={
            0: TrecQrel('12', '149', 2, 'Q0'),
            9: TrecQrel('12', '79772', 1, 'Q0'),
            2069452: TrecQrel('54963595', '263597', 1, 'Q0'),
        })
        self._test_qrels('wikiclir/tl', count=72359, items={
            0: TrecQrel('12', '87382', 2, 'Q0'),
            9: TrecQrel('305', '202908', 1, 'Q0'),
            72358: TrecQrel('54959191', '155814', 2, 'Q0'),
        })
        self._test_qrels('wikiclir/tr', count=380651, items={
            0: TrecQrel('12', '21889', 2, 'Q0'),
            9: TrecQrel('12', '54359', 1, 'Q0'),
            380650: TrecQrel('54965031', '2098262', 2, 'Q0'),
        })
        self._test_qrels('wikiclir/uk', count=913358, items={
            0: TrecQrel('12', '12101', 2, 'Q0'),
            9: TrecQrel('12', '1370301', 1, 'Q0'),
            913357: TrecQrel('54966570', '2004654', 1, 'Q0'),
        })
        self._test_qrels('wikiclir/vi', count=611355, items={
            0: TrecQrel('12', '307178', 2, 'Q0'),
            9: TrecQrel('303', '33804', 2, 'Q0'),
            611354: TrecQrel('54960571', '2174311', 2, 'Q0'),
        })



if __name__ == '__main__':
    unittest.main()
