import re
import unittest
from ir_datasets.datasets.beir import BeirTitleDoc, BeirTitleUrlDoc, BeirSciDoc, BeirToucheDoc, BeirCordDoc, BeirCqaDoc, BeirCqaQuery, BeirToucheQuery, BeirCovidQuery, BeirUrlQuery, BeirSciQuery
from ir_datasets.formats import TrecQrel, GenericDoc, GenericQuery
from .base import DatasetIntegrationTest


class TestBeir(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('beir/arguana', count=8674, items={
            0: BeirTitleDoc('test-environment-aeghhgwpe-pro02b', re.compile('^You don’t have to be vegetarian to be green\\. Many special environments have been created by livestoc.{1667}, 12 October 2010  \\[2\\] Lucy Siegle, ‘It is time to become a vegetarian\\?’ The Observer, 18th May 2008$', flags=48), 'animals environment general health health general weight philosophy ethics'),
            9: BeirTitleDoc('test-environment-aeghhgwpe-con01b', re.compile('^Human evolved as omnivores over thousands of years\\. Yet since the invention of farming there is no l.{283} over to farming we have get our food from the most efficient sources, which means being vegetarian\\.$', flags=48), 'animals environment general health health general weight philosophy ethics'),
            8673: BeirTitleDoc('validation-society-fyhwscdcj-con02b', re.compile('^Many of the organisations that run child sponsorship schemes are dedicated to improving all of these.{594} encourage the sponsoring of children to build for a better future alongside other charity projects\\.$', flags=48), ''),
        })
        self._test_docs('beir/climate-fever', count=5416593, items={
            0: BeirTitleDoc('1928_in_association_football', 'The following are the football ( soccer ) events of the year 1928 throughout the world .', '1928 in association football'),
            9: BeirTitleDoc('1998_All-Ireland_Senior_Hurling_Championship', re.compile('^The All\\-Ireland Senior Hurling Championship of 1998 \\( known for sponsorship reasons as the Guinness .{91} \\. Offaly won the championship , beating Kilkenny 2\\-16 to 1\\-13 in the final at Croke Park , Dublin \\.$', flags=48), '1998 All-Ireland Senior Hurling Championship'),
            5416592: BeirTitleDoc('NW_Rota-1', re.compile('^NW Rota\\-1 is a seamount in the Mariana Islands, northwest of Rota, which was discovered through its .{1135}many animals, although the unstable environment from the frequent eruptions limits animal diversity\\.$', flags=48), 'NW Rota-1'),
        })
        self._test_docs('beir/dbpedia-entity', count=4635922, items={
            0: BeirTitleUrlDoc('<dbpedia:Animalia_(book)>', re.compile("^Animalia is an illustrated children's book by Graeme Base\\. It was originally published in 1986, foll.{136}al numbered and signed anniversary edition was also published in 1996, with an embossed gold jacket\\.$", flags=48), 'Animalia (book)', '<http://dbpedia.org/resource/Animalia_(book)>'),
            9: BeirTitleUrlDoc('<dbpedia:Alkane>', re.compile('^In organic chemistry, an alkane, or paraffin \\(a historical name that also has other meanings\\), is a .{191}cal formula CnH2n\\+2\\. For example, Methane is CH4, in which n=1 \\(n being the number of Carbon atoms\\)\\.$', flags=48), 'Alkane', '<http://dbpedia.org/resource/Alkane>'),
            4635921: BeirTitleUrlDoc('<dbpedia:Frankfurt>', re.compile('^Frankfurt am Main \\(German pronunciation: \\[ˈfʁaŋkfʊɐ̯t am ˈmaɪ̯n\\] \\) is the largest city in the German.{400}t of the European Union in 2013, the geographic centre of the EU is about 40 km \\(25 mi\\) to the east\\.$', flags=48), 'Frankfurt', '<http://dbpedia.org/resource/Frankfurt>'),
        })
        self._test_docs('beir/fever', count=5416568, items={
            0: BeirTitleDoc('1928_in_association_football', 'The following are the football ( soccer ) events of the year 1928 throughout the world .', '1928 in association football'),
            9: BeirTitleDoc('1998_All-Ireland_Senior_Hurling_Championship', re.compile('^The All\\-Ireland Senior Hurling Championship of 1998 \\( known for sponsorship reasons as the Guinness .{91} \\. Offaly won the championship , beating Kilkenny 2\\-16 to 1\\-13 in the final at Croke Park , Dublin \\.$', flags=48), '1998 All-Ireland Senior Hurling Championship'),
            5416567: BeirTitleDoc('Raúl_Castro', re.compile('^Raúl Modesto Castro Ruz \\(; American Spanish: \\[raˈul moˈðesto ˈkastɾo ˈrus\\]; born 3 June 1931\\) is a C.{1534}ighth Congress of the Communist Party of Cuba, which is scheduled to take place 16 to 19 April 2021\\.$', flags=48), 'Raúl Castro'),
        })
        self._test_docs('beir/fiqa', count=57638, items={
            0: GenericDoc('3', re.compile("^I'm not saying I don't like the idea of on\\-the\\-job training too, but you can't expect the company to.{260}g out with thousands in student debt and then complaining that they aren't qualified to do anything\\.$", flags=48)),
            9: GenericDoc('138', re.compile('^So you asked him in 2010 how he was gong to compete with DVD rental distributors like Netflix \\(which.{103}y were going to continue to compete as a DVD rental distributor just like the mentioned competitors\\?$', flags=48)),
            57637: GenericDoc('599987', re.compile("^Giving the government more control over the distribution of goods and services, even more than it ha.{165}ply aren't competitive\\.  https://www\\.thelocal\\.dk/20170829/denmarks\\-government\\-announces\\-new\\-tax\\-plan$", flags=48)),
        })
        self._test_docs('beir/hotpotqa', count=5233329, items={
            0: BeirTitleUrlDoc('12', re.compile('^Anarchism is a political philosophy that advocates self\\-governed societies based on voluntary instit.{149}ierarchical free associations\\. Anarchism holds the state to be undesirable, unnecessary and harmful\\.$', flags=48), 'Anarchism', 'https://en.wikipedia.org/wiki?curid=12'),
            9: BeirTitleUrlDoc('316', re.compile('^The Academy Award for Best Production Design recognizes achievement for art direction in film\\. The c.{280} the award is shared with the set decorator\\(s\\)\\. It is awarded to the best interior design in a film\\.$', flags=48), 'Academy Award for Best Production Design', 'https://en.wikipedia.org/wiki?curid=316'),
            5233328: BeirTitleUrlDoc('55408517', "Wilfrid Tatham (12 December 1898 – 26 July 1978) was a British hurdler. He competed in the men's 400 metres hurdles at the 1924 Summer Olympics.", 'Wilfrid Tatham', 'https://en.wikipedia.org/wiki?curid=55408517'),
        })
        # NOTE: Beir doesn't handle the encoding properly, so it differs from msmarco-passage. However, we do not correct here so that these benchmarks are identical with the Beir suite
        self._test_docs('beir/msmarco', count=8841823, items={
            0: GenericDoc('0', re.compile('^The presence of communication amid scientific minds was equally important to the success of the Manh.{125}nd engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated\\.$', flags=48)),
            9: GenericDoc('9', re.compile("^One of the main reasons Hanford was selected as a site for the Manhattan Project's B Reactor was its.{13} the Columbia River, the largest river flowing into the Pacific Ocean from the North American coast\\.$", flags=48)),
            99: GenericDoc('99', re.compile("^\\(1841 \\- 1904\\) Contrary to legend, AntonÃ\xadn DvoÅ\x99Ã¡k \\(September 8, 1841 \\- May 1, 1904\\) was not born i.{120} in the way of his son's pursuit of a musical career, he and his wife positively encouraged the boy\\.$", flags=48)),
            #                                                                         Antonín     Dvořák
            243: GenericDoc('243', re.compile('^John Maynard Keynes, 1st Baron Keynes, CB, FBA \\(/Ë\x88keÉªnz/ KAYNZ; 5 June 1883 â\x80\x93 21 April 1946\\), wa.{46}y changed the theory and practice of modern macroeconomics and the economic policies of governments\\.$', flags=48)),
            #                                                                                       /ˈkeɪnz/
            1004772: GenericDoc('1004772', re.compile('^Jordan B Peterson added, Jason Belich ð\x9f\x87ºð\x9f\x87¸ @JasonBelich\\. Replying to @JasonBelich @jordanbpeters.{24}for anybody with the authority to deploy code to slip a bit of code to enforce a grey list of sorts\\.$', flags=48)),
            #                                                                                 🇺🇸
            1032614: GenericDoc('1032614', re.compile('^The CLP Group \\(Chinese: ä¸\xadé\x9b»é\x9b\x86å\x9c\x98\\) and its holding company, CLP Holdings Ltd \\(SEHK: 0002\\) \\(Chines.{290}any Syndicate, its core business remains the generation, transmission, and retailing of electricity\\.$', flags=48)),
            #                                                                     中電集團
            1038932: GenericDoc('1038932', re.compile('^Insulin\\-naÃ¯ve with type 1 diabetes: Initially â\x85\x93â\x80\x93Â½ of total daily insulin dose\\. Give remainder .{115}tially 0\\.2 Units/kg once daily\\. May need to adjust dose of other co\\-administered antidiabetic drugs\\.$', flags=48)),
            #                                                     naïve                                  ⅓–½
            8841822: GenericDoc('8841822', re.compile('^View full size image\\. Behind the scenes of the dazzling light shows that spectators ooh and ahh at o.{266}h special chemicals, mainly metal salts and metal oxides, which react to produce an array of colors\\.$', flags=48)),
        })
        self._test_docs('beir/nfcorpus', count=3633, items={
            0: BeirTitleUrlDoc('MED-10', re.compile('^Recent studies have suggested that statins, an established drug group in the prevention of cardiovas.{1524}evaluated further in a clinical trial testing statins’ effect on survival in breast cancer patients\\.$', flags=48), 'Statin Use and Breast Cancer Survival: A Nationwide Cohort Study from Finland', 'http://www.ncbi.nlm.nih.gov/pubmed/25329299'),
            9: BeirTitleUrlDoc('MED-335', re.compile('^OBJECTIVE: Meat and milk products are important sources of dietary phosphorus \\(P\\) and protein\\. The u.{1495}s\\. Copyright © 2012 National Kidney Foundation, Inc\\. Published by Elsevier Inc\\. All rights reserved\\.$', flags=48), 'Differences among total and in vitro digestible phosphorus content of meat and milk products.', 'http://www.ncbi.nlm.nih.gov/pubmed/21978846'),
            3632: BeirTitleUrlDoc('MED-961', re.compile('^BACKGROUND: Current unitage for the calciferols suggests that equimolar quantities of vitamins D\\(2\\) .{1382}cy and lower cost, D3 should be the preferred treatment option when correcting vitamin D deficiency\\.$', flags=48), 'Vitamin D(3) is more potent than vitamin D(2) in humans.', 'http://www.ncbi.nlm.nih.gov/pubmed/21177785'),
        })
        self._test_docs('beir/nq', count=2681468, items={
            0: BeirTitleDoc('doc0', re.compile('^In accounting, minority interest \\(or non\\-controlling interest\\) is the portion of a subsidiary corpor.{151}of outstanding shares, or the corporation would generally cease to be a subsidiary of the parent\\.\\[1\\]$', flags=48), 'Minority interest'),
            9: BeirTitleDoc('doc9', re.compile("^Hermann is rushed to Chicago Med after being stabbed at Molly's\\. After losing a lot a blood, it is d.{172}lli grows more concerned about Chili's erratic behavior\\. Mouch considers finally proposing to Platt\\.$", flags=48), 'Chicago Fire (season 4)'),
            2681467: BeirTitleDoc('doc2681467', 'Rookies in italics', '1990 New England Patriots season'),
        })
        self._test_docs('beir/quora', count=522931, items={
            0: GenericDoc('1', 'What is the step by step guide to invest in share market in india?'),
            9: GenericDoc('10', 'Which fish would survive in salt water?'),
            522930: GenericDoc('537933', 'What is it like to have sex with your cousin?'),
        })
        self._test_docs('beir/scidocs', count=25657, items={
            0: BeirSciDoc('632589828c8b9fca2c3a59e97451fde8fa7d188d', re.compile('^An evolutionary recurrent network which automates the design of recurrent neural/fuzzy networks usin.{1388}pared to both GA and PSO in these recurrent networks design problems, demonstrating its superiority\\.$', flags=48), 'A hybrid of genetic algorithm and particle swarm optimization for recurrent network design', ['1725986'], 2004, ['93e1026dd5244e45f6f9ec9e35e9de327b48e4b0', '870cb11115c8679c7e34f4f2ed5f469badedee37', '7ee0b2517cbda449d73bacf83c9bb2c96e816da7', '97ca96b2a60b097bc8e331e526a62c6ce3bb001c', 'f7d4fcd561eda6ce19df70e02b506e3201aa4aa7', '772f83c311649ad3ca2baf1c7c4de4610315a077', '0719495764d98886d2436c5f5a6f992104887160', 'a1aa248db86001ea5b68fcf22fa4dc01016442f8', 'a1877adad3b8ca7ca1d4d2344578235754b365b8', '8aedb834e973a3b69d9dae951cb47227f9296503', '1e5048d87fd4c34f121433e1183d3715217f4ab4', 'b1c411363aded4f1098572f8d15941337310ca15', '05bd67f3c33d711f5e8e1f95b0b82bab45a34095', 'f59f50a53d81f418359205c814f098be5fa7655a', '8cc9fa42cb88f0307da562bb7a8104cb2ed4474c', 'c26229b43496b2fe0fa6a81da69928b378092d4d', 'fe49526fef68e26217022fc56e043b278aee8446', 'c471da1875ad3e038469880b5f8321fb15364502', 'a2f65aae36fee93adf4e32589816b386bd0121cf', '97d58db3c8d08ba6b28fcb7b87031222b077669a', '3bb96f380b213d3b597722bf6ce184ff01299e14', '2450a56cfa19bb75fdca9bb80326502cf999f503', 'aacb4c8cbb3ebeba8045169333d9915954bfc9e0', '21bf7734d99d9967a92f23ded5c97a8638defabb', '6c80c53474a48d3a9bfdab25c6771cdc32fc754e', '1e4aebb032a75b186f6bc80d3ec72ce415d2c509', '278a2bcb2bfdf735f33bcd3423f75160fa349816', '5a6cf5c1cf29b080ed49707961c760bf4f68031f', 'cd22b27e2f094ac899b3f4795db0fd59d90ec4ef', 'ee05187997dcb548b86ab25e25a19a2eaeae46f8', 'd9a2c54ec3aaaea66cef9a664b704a056498d958', '41a5d7d783e7776715543a80f1dea31c2a6a416d', 'c91d076423d20939df90447c17f7995ad48af5c2', '115ab3aa4915185549dcb488a432934bc6e9602a', 'd3c27966e7ff87ea64f8e7644964d5d210bb4bd0', '239eb7a57f4dbf67da36d0c0ab2bc9ed7b2da740', 'b738cd6aeb90fcc4acae1811adb7bb569b198f26', '6cbc15829a4c16189f1871b7fdb5ca850555ec5f', '9c244015d82b2911bcfa74ca68555db4660bda49', 'd2a2add50f11f8c5be0db504509e1acfad435817', '0f7606f0f386e860db2b6ef97f4c71f4ae205646', '4e5da9e9bc3695609fea69ce04f147c7096ade8d', '5c01686a41c31a6b7a9077edb323ed88cf158a98', 'bafa852ed1764321494cdbe4cad97d022cbf24de', '2c7434dc50df0b4adb11e52fb6c3c1dd816dee88', 'a2d7c5237e7e0f3ed63d04a10ecd33a2e289c0c6', '644bcc8870ca92db212ab96640c98b26cf4708b0', 'bc7355ebb81756a284aa3489edca2da2f67e8be2', 'e7508004e13b0f2d3d0c3b07f4f967b38561096e', '1143e42cd4fcd8f2564834138c99555cfbff20fc', '2838302385c5f2338212e81962485c7bfb52bb15', '302e4b35e9c55c367de957d99c53567cd4f9af40', 'a468e406d170f802dafba994dbe9950c244c7320', 'd600ce2e7676b6d5d97be126014faceca3650408', 'ca10ca753aff094b91f51785dbe7b387e1c50275', '5d05a1ec8ae2cf34ce2ffd9efa07c6e5d39136ca', '1ce9cb252c3c3c4083cdd8c51f24ee1eb3a7cb17', '8f75355add4f9520fc4ffcf525419c5a299814db', '23a400d9b5a70223bf15cf0438d3408d8923ef1e', '7cbb016c73eb05f5a2b3996af687a0a2681fed97', 'c36efedbc8c0aaafaa32e42d93dfe6352c1f99ab', 'b9190fa1b349435a0532c7cb6a29bb26a7f7c78c', '3e4e7eab7bf967c2fce4c4af41e212f9aa26af87', 'fdf8da6cad2e443280845663f2fd5211fa2d5316', '9fcfc1ea5a4171cc7d1e6de3931999242f8a0ecf', '4339003eae685c293426398e801ee7e79f5416e2', '7c55fe6aa32bab5f61cd16df4d8156a0aad47742', 'bbd0c02b60a5737f49b019606f1d8adfc8eb4706', 'bad5e3c5fc9da04968790f1aa4166aa570511454', '7e720d2daa6a72b02f04091b14ffa74b5f9d6755', 'b406feee35d476c4aa516bff4ecc5c4c6ff6c353', 'fdece23a929504045b87fac8ff2c490110d1d624', 'cd887424b25d9036cbfb817fa57a3c509297a82b', 'f0b9dc64f6df004d3f776031050317f0a7fb1bdc', 'd71e30919f03df92d76bd3bb8d113f1df6b03710', '93336ce843d4f83e8c22e02494880398c210908c', '3e4be8785a1ad34c60356385a5a7417f7f2d6699', 'bcb7894325606c9765810563e89ad4fc275ae010', 'da10f1c9b5a6253ffbb9c8e933993b773e49a188', '106b8c7a4053d77ccca319a7dd4b054f60cb4026', 'e233b5c4b8ea9e6daebee83b956bffbdca2d08c2', '59066ff5d305249658150618dbebe7ab21ae82ea', '3e769b6dd0b0fb3393dc894752b0afffd8d2d064', 'd127160840debb1a7edf38ac5cf02914fb2f8a59', 'b8485fe3b70588697cad5f46726ce18ca8afb77e', 'f0ebee9b612d517fc3831cd45abd503539e25085', '381323297bd2017330fb53c2d81b2802cd7caf88', 'd6e99b3ba3d2d8c00c50a154a3e5171f99ae2c85', '03576d44433ecb04e7d87b526fe8238a4ae6d15f', '09b48c8502fa63183b39cd725ecebb634f83c037', 'b2409618be98dca139d2b1a326c9f47e279bd600', '8d0d940113d281ae522a72662fef3d6c40f9d6cb', 'eeca94cc6cffe49537533ee37fd7ba5d18e70386', 'ef8ff6394a9e463acfd4ce2784c68e2f92a55e17', '14d05578d0b6b71c2f023b71e9fe71a6b44430da', 'ae8440cdf5ecbb8dfd00df723f92c34d21fcabb3', 'cf6a72d1eb2011d5a2b57d58c4ad9bd3751c3443', '4c3c3bf48d48b3ceee50a9b463b80ab9834aea68', '7720e805627d1f5d6c992fcdb0f2bbc31e133284', '2e440ff1540094c0608c200eacfd5573b424391e', '919955f6b198260e4d889a8f6ac55feac3f20ac7', '88f0d71a63552a7cee72f7e8e18588d0776f6d8f', 'b4f7477fb596e4933323f153fb1e287bef8b1335', 'b52c1f8077090c660ab4a22f47e7f8483b4bb7cf', '7486b9ceadd64e496470e75b315eac543aec7f2e', '5d068f5e3dd9813c3b108f6eb08dc436a134a218', 'f36bb6f93f3045d3bae924411ec7a07be0a49c6f', '3dbb1997727ad478b1e2a6c8c27386cc92fccf9d', 'eec9b96d796a8a4b00af1b3e2ced301dd4607312', '5ee347218ba58940df02c35fd7fcf1795ff477a7', 'd5a8509199984393f728f4268acfee97a8ba4ff6', 'c1220fd757136150ee5f55e83b12cdcb4302048b', '4c422a7012858a200af4967b11da8f9a457ecad8', '44e6a642e05f9c11f2df02133d9b57d2a4b30d50', '02a4293cb083dd54a0d685dd4fa25ce165850557', '6222bfa3e282948e250a8688027079380012b2cb', 'ee666f494724f4ef3949a5532d59b207bb42de6b', '14e21725a5e25978b25ec4e27dc190d2e2ec542e', 'e37b6d3840d037f5536404fe65c81373c2574d66', 'bdce2c4a25826d3242e04f993af16c10845fc78e', '92ec3f0e7d0cd30a68b901878b3d27a752a6705f', '85c45753caf576297cc43dd285ca4d78f230dde1', '1c8bc4fe215841be7290956ea96502a7e494e76e', 'b0574087d5b0b4d4ed4819bdd590a1b7c2024802', 'aca085cef4b6c042886cb608bba803036b704000', '4da844e414c5ddc177ecd4bcd73ed975ea8cfd23', '924c6135ca4faa043fb8e0edce850630863a0302', 'b72c6b29224871d4ced87289a225b1ed47cbc6fe', 'f42b9bb8e50573c66e9b9166102989dabc76fe7a', 'e4a2496ebd30c6f2b882be322eb2aba9de3cd15b', '294ca24667d7c88231492ab3bc9167a4ad958456', 'ec658577c3612814cf1a2a0f7d55457f42e788e2', '368eea6972bf8140324e5c22684cb60b52a7c35c', '14e9aa7473b18aa729315939cb5b1e427275cd00', '9316a26093d195273702effdd9502a077f1b4dcd', '5bc936d39907e99068ba1d07f3fa8883ffa0ca74', 'a1f4ea8b9f875567e71f71f00bb3b0168642c91d', '21229ea3dc420e19b3b3b57345dcc2a5d6bea98b', '04257c2d45c025d1bac07119af163dad7b1aebd1', '27d6cff7c45020b673790d94f92a58ae1880027b', '634cbffc7aac82a1a7843db2d9c1bc5a351f6849', '866696dd758d404291195c651e254e428b6a4be8', 'd24ecc520e2d4bd7980dc5bce547791688a71f72', 'd8869d381387be11924a04b4525f9a408ef37cdb', '77db5dbfb33c0919f60c49bb5ddb99861ffef474', '0b5da93fe58dfc004d3569998e00f979766af658', 'a90b2fba001ea50302b8ac1023a06ffcbfd8f7bc', 'c5ab88898f33f2a37e4a6ab7563f562264c47854', 'd5708326c5e682d933ae1539b424aee93d6b7188', 'b2d146286843822549fa46ef0e5263ff5b8ef436', 'd66c46ae198aec982bb9e98762b39c9bb11bf6ca', '1f7c71361c065d2a0586be3868260a81122648d3', 'd76e965a8a5fb9139e306ea5052896f5816358a9', '9bdac06d5b9a9804f3f5ba029aaf3a974ce831c7', '284f78d2b7fb96868e1ce7cd4ed02321c450ef68', 'fe7fdc4dbd9d45487ab6c69caeb6182a69ca2019', 'eb21adaf5017ddc593cf9a3f252adf31ee240645', '1aa9501d7ec7084f8500864b3ff808100c8045be', '250019746203612925abbe02d83589c3738d3982', '60127d1a428037c1835292ddaa3dbca95fd12ab7', '518158093bc0234a8a7c9657cc03b79483c21a76', '41251b17e995217b2417585e2a44cdc07789a0f4', '014951e19c98ce58a2607fc12df411bacb982d3e', 'aeb6458d44cd2fef802fa5a9f59f87d62e0c02c0', '1c7b5df940825c71c97efa97e309bedd89562635', '4fb91b679f5e987ca6c3a9948f0985a52e9014e4', 'faff869e53b2f379ab23afdc01292a300132edba', '3fa12b2e36163600f62bc8fcb4946ad734cbdc00', 'ee12be5cc21a34d28f5a98b68d0ccc5c416caa12', 'ba91b468f06275fb4b7882421efcd8070aaeec07', 'e8309b9b44361c03231a62bf59d2587185a7e81c', 'eb7f5ffb624b9e82a825b27e1a3c7d23e2527a35', '468a2bb7a12fdd1dcef61a3194070d7d9a644fdb', '6983da35d34d9456bc6184e36d92426c5a117e97', '969b3eb194dbff25b137f29cb9a015dc38b6a2ac', '321e868c86d2a3c86003ac7aebe374c1eab25b81', 'a301b95dccd5a8fc6362e475af50182bf6a1caa7', '9396fe8096d937bbd482cdafe29e5c4e1751fd06', 'a9ba36a7b0bc90a9b85574c1c597805f05771e6a', '7184e02e6ccfe08ca3ef7c3b16be2023b6be0e24', '25917acd4d1e96faae4398452eeca743dd64b2d8', 'd0fae819552bd425bfc3d780429bbb7b8d7a4d0b', '410316db0cdb9001e76daf3e2a27ccd3c6156042', 'ce49a2c822c9a27efc00cc7aae022e8d1aefa982', '78d2f29d9b5af247250e04ae1e686b0c6886b2b1', '94ea5678154d34b270133ade5265fe21a551e2cf', '38e58fce0b460951ee28a2162fcfa7d2847f4ce6', '7783feae9f5f2abd5bf1584b98a5707c519d4769', 'bb7ecf6bfe1776320f4e7d68c56435573aa5eef4', '3f7eb16d88d60db473f703fb8137972293b6eaee', '55f923c75fb5344d4df8b3fd12e16bcc49db7372', 'ece8d11971adecfb15c81ccf4e0c5b2b48d10649', '0d616bc6963f3241bb2c417b4a584c0dcaee7125', '396fc63019a3dd0e550f6eecb0bfd1c34601aa22', '592623484c2d4a482b4841100eb2aaaf6fc85ead', '15667d08f9a2c4bceb5d6e8d3a368dcbdde75bd5', 'c30bd16c400bee7a51d7ce9aad20560d01e28ba0', 'b5a5534e0d3104a634f61e47773801649bb277d5', 'cca7f73fea0ad9d96622509f5428ed8410421948', '17b3bc528440771b104d3df884dffa417f61000f', 'fcea8027dfc0ca7b8871084ad55c46f09615df22', '410ed30d7eb5fbf18764d15089d15c2f68896727', 'f4ff2c3a64c8e094d46bd4ed89e9b02897b9937f', 'dcf92a2d6e5d96678031f313b8a78b6d8e4fdd3e', '50c079e60bbd843d32e46cd1b9aa7f64daa5b8ac', 'aa9d4403443abe31b7c828ee6df0b21c155f3dfd', '9d7ea82ffc353f5be53e245981bdf6c0e8e93839', '6098353c87dea12e0a5881f66ccf738face30d7a', 'b91b62138e674d8e35edd564d785550775d2c745', '0e31b1eca8f03d8b0cf561fb0f76835f7ee7f91a', '6b1ee3d9df1356725cfc04a6316725415e925fab', '1b1de1c77f7aa95eaeafe3bd0e8fc681c13e5a49', 'cb2db48e636a4d871185d832074f68449f424a59', 'fee8292e18978a34260e4a500ccdef9f1994a536', 'f276c78c60e04ba9b0329be8446faea57366a236', '41a0d41f46f90c017b539acd752674970b54ae09', 'a6c0d67613e238e73ebfcb1f9b90d7e248a89a45', '91df33c9139df01b8b42b9650d8690b6bcb76bb8', '269ff07c14a212d4f3f7711fcb1ccf5ce1b9450f', '2aa083a360f5a1d8b598b63d098f8c1b19e428af', '4d6c669dbfd103083f96d2e8a893a31738ab235e', '79262d5d32c1c7de6839dee1d848121d92d96c37', '6b8d37fd28d1dbde8cab08e032eb3830e994f8de', 'bc4e5593972e8cf713535b0fdb6acb1f5cdf93b5', 'cc041ff04e40f09cec667065ac30b9ac3ce2f3a7', '1c312bff25f99daf788c9b8a6db902baeb3dd5f7', '5762d52d8230068883364b93dffbca73c809e49d', '8a19a2bcdcb16929da444ae71631e258fe0b4bfb', 'ea5a76e30fab975e2549bc798bfd2c9bada3e33d', '9a872f7533e423a59aacdb54fe6139fdbb4e7cde', '7d88721118e3fcad46bcc943105c2e9f478d5fa4', '05c4d2ebc8a5bdfb32714fdb1950616891074b18', '126df9f24e29feee6e49e135da102fbbd9154a48', '405dfaad697076f3ca61b50d48db45722bc3b503', '23aa5210aa633ec78e3ce1823cb9cacb18ff7124', '6fd38c3d2f0a455c1223a47280485b26c0fb9b65', '9155a25fa50c2df0fc4e155f8f1e3fd8679ae4bd', '29c18531bdba93cbef2c431de4047c182b642694', 'e01d12ea29720c96003f59ec74cd56b70571bc42', 'abea867b5328f52c0c6beb33f60cb4876ea14595', 'a333ac76e4b2041ac684913b23a8cd719fd46445', 'd6ebe01eb11e9211760a220466238c368f712474', 'e5f06f97eda1af0488eaf495db763f4044a52769', '95eecfd28ea0f3441d838a50c5bfbedd3550070a', 'a5c60b655425ec47bd1119b6ed33edc96071f10a', '04f1a046c66f87073660a839930f1cb6200886aa', 'b43b67fafeaf88eda5673504063188a02d4e1b45', 'ca2885d3eba2983b82345e9362adeecac63f3ae5', '8d677c93182b8163e8bf8d6004906c79c1c06b70', '957d4b3ad38c69ee0b231b385421fac362ca5d65', '3560b2618e9c64cfd587b87966d9f19ff659a148', '8aa041e9e43afdc538647a3f68305538cb321003', 'bb6694528752e33fc74a6ad2d28ef9c1a7b8d750', '1c21e5c66cceb27d56f5823934a0fafa0157b231', '8d3777856af3010c935730ff5b3f482f259f0e74', '653818a8f78fa6b45d7d3fd89af84daa96eca38b', 'eeaf75f79f237b450976b6305724c72400c84095', '7b72aa337f1e5019ba47d92c26917062605fb6d3', '5ac98eb478b8c7b1b20bf7dde0be6ecea38f82e3', '78495383450e02c5fe817e408726134b3084905d', 'ecc84e3ee9348fb182b282a848159a24423efdbe', 'dd4041c26c4d50a7966697819a2bb1ce0e4d1783', 'db38399a4851e72187322ae7f7f9faa3f2eb69d1', '4068609212f024a8cf3c5b7bc755d415f269ce74', 'ae2a4c6f2ad099cd8f5eb66f1935cc1777244bf2', 'ed073c437190660927387d93f41b6db3b2684311', '5bf7cc6483fd054d0ad7f3a37eda94db4cbb6e58', 'c0c00fd1934b61b39ecbbcde007eb919d7a59bd4', '0638b5dd509cd4f6d68d10557967e7b66a741852', 'b88931cf54a7fc51e1b09fa3fb99ecd6cdf41d6b', 'cb9f43961f4b7033d3730e258b14bd5f59f242aa', 'a99f04d7250b5b5c29e3cc28a6c81dd0eebeecd8', '64835ff8ae3412811c182771e30fd33841cc92a1', '15b991b966a9b446705f4484e3380702121e470e', '2b497978211c471e4a5586a2d99dfd087b533b4a', '9313677ca439b3c63a591786d2ec4b3a192aa32f', '7e5110d13ce5a393977c1b4aca7c2a7c06680392', 'b04645579b5be335ad0e107e248b3c1885b9168d', 'b0ead0e018797a68bde2a5cef44926dc5dd8a27c', '1a93897ed610235bbd42debeba79d7cd3d37a28d', '01924bb7ce5da3457c4a20006e2dc2b92af72434', '07bbf1e69528718397e5ebf9a2101b8d9b320743', '5f074195e88c9a6da54602ccb5d7a755668f055c', 'df207c92b2196461da35d5fa69f0968b339709e3', '9bd81c4e318cb217b29f7c381def34f1d7454ecf', '850793dbb35ce40ba591eaae09f59b06ea27f4b7', '8eb24b298856bbe5b13590f185152a3af198bd2e', '353bf82f2ffa870473a814155b4214f93af41bdd', 'b4ded7e3a94e831dd807f0078ca2bad8598c8578', 'd7e7a08814f2d6456690334c04c832a117d35b2b', '6b8f96342eaafafbb4c87e7e6913aea2f9a663f4', '729c81595b1a792d08c407fe5d57826020837a53', '97e1cf63054283e8df52425cacd22eca1eb53499', '392ce361c5b9d0d948baf2a4010676dcf592ee7b', '6c60baf027de044cd8236deff23cdce78b525361', 'a10c03e8084116c112a954da9ed8ae59c426e356', '842ee396e6997b5036fb4cf0bdea527bd37aefd0', '41de39e0cfa898bae4a977f45408ce19dab329de', '21f427b1b38c45e499480f11856b9fe30615eec2', 'f80d7a7f97258d7642491434785db78c195c6f84', '807c519bb57af7329db35dce846849c900b7097a', '87b39a2592f2b96f8366f7467beaa406434dc134', '198e0e490533df571b9e6606c5b8a6e54afe2065', 'e3699b84cce7e3e9bc26f6450dd261d29721f00c', '9ad6f491f498a1c503c5f80a8acdd03156af1429', 'f64a08b8937bb243219905083f6396f11e33654e', '7f0264c0e4abb2d6701ea5006bc33a8e5b1b569f', '3ec328171fc900fca1d324ce25f238718cf7893c', '2c13925dce07b0b829ef87a95beb1942fe1f0e1e', 'ad64a762ca7ff3fd65cb065c528740a37b5064c4', 'ec9d2d7868125331d6f34d0cdef53cf27b450820', '1b39e04c57ff07f5614f81c0c4797ee50803122f', 'a7065010db5543c02d53f602ba6931e9f0e69d8c', 'e0171a544d32782a4c3882cb3ff34dd057a7ff49', '57c587eedfd0f25f86fad3991ab2978398c890c8', 'b441ddf343911c1a0f8d6b3a437d79ce00c68f0f', '89c8aa20414672a4591b2d4df82d334137fff44b', '64e1a2b174496b919cbb3dcf50902a9af5f444ca', '53c86b26f02f56e92a38c560e4aa5b046878c763', '4c32870cc6450cb596cb74b01eae3bf7144d03e4', 'f878ae929922759f6a094d6c856a23e939858853', 'a73308d440625a67c80cd2d5c9090008c9be9f80', 'e890bfb449753bc794fab3cc98ecf8b66a2601f6', 'bd1ab7fe6df2a5dbd292f5a89e777972c1e500bc', 'ab2129caff74c53defd559ffcd1f12d793e8f174', '464023e4041a378b4826a1ce406575dc79b473b7', 'e8bf30c2c133c6e3ae6681909efa25d8edb8971e', '5ee43bc72c88f50fcfb6fdd710a06225846ce00b', 'fdb874a5c8e58f8abccf525a727a3413e1f1b759', '643c8c17ece46f4aec5db67199cc40bba3fa2a78', '0d286827a2bc9e7de533277d2c11255bac66496b', 'ea864047e3fd214878beced6a648408f8eaacefa', 'c0f8667b872b62155e66e57e36e4bb4044506747', '61ad1c50d8bb1a67d33e0020673f3347dc85935a', '6a32f50bb7f92445f5e5ace812ba146e9e2d7e63', '2a2bdad21be9bdc6af948462abe2a222f099097c', 'c93db39154a1eeb6085abe6119d1f9e4de4e5c8f', '69430980fe2880b9b06f72e0327ec15d669b1a54', '8e6d070195d4a5a048aa8e8d06cc2798afdc11ca', '0f1ca0e044c0998f0ea294ce5c429a574916d601', '2d9a5fc8c6affb69e1cb38a0181cd8b1b38e83b7', '9e5a8818e1386f42c59a8aeac81d32a12f011d93', '5c250d3e035ef770850d36599e29ebc8c69d7dc8', '4fdf040a82f1f15a066c9a8ea0943ec2a6358395', 'ea1a9eeab82cae242f8ae6a8b0f8a5fe8ba27845', 'fcb3a54d3a6b9b339eb6f8583f84cc10efae8986', 'c88bebb40a245d1f5555bdfb80f1b902be62f936', 'f4aebce1b34d9de6a2a403641977c7870d6f3918', 'fa70d436d25ab51992efac193b29fd398a90f5f0', '8a14cd4d89d3b9431bde792d9d826d7c2c407383', 'b4066a397e303161363ce89862132fdeae0199a2', '8bdeedacab0b3ecc5968c76f219e501ca6659176', 'af4b5684fbca9ed7b50c6df3f72999b036ea8e79', '6e7b61999ed8275efd2d4b34e4f53e783e8a9164', '05f7f75c3cab14e941bd27b585c86d3998f412df', '4cb570c770dcdf88b48d235ef280cc8e2c9beda3', '1561f98b6683778e36b0df2f7a7f079069bb2a3b', 'de16ca9859ab10c36e8aea8eab09f5143f68b39d', '8e57c4b3669133e09961df068b7ddd357a2059eb', '34fe25f49a3c7e0723ba4eea466884f7ef33104d', 'd7ea23352a88c086c148cee79890a63470d16b8d', '7a05002e8edb4c28e40c57e987c792f97bef71aa', '35e90d988889a16545583b267a1f931882127e99', '3678067dc8d6ec3e787a18c8e99762cbb4c232fa', '787085f7b73000b94b9dbd59be04fc0b6596f720', 'ac1464a6d98cf0ac9a951eadad9dd065301e99d8', '0df69845757c31a9329dc74cde93e0c2138a6896', 'cc2ecd92bab62b16732b9d20636f6034fa439a91', '49838912325b6d046b6cc14d27b062c5f7bc1449', 'c7037b1179e4d38336b53b5727845f56dedc9a99', '15c174e00313143f52f667a4c0ae3b4a31ba07f7', '8ef4ea7a639b7f7e1ae9ac16177900dc6ca76000', '091bed11c0694e311fa5676a34b03079d62e5472', '81d86206f59ff83c5a46c584c6b461a95fe3dd32', '9c1d843c92c3efb30f0a151657b0a0ad92f2ea62', 'b5ccda12348d74005d9e928a42a7671a337b8ff7', '0b588ebe591e163d8b1e1df7982769e08ae1eb0b', '21d41af61952500538e907673613b1d3b765acdf', '0f79e6324b3ee895cb672a68c7020b8a7fde1b17', '4ed3802e8151f582807363e61422f3beaf17274d', '043f85b09645e788f5b069c7542c348dcdfe173a', '006ae86a02d8cdc2b471621ec1761583577ac484', '5d0885053508c58638af879e0de3376d3b3ac87c', 'd1176e115e3e49775f15874142e41885b1a74ce5', '7f2a764a9f5da7b2e5dbc9ecdf7cb85cecd9bc3f', '3b93daa4adee016d5fd0439e3d52380097c5a099', '8b6b4d9766c062d0e2ec8fde014bf90e98538049', '9fe20cfafebeb2d0176700fe1c9b64ccc583853d', '15f716405fa7fc11d3b59f19c98f2ec28734f8a7', 'dec613220a62e998b20037a447c1782f842def1a', 'e88087cc3fde1c50a9255a24a7822103d5eae9fa', '7b9f983e9c835a325b64bd15449d284f38d9a5ee', '753d9142646e875cf9ab1d1de18884f936e6daa6', '2d6d6226fa47bca22bf875335d45ff4347d9e67e', '77c2a4233cb6499d3739ffc530635f43960b41af', '899a5b6629a067ffab4d9aa570b0c13294c42983', '888c1a0c5977d413ee58e0eb84042f6d5080e938', '6a6c706607ff24a4868d5f16497fe0ae3d8e6859', '6fd2ad1aae22d635b382ea4acbeef67fae3af5d2', '975aa6ce3ad36ea2efa34fbca953c87f81657374', '574e7de856c34f980ac80336c913ae968b3c4687', '6f003acbc28d7b323862888d92cf12db16d4c873', 'c05874dae758a7456ed10679f538a00616e94302', 'cec1faaf96cd970a15dfbbee015df73cc6770a0f', '86d2f41be9a37be8e2b36ea57a2d41e82c61837c', '97a3cf067eb6c7849ecd0b47e215097a485170d1', '1b39e43ca0438d348a333c13c73c59943fbb9c28', 'e05fd4a3d18c58d6c84cfc82810a6244328b7689', '55924efe87fd5ca2642c14b5c9e6ffdaccfe9143', '6b3c55ba65d3ceb8f9db1472655ec0ce5a121aa1', '43720fe1b3660cc2071c2f72a05516f7b287c966', 'c33e1b6d5a6fadebf50c9df1ff901faf366413ab', 'a4134433d5d1fe9f83b44c04c59636d1a85120bd', '4268fdd19ed1d233dfe45ded139dedfa06a193c1', '9e164ab067d610a244224d74fa0b21aaf217d54e', '610c1c4ddfb745f44885841ed293c0d64d225f58', '6e0f1085945fe359b46b8a6b583f8bd3ab48d953', 'f300d56a3dc444c8f2ab7a4d0df71f10dfe0467d', 'e4a07483c81e90b2278ef29bd7bdc3ccde6d1ce0', 'a7121630c53b6cd44bcc975c293e9bca065498db', '53e4b45e7e652f55276b47f34e66f29f4e33df0c', 'b011484ac12278ce2fc41990b190d623b328306b', 'dc7df1803ab4bb87dc92679210d7b69ce0e237e9', 'd33a47e79344733264e95a554fa8d5ced7c1061b', '4bcd7aec3bf2493b57363f59e890c0a49a6860a6', '8b3956496fb31ca373991b136e476f883b8a0937', '9837eac799c279e35ff02f80e1651a89b4962e07', '06eab37d7f50713ca38a89744ad35c4e3b596cc4', 'fa4f5d701711149913af347be12de46fd0a192ab', 'd6b587f28be7e334b29881970ebc53d22d4801d9', '4bcb6a68c40d11d97fd33bafe8928c7e8bc784e2', '21e58c2114c2e33d7792881f95dd73ed4532e916'], ['57fdc130c1b1c3dd1fd11845fe86c60e2d3b7193', '51317b6082322a96b4570818b7a5ec8b2e330f2f', '2a047d8c4c2a4825e0f0305294e7da14f8de6fd3', 'e105fa310790f91644d2d9f978582652d2d4de55', '0e5fea0a13594cfc6ab9c8cdf3095ed16b728e70', '2883f7edbe5d4a80bb694a4ee36abce29cab5706', 'f191a434b8ea61ddb6b20cfe99e65dfa710ab5e4', 'a24109c954c160dfd52ea6ff107b9fe6f75da0fe', '506172b0e0dd4269bdcfe96dda9ea9d8602bbfb6', '9cf5415eaec3c9cb70ea2dac92eab7652f829fc0']),
            9: BeirSciDoc('305c45fb798afdad9e6d34505b4195fa37c2ee4f', re.compile("^Iron, the most ubiquitous of the transition metals and the fourth most plentiful element in the Eart.{813}rk has begun to take advantage of iron's potential, and work in this field appears to be blossoming\\.$", flags=48), 'Synthesis, properties, and applications of iron nanoparticles.', ['5701357'], 2005, ['82b17ab50e8d80c81f28c22e43631fa7ec6cbef2', '649ad261855d2854f25093bf3efa541b2a249af7', '76eeff302dcb0fbef8986a6f317a70bdc1b263be', 'c3fcff9920c799eaf96378b289f79c26bf61a049', '4422e9e1655c09b558b3898f4b30caacd9bd3429', 'c93df6a2f47c04e3b45fcffcf5dbaa4032e65399', 'bc01ee13cd600fd91cd61a2367fee18083d38d2b', '52479e45194554361fb9d98c9ee33e23d252fefd', 'f6c961488bd11b9a4fc210b047f170a55a9eafa7', '1230a8fa38065b5310385d4d654bc425ccbbb6aa', 'e350e0d36f2f378acc8e81e773bd44e9ed22e966', '21cf8ba5f7965aff50269fc0115ad86e90aeccf3', '70fe357999f3860ea6ea10cd59d23148d7c62af3', '81316082c6c3dadc10ba97c1967979c02779ae4c', '0eddbed524231fd153e45a8c395601d754863468', '81c144d26df3d344b79ed36d49fca3aa20c9552c', '3f47268db51e4d283df6f3ca130e9913fda7c580', '849878e5d1140023cfdda8644052b0b2ef6d2aa3', '0b5c42f099d973605a9a6467baf9031fc9ca9a7e', '700bb2eabbe05de449ef18c74eae338aadfde954', 'ed10bf3bd7b9b006a916fa34d910f20fdf83d631', '9b83f2c29a1523fb765aa6317a3221892c3f173c', 'aad2912ff93b99a2f6abbbfe4627a82326a33565', 'd2e5db5f68170f768dc27407d02dbf56be7240a6', '56ba06f04c8e80920ee46b3a1c37de6918e0da47', 'fd072638033a25f2d635e12e17d4856b9611a1cf', '3f5a0e1110855a442e0fe31bd15c3006d90ba683', '0d2a00690d137883805ea160305ebfbdb9a0e9ef', '0b4a03a77b9c066421b94f18d06ed5051586fe5a', '074e2837dba47e756f270061fb723f06868d550d', '219872c6070a2f5400e95930a5e659305ddee09e', 'ece0c28b3b60672a2b2d04a529fc43a2bffa80e8', 'dccedf91d83f215186acbaf0fee8cce96630e69c', '016a58d257f1592fc4bf4f409d46e34448af9328', '4bafd7734b3f54b8d55b9e0755bef768cd96d1c3', '324d172296533539fbed787a3c255f883f0df455', '76e522626f8522e89e4e4334545ffc45d28b1c1b', '62226d54b69b7594441707e2af6f15d7c284b82b', '3692ac11ecf46a584e47d3784cdc8589628ffba0', '7982e3b444f78bc83e69e3d805e817fce0836d9b', '3fa55a993ccc2ad96d30416bffd346ba87fb47c0', 'ae9ecf8f5a62cd6145305b987ec796aeb0277d32', 'b9d80a195094d78080414f55988532d53a15d2b6', 'aa3d041859dd502b575638c7f118b722f066dccb', '653277b69172858b6972854291e0e716d8f487e1', '97e1b60b4391cfa956ac682bd9cc422694f542f8', '1e29d7d2fa274aa34e04ff223ca2e14a1e38154a', '3c57f7b2b31363cc370bf6d0ac16d47b8f910d58', 'ba2135a7f332e6cbc15680e57215286baba69964', '7311bee2d68d0bbee8a33070e681c7eb225ebfc5', '73245bba0fe6a8a344a3243b7b471217b7451e9e', '2b14b5d080181e1bba344047ef799ed6ed944298', '27fb0b03765798505a08760c748fffd3a0f477c0', '753670263c3ebd07de930a4e08b2a958efbf5601', 'ac5a49658c3f623e4a5c834edeb3a05c6443e72f', '2eab1eb680cac5787ae2b151f542cf37c6021ae7', '41d5b1ea2e9c040265e75eaba77d47dda24b8b71', '0ea10201b14c37f09cb707eca0ce611511b14fbf', 'beeb798dc5a4b1317d311934e85db7aada84632c', 'c4b70f1dfd91635dfb26adc2043ca50570df2ce5', 'ce8d1f138728bbdbcbba60ce511c1b01d9099ebc', 'c853a66473866e6a84f42a7b08158efa204926de', '1075e3a139b142fe8b020f0b37a96967caab72f8', 'ac6750c5f26e3cc6bddcf94db6f12db0c806e4a3', '11a9b9c13b00ac50efa85b7408a69fbf8eb0914d', '1755b4180e6f160645ca0f2ecd829288ac3558b5', '376f5f4b92a36b799e75efaf91f525aacf41c6c0', 'a2b6fb94bfbb0a9ed912df378d515fe12fc50c21', '79d0dded1bfbd307b1449e9f0515f384f86ce9c3', '82c0878c01a449e35d0d55cdcaf3a5a117dc3dbf', '57c6b7148d049c56a128b1c785a71a060aa5fd83', '314fce43a13565578b55a24dae9a96ed5a666c42', '0b07b032ba0eaa3082909e710c39a7c0baa6a712', '29b8c3b4fd393acc338f1af30b35dc0ab4c9abd4', 'f62ce373a2984a4f393723bccd931f254e476391', 'c8333d1c7faf00497eed14e6c0b8e76e3a3ee0d0', 'f7789ef50c67e3d9d57dc9d3b15a7f6f720f76fe', '1be32177c7a1b9c65dfacad88f0a135164b44246', '9359d828b98809f71d91a913284ca0b3a864b168', 'b42f4ff18a858ff8f02e6b144d295df7fb24a862', 'bcccfa9ea926dc235a1ebfd6447b722744628c80', 'f38611811b63a177d10273b9b3d2edd3456cf8a7', '2d574bd536a3ffe32bd90339beb7c194b91c7af9', '01302fc606ae1b1d5c7fccf0ea938e9fbcc74fff', '5d610cc0cced232b7ff3c8610b7e9f035a06d758', 'd6f235ff5f6a85c25d5d239cbe9a8256d7543ffe', '30b6c4d696985c2d0ae8cba908fcd09312de4bb9', 'aea3bf8bcdf385288c2a9f8abc594ae894976efc', '8afc9f9315f3549c3ecc5f8db52a55533254af23', 'bf0bfadcd6a7d24cc07deab36c18ef54daaadce0', 'c0a22c3f7cca7befde2ce5d596bac180dd64b531', '77ed0d5b23793683b26e40f894bb11f6978222cd', 'a4d3445492f8b44163704ea90d72072df228fc8a', '0d2707b130fef30620c3b62d48e59716fa22a43c', 'e4685e3bf2583d249fe0163db58f620993601774', '5df1f62d5d94e4902064bb13caade24014824ca8', 'ea6525e27686631438c56ecf6446e8718a9fa39a', '158037eeb0be786d51577d0db76141a166b0006c', '099eaa1b094a3e4228098d7cc85a5ddd133bca71', 'f3cfa4ab011b43468de46a109de6eef21d1f8d7a', 'c56349ace2995868973d5f05f08a073dd1d7d9ec', 'c4f6b2eba635152d382a4b14668c11f9c367c343', 'c34c62fedbda674aa0f16994a0978092f7dfcfc1', '9992dc990ea5d51c9977dae7dd97410aa59a2a3c', '8f23e205e11b4ad316791f200913e35e89516377', 'adaa03397ea52277404dc19b791b25419b31c373', 'a20414c9016670be7d6e11bdfc9ca7f856f749fb', '909d29ff282cbd443502c5c0d96a613c67f33629', 'e29310ff23984b0c20d1df175dfcf48b92fd640a', 'c4ce94a35fdc50bbfacb8224f852d74259605671', '6fa1ab72cdedb42156299615c740717a44158cbd', '867a0973fb128e373f02b35407fde004e8fc4996', 'dac854b3a2e5953da02a181d053528c9eba3d975', 'ff5ed956fabfae3df623645a3e320eb8e2dd5e41', '351b44c60e6a2e02b1915151904c215a25a2cbcf', '0bd95a3a558049ef440458d5790c36f7fff9174b', 'b50647c4c2d0a34a2399e9b682f7d7387bf0e547', 'ac04f4f12cafacab1853bed4eafcbe7e4afb69a9', '0cf7a2b868929aa88fc4561b83f05f7d58fdd021', '64dd6882be209c9efdc496be3f7a399c02bfa9c4', 'de53d4d37a37668b26a54afa7120a5182cfd786a', 'e12c59792ffdeb75a28f3ba81db308583c0c17f9', 'c976caedc14e0400d021ea3d61199b88d827a9ff', '017e2fe5742d457636a86d9821729aa05cd3bdbc', 'c785c0f92a180f09601d9962f3aec18814e649cb', '74d843664fac2f50d591fefd451febb219c672aa', '4d4e54fcecf8c2841295a7331382a1047a5fc662', 'c15241dff7c872c86df0990c94f9883b5af1808a', '7d1f3d5a1d7ccfcaea5a24152bc7c1d0fe10d62d', 'accb8c725011e522f87f15b7f2a1951f50726d98', '9604163e2a0e3471736f6417b7867448a869356b', 'd55d770616c3199a53b352ac618633cf10ee4110', 'd3c61afb3a20b887c5528ba87af70a47c557d505', 'ffb56b30db82123488fe837b080b26d69de1647c', 'a8b884d6164039714a5d418dde0a5a8fd6693120', 'e32cb4b6c12c7a26c9e9e3d0321d255b95623856', '6f9af3a5881b9e6c2be0f3e4f56d08ec46f06166', '782c86ae6584aac656a8ce62876188b16afc7eb5', 'ce94afc8f8487ffc21981377c69f8f818caa0ef9', '443da6abcaa3cf6dd145f8dd557e19084bbc9815', 'd04bba486e8c9de2b8806e0b99c48ab8966449c4', '28d536de93f2bd84243ae235b4cd258fb48d88ea', 'a35a221b962e4a665412365fbde6367d9c369d96', 'be2fc72b84bc786939c991f2dcd021fb1dd7dff9', '68f36ad7e630629c520c58ce4fe9ac4d2491aaf1', 'ad5ad62f929c1c4ab202e4ef7c6bb6bfd1ac95c3', '24bc7c579fa0f912b4a3995641ad0bea0fd2dceb', '854b6cdb220968a68cf6ad11d6c3fbe96783d8c8', 'b009814a37c19914a564c071540bdf78c76f56bf', 'a5f4f2ab56ba1ac85a5770188caaf2a4ee16dcef', '7118310a746334aa772f94223c8cd2a240a9f4fa', '41670cba9f5c684c49998b1831abd7c46766bc11', '0a173cc4df2d354ae6913d737569b28cc6b9d992', 'e4bef1c2f5c2ddad1a8d869154fac6d0dd989404', '15aa004a99ea2842d2ab70d840c2ed29c4072c2d', '5bac092a3a48743ed515db42e1745004a0be525c', '092787f5e169a4658646d884ddd93092bc4b301a', '969596feb7162be4202d9b356bcd201ae1b9d70d', 'c25732e3973def6cadef8763684be8ff0235089f', 'e2021aa1ca43bcd99b6ac04e4da3f8d8d4ef39c5', '8209bed97970d4e8c908a49733a2cfe79b5f2315', 'ea1948d8a5eec4f9a0ccadae9beea0de08b46c71', '41999eefce1245fb1cc765a9246bad5f1ae247c8', '8d52d2e3b5b17413da36db38b7e29867888e7af9', '5bd9c844ee92675cb37c2a0c2156aa12dda9f9ce', '8f4b37f0682042416e4fa0f8d5a3acfce5c23d22', '958d8d959eb1eeeebea185d64aac34b45e46c2ac', '03516888707a1e1f87d8fd12345c311a90b961b9', '39c5d5a21947311de08c8178bf903361f257a14f', '35ee5c602cb52437ce7a4c75fe96e53df66288bb', '1b36511b90d6bb197f9308c4fe26ed1667c33023', 'b5bb5d8e2a5c8f5f7f04bd0965ceb7dce221bf6b', 'df8350e74a4d1eaf4a17236f42b9b77ec2951b5f', 'b55a5ccdf3e173a9f9fe470b8d61980c4843d67e', 'b37c32d0802a93cd4fa976452125344d516fc004', 'f670c311f4f0c41bd4202f29fb4ea4cc0ca8bd9d', 'a24d84fe139209fbda2311c6a9b357e28c10bcf1', 'e00ca9adfa26620699b0475509de93460f37268d', '865caae7eaf3a0e7ee0a12e95ace2a02bfdd8919', 'efc2cfe6a9003a230713003ec5df8eae17528826', '22bbcb8672acd1fdbe2f694058a6244efd435d1f', 'd3de8dcb601d10e9b71a3b726e0ff5111cb08aea', 'd7aeb2d16745ffcaeca1fb8e0a8f87b6738bb44f', 'b62fca281fe3fd29a1e11a636f007c5a55bd9938', 'c81c75bcdd7ea2b40a3712969eb5beeffcb390f7', '53eb63a95ca2b7e6c9b82ddf24ebcafd6cdaef48', '7a704ba047cfa97a2c8057996576db9a531f8afc', '72385f812acf7ebd57cffaf06536a3486383e354', 'b955cfe4a4407d3ad2e6d53c152a36a2b43d56dd', '1621ff5f63dac23bebf332fac295ec6d41e0ac1e', '7b4beb7fa993a24dc88ddeb5faa9c9994aa18230', '9feae90b61adc5bbde268e035897aedf9e913f5e', '74e6d3f71ddcdbdeb55a46c922e8833f103fbd16', '92137a58b6a250a4f39af004cdd05bc496ffe9f0', '39cd8d5b25186bbf99e61022e98b610d20773317', 'ede32d4160582f65d737a4712aee7a2015f5390a', 'fd179de2435327e7ccb95704702e457aac11123b', '3a8d6149eb4c92f1717ebaaeb9e51c4163a77e11', '992557cc89374c5b3c2010ba12a82717912608b4', '3b6262bcc745fffd6aef146157691fd6ac33e6f9', '8129b5b0f3190e29f65c09ec3ef8727c9a2da3b0', 'bcf018c16e4e2c11d6908facd658eed1313ff3b2', '401e73ef446390882f78d7375104e1ae4a344196', '4dec24735e5484fafabca32d9a3209e18a247942', '497163e2a7d3cf4a3908590b6ae68d0e45c0d6ea', '7851c70e64465581cadf41e18619a0a0726aa00b', '4ef426e926d2563d59915dad1ec87ab16b947427', 'a33419689c715a8a260a01b2352a05fa082ecd39', '8a371dd14880a9269d12f70037a45b154416babb', '48f10958236db99c535afae9bc2399442ad899a9', 'bc3e934f0b8c5b398dd5f7b9e9bba331c4d0dd06', 'fba52f021cc9318ef8a3eab67157858bdd6c55f6', '525d7d59cd78ae002c0fbc7407c8f835ec104ce4', 'a6414214a8a14bff445670890468b5d5e0f89fcb', 'fcf6e8b6d040aa6f1ff3c2724be88ec27d18a61c', '5ff79419c1222da84b5e37049d2b8eaee360c474', '9bb1864ced59b42d664a968dabe3645e91b057c5', '77f8a71ef25c0e230ae7c34b13b132d73fe37f84', 'd46300092fe5fc457136182b83483b8fd2a02e78', 'cc5d71a21289e532111b57be9d306bb070cf959c', 'e6a5a36705a34393dbda3bc8a9b77d94b75651c0', 'f366350fe99eec61b9e5e5fac070caac670da5e0', 'dc825f53d9d148226a293905a0ab796682050035', '68931e654a5dc1d970b4e2f22cb42856654faa2e', '68f4c1207bb2157fbb9f475100772797cfd098da', '2359ab3ad6039b5493907917e2224e2cb9c57d27', 'ea338d19d5a05d1a58730228f34a6feaadde464c', 'b7f1855af7785a6a5072f444ea9378a6e6ef0415', '79b4960b495e7d41b94f916359c0fab562ea78d8', '12c1b2214448324fd3d01d316c86665b00fba379', '0219b02acf88159b1357139c559f8eefed952091', 'e07392e9f2318d6eeb7f7c41b691534247e11a2c', '3eefbf1328896be2c792c7cbf96a990d77d8feb0', '49f8b520f74d461665d954f70acb3935c73aa298', 'f580f4b6c3366a0a1a945b1327c3b0d4c540507c', '2d0ebbc0e8f54659845fa2153dfe226aad82ff95', 'f00eba6c08f005ba7aa6f18e44cf0015bf69c8e0', '0f184261246ccaf557de764ef80457dfef546403', '73b9c4c0e575c51879c3631b6857661213f5cd47', '5472998c0f2e46a647197bd1c48041618ab522ca', 'efcf2bfcefaaba169ee645709ecf5bcc206a747b', 'bdf9581688b89c21ac1b1b861f3aca5db5b95a2a', 'ca4fda4f7bf014d4dbcf4dc1bd80212d1ae61d71', 'e206bd8357ba07cd3a48cb306dc7c51b0d329e7b', '4baa1d0a041ec74ed0a445d9387c57bace6fda3c', '39ada29c525ed3717e3d7869ad704b2ba7b544c4', 'd17409c5ef135740ed24337a5da29d7e9154f87f', 'e380b3d3b506aec795513c66371006d43e426f6a', 'acfc24f92024c4e2100c6514ea80b81a8635c410', 'ecb7771f0eac083f85e0cab38e314fda2818d3fc', 'f1199d9ae3e967e804a742db3a593e9ce7654a1b', '7d69bb4459ec841dffe85b9a54fd5265de0cc007', 'c1053712f2ca6b1b937a414b2715fe96417c9083', 'ff53c4d87109c09b43b8a76e6e87ec1da64badb2', '372bb9084c6045f281a81a6dd782a06b209f8d35', '6976f42a2540a257f330f6a965dc0695a8cb7092', 'e9147fef923c5eb43f0dc12fddb637ab15d51ca7', 'abc663f70ef0a1158993bd5701c8477fe1e46982', '79f31764916adf5027c972acacbf68084b5b7b37', '3d3be4c0e405517af14e27b8e1fdbde543658391', '1ab25555a9174c4707e388626d0d6540f335341a', '5ad26cced507b1b39d5784d35c71e3cd8a080f58', '78c618f755b5822ed7d9565f63a717b41ebdbd47', '230b964bbfcb5ab9476ca159312d56e917fc6dbd', '33aac94569d23bd6a988e94100e47839808ff04e', 'b301638ae79e0d3af5eca862092a1a2c39562edb', 'e78653a4f162aa37ea4acdfeaa23792f2ce13bb5', '575f59ebb94ac4c1a29624affa1930752628a4ab', '7ea6f7ecb8987fca6ce0b561956029da127e0b30', 'f55212398b23a7a441a69c3448741f040c0929be', '4e3c76b7cbef1f11863ad535e6a43557ef9e0ed1', 'b76e1330c3d373d2492cdfffa4665209b5fb50fd', 'b0b2b7600879ba59b1a6b3dc369c0e0aa5464ac4', '2e0fa5c6e38b900e1b3622bf2ce3cc6eaa4d983e', '5ffa3c58579776df72e2b77b9f9bfd51c235de6b', 'f83be882c6b9f84b5adf76d4251bfba4b703a399', '2aac341861cd7269f97ff5418de23836dce6de8b', '742d730969401e56a7de1a35321f1ad8ed50ecbd', '764774f071883e2ff68babfd063d43a4daa2f921', '55ea401f7660c52e877bacd2229c97b2c6955ac1', '0deac7c72edb8cc37db930680238bb2601630e26', '0dc294932dafa11cc89149ccb29e810521654787', '21b180e4e2ff346e45d3f1aa2c48c81db84ef46a', '554386661c697e1e6c341637782f3ae00f126757', '9a08d01c5e48debce2d52e698beb6c50e062d665', '9f8f731cc84ddf8f44249dbabed4b8dd4a3b8d08', 'c3c8547c666701666ce75e92163c241ca5f85e2a', '2f7724bc63966142ff91ec88d31028b419866432', '91043b821acdb9d9c40462462902f46e51fd0a63', 'b089c66326e30ab02fed73fa264fc2e3a5df9603', '7f62a0e29ecd6015dd43770c0518793a9cdf96b6', '7c0287d7e14bd29f349c34021962cf9426f65e92', '060c0a33f2a20a0119dcce7757653ae7dce9a42b', '835752249a453a521656f81ab304255036a08949', 'd738014c3dafd3ae62bbf174f03d54d2e1a5ff80', 'eb90379a029c1770a84046c87771d9dc0d307d09', '1066e0f9b917b48d27e80b7753cfa28993f533a7', '06af1fda3e210c48fc53448adae16968a3b56dc9', '936b73c9112ebd80603657df0a2348ec77bcd777', 'beb69b64de0a5990b4e5fd2b6c8242d11a607939', '9cc8dd3f817acfa7bcdb7396ee634c83186c0965', '2c2e82fa02d82854f076a48f4df4e6cbd19d71cb', '67f2be0495f4e254e06326182a85dc94519c82a5', 'b33fed1870ad04e3a132d27cf8b851bdac500f99', '7427ea64f1ec24863a5908e2b150951b63e3a999', '16ae92614e0d5bc012592eea169351e1552227f5', 'c1dc82fab3bda6bbcb62780a277c40735d892e25', 'a2bdd8eed73fd484551675c1428a8a80bec59a9c', '52b8208627010d872222f557891e82cea7e46ac9', 'ae103997d2ffee109ddd6ca5edc9488fbe18fa7a', '1608a4f9cd7348eaab2c38083a97aeac9bdd0179', '9fc4fc4680c0889189219d97c99ef21d4ac0c3e7', 'b8c0dfdaa8a10b2154b1009d755ecc4434a59b55', 'a03cd9f82b0dec7211cf69706a584e76e88be92c', '60dc76da9a4ec31b4a82412805e8bb9da97d2b62', '427f96c89f2a843d4178d70c567b1eac0a34c09d', '52f7101ba422d10ce51f91ae17e8a9c3c3e9d803', '319af963f7916906a8b289bc29ed73712e62c924', '926c26d1f4f44a3e378a0afb0dd4902f54f56cfd', 'ddce99ce4ed40bb0d628794676b9aa7afa02e3eb', 'be9f029b77c20f71167a73332ee57012ea0a7c1c', 'dfbd9e2f0b2c09d9dc554bbff38c02b80a936ad1', '8c9aea4b33049b53c90f40bd5ffe914edf6809db', '395b8c11dc7629faafb4b36b7996fae8888c3734', 'b86ccc5d835789db47e67e3fdfc8431ea448b706', '16a59335dc6283b5d6af107c30f7c95d801c6a48', '9b292736c1a20393c5066b251dc7eda48cad837e', '80581200117906d25b4613d37ad2dee5bf40412f', 'e6d99d566c8acc46b88a49a5441e641ac9bba22b', '5485c00be97e3079be0adec7ce43930b8e88f7db', 'dc052e81525a6919e21a58fadc7f32b843d7b513', '566175d708e15390b3a8e2c4f395f271854973a5', '32b623788b524de9c2e63cf6c7e9b2e7fd18fcfd', '855c40f815b90d177109232b88d8b3a7551b5237', '7c4d2db34738222fb564df8b0664157deab68cc4', 'c67a4eaa3f27267a25037ef8b06822f6631e783a', 'e45a59ae928c5f2ae0f56b6ba781455958eb01ce', 'c6f221afbe9610f2e9794f7346aa1e586ece2ff8', 'e145f48e48416a03d428a8df054fda200ca93eea', '09edabbe36b0e8c093d6da0047896587360e9112', '9746b0d7f7405fac73e347a98eccc4c387b216a5', '6c6249c85922c18e422031f1acebdc9339aac0ac', 'f251e5af0d9a2114e6becdaa3f54325cb743ca98', 'edbfe3b8c228ca664723d464208f161b99819514', '32d814c8f0b83c922953b3dcde1842a3bfb12f88', '33707ac4306abd31f160bc2b2fe51f846a7dd8e4', '24abddabaf2bc24b74b39120481f00f3bdf8dee1', '8850cf6c6666b79a37683d6dd23e6a1a42fb90de', 'ad889aa95c9e9fc81bb39a830efd816d40ecbcb1', '3ff1f4f289a5a3b30843977b9e2e0b96337c80e2', 'b49c22c953c3347f3564c09143d2b4af8de650a8', '4b98980d38325f4acc7dc97a4e0f28b59104a2ca', 'bc06585dd065b5dbdd8c921842fc9b5fc8b0f824', 'd335fd80c55caa51bb45df048867145741442efa', '26c88ad749aaa5c38bb8df402c688945919a509a', 'a539ba2d6f9681673583ff14434ae6a10abf6007', '0f0742dbb0c5000610eab03a19ec3e143924ae85', '91c009e7c9ad80bfe17afe16549592434afab665', 'e9c287b098a12dc688b5bd15ccf3450b52332afa', 'bb72a1231d67087e7f9c6127aff01bb2cc88d6e3', '3399c46b98dde9ad4ed5f8ac6d845e879e8652c7', 'f813cdfcc4335225363dc2e451c3fffeaf802d48', 'da1478be124100b7b1e7568ab620c9256268a277', '6108b950956c81a70ff3825080f861d05089edb2', '6828e0f244452d65b87d2ee9d951e355dbf211de', 'bc3d402ec8f695b6d2693e2fed826a3df53311e9', 'fc9f9e817d2e516003f748f0a8b327c5490a0231', '864ec1624f5113c76e43c722a950993ac89b179c', '91f41249f40d7bcc23168e9b619e926cac0c0c04', '230f5f69a64e853d9f82a7671d6a3e23cef21833', '53d6201b1b93fe6e3b2960379e04451021e100c0', '18dd8da3ff75481aa3886c45db32a82c73312bf3', '375a5f47f41681a56972fd459e6ee4c45c157075', '84cdf4f541e5df3979c0fea8ffd9cabdd210d9d8', '00c988c15598723a56554afebeb7205ce9e125ce', '50272ffe52aef0a4809ec45fbc7db08b2ac5e061', 'b111f0a20689f728775ceff8eb9428080a38fc3e', 'a25987f004462fd10bfe725b80ce951293d1f023', '4f92e2f8aa903474b7ed42d04ad46d8258bf35e8', '07453e6118007b17085f4c7b2488980a2f286d4e', '7485695064599f3c8bce132a67b62b3bdfe00494', '7e91565735983342e2d574a9041abbd489a4cb30', 'e80c8919ad8c91b1d07f63840f838c030a1f0c9e', '6a365c98a6e471575bdfeaf91bfa6a4e59dd77d1', 'b2bfd9406c86673be294d1f707f0880e9635cb6c', 'd2fe98b10345504770d388882dbb5bf1066048c5', 'b239b72a6889c3886bfa4e8b4e635f68062338ae', '88c5ac54a4833632bd41ba007a18f8dfe383add5', 'e23eedf4af59c9a6b25e5131185945f5bf7f3d40', '442855386a0754f524d8ab75b0696c830b4dbd45', '4e7b1213b71a0c32a7de951140214811d6606601', '8fb23352aaebc55e7be1245b0cf17470cea10f01', '967aa275ef52cfd0c1f9120ca39dbc94d5e23181', '1e3a6dc165d7351f20112a4a6a3ab77a9ae61f2c', '37aa433dae08bda1a45c5e83ca8497c4c2f8d95f', '3b6c72d3eac24cbc966cc5fcbb4443affe67fd15', 'f890bc9f98fe18f811d8bb2708f3411bba5dff12', 'd978bf57ab07e2d222f6c13fad6251d10ced695a', 'f35865413c0769a1d018bea58e5552abd2f2f141', '647bebe39fbd4ff408600f4d7fd599f4bbd8b4b7', '97f300a878382f910dbe5c950b29ba0c7b6d1638', 'e43f56c393009fd6bddb1628409c37162310591a', 'cbe50ce355fef7f56bd5bbc23633e5e0d9a4e4aa', '2185bc89e605412401f7bcf3744c0eebf6d160f5', '7dea0e9e5685ca477f512ef3c37147555e03e2a2', 'e0871c5b82ac7eb4345c8f7f3d393454eee32d12', 'd27dcea3db37e2135ad62f644dc6d8882581df59', 'af1f8904fe6f6041869790150c64e3804d5ce385', 'aa869232abec3716f6bea6b05003dc8fcc96fa0b', 'c558c38a0ba025342d50a7bcf4fca78ed121322f', 'baca82c40b68348a9788f68c1d917883f4455bfe', '1a0f4fee9f2ce40bfa98963659400ffbd72f533b', 'e95d55ddbc0d651bd543ea3bca650e4fea6ec3f0', 'ae61a81a2986ca66ce262d096c7557c926775ece', 'c87444cef977cce169853815ba3e44e6039a6176', '929d760ff022a61d83267bb31e5a9debd0fad992', '3eacd8a05832fd3741865f0d33e93509003057d3', 'df8843a770a3817aa6128144968f9780e4d8b93c', '6babd89607b1d72b80ee6f7e5a2b600eb7dde83c', '48d80bde5fe88f2c1f02a4cd953e176007171b39', 'd66382542ae64f9f72845a22e584c7ba3814263e', 'd249718d5a0abb9a24bc58085dc052a5d0065118', 'aef9504ab27b78fbfa8c9e03dc676546c0d9ace7', 'ce630a0ca55c28ee5c78701445f99de22e37d908', 'e5b2f34a3cc5b0fe8967fe4e911060fc33727608', '92096cc0bff6454642716f5f8859e37bd5a26d89', '3febafa73e50ba16acd9ebdcb8f97520392acf25', '531d1311849001df8a199a1561a8ccea74d69b31', 'dec866c42e27e6ae9a1d04de405e06abc9c43231', '24974211a78aba94f94e0b1461ff8aecde6a2858', '02060d0511d0b69a2a88ae24d70bb3bff5f55196', '7e0ca91c5a0f2358b5e4123e6ec97edc8b001f6c', 'b47975da306b3a13b51a2ab9d2590e9cb380a358', '199b8894a9d6c45a60784afe53e791663796ff82', '3cda6f14b9c753ff7c7a1fe1a7ef238d06de7d8d', 'b6f87b13112358d65ac4a4868620bb15e916e74b', 'ee10fff76b317675bef74fc788f89f1c59eab4b9', '2063fa14fa570773a6d88e77e777d87811ca0e55', '21c4188daf823693faade26d11b2bf823d68a1a4', '9fc29f559fbe1062abb47a1accf6357c2a23db0d', 'e5edb1fa542980f20a99d5e740e03e866a0ff86d', '3d3972fcaec4f025766c9a9fad74675caa5ca991', '023eba60e52495d80efcafcef9abf35adf38baf9', '30117df64ded972ef9ad78eb1cb3086e43424d08', 'edeea9ff910f0a42f3efb722e203bf1a190ded01', '595054842ee43c6ff199afe1e6e95cee3b18fbf6', '599b3cfab390ab4d2757cde4807b2468accf4004', '58d91aa5b6626920cbb924bea2b8933e95b7ab57', '060163b1ca089f648cc95ccf9f31f6bd5645327d', '8b58936a85ec96e6f12e7de5558141955cb3baca', '461c23462aba4e4c34a49e72564da739a8f64d9c', '0e00e2789b9907bc3c5d192bb0f6536a816b2a72', '92824bfb08987bd73c60b4d69abcb30dbe9f328c', 'e14212c50b25171a3bae06acd67088f6189f1f0a', '09ab8816f2dffe2d9fe651e04a4d8868e701afbb', '788d64bc1282b8ff8d0e38c19b60e7679b6c569e', '9a7d6f41fe294aaa7ab3050d2cd695d5a25ed3a1', '32f134d380a8d46fbc178fd7f66cebf6374ef570', 'fd8ef7098cbc17b1b39431598250a10490a172aa', 'dc9364152536239d00204c963d96e97587d691ed', '16bc2fd12f2c4bf216e2b0de71d75477151926bd', '3ef379894ea4780ffb5be0f180008d85fc955c7f', '1487d2e3f8f66df27b1d62f999629b1dc8e7850b', '525f139a9b30ba35649c122a0e0c3024acdf7a2f', '592074660bd09cc687eaf1b5405e695b9a7c7df8', '2446763300c204dcecf16e603a38f3a9572a428f', 'a1025f329fc408b30e46a7ba2a4cabdd7b4b986e', 'b8277c72a333bdc50fc570e263e1a00e77efafe5', 'f287158565caa170d99d47f31c5fcf03c93a293c', '2af88fe47c4f87430126690410366cdca27b7c05', 'c8504bba850d87d6c1ba9ec3d32a42f5bcf38078', '0039975b8f375ca7f4905bacd70cabf2bdd38f84', '511eca6e5b4e68c55bf090ec1802501b76fdecdc', 'f00dbb58d325623c5a6f934fdc49c74995174d20', '6d1bd66c17e47b03f4a2449a934bbfd3a6e5f535', 'ca98a256c3430490eb6d2cf7994b60353fc15200', '431a514a8b439da8612575ccfbfc4f559880b38f'], []),
            25656: BeirSciDoc('dec997b20ebe2b867f68cc5c123d9cb9eafad6bb', re.compile('^Training deep neural networks generally requires massive amounts of data and is very computation int.{1320}classification problems, thus significantly boosting our ability to solve such problems efficiently\\.$', flags=48), 'Deriving optimal weights in deep neural networks', ['9716460', '2116548', '1695338'], 2018, [], ['367f2c63a6f6a10b3b64b8729d601e69337ee3cc', '178325c2b267bee56931f22e4f17c6454de7475a', '0d67362a5630ec3b7562327acc278c1c996454b5', '2efc0a99f13ef8875349ff5d47c278392c39e064', '15e0daa3d2e1438159e96f6c6fd6c4dd3756052c', '5d90f06bb70a0a3dced62413346235c02b1aa086', '7346d681807bf0852695caa42dbecae5265b360a', 'c61d139a2382760f560164e25e4be264de5dd59f', '1827de6fa9c9c1b3d647a9d707042e89cf94abf0', '563e821bb5ea825efb56b77484f5287f08cf3753']),
        })
        self._test_docs('beir/scifact', count=5183, items={
            0: BeirTitleDoc('4983', re.compile('^Alterations of the architecture of cerebral white matter in the developing human brain can affect co.{1609}or MRI provides insight into microstructural development in cerebral white matter in living infants\\.$', flags=48), 'Microstructural development of human newborn cerebral white matter assessed in vivo by diffusion tensor magnetic resonance imaging.'),
            9: BeirTitleDoc('70490', re.compile('^Likelihood ratios are one of the best measures of diagnostic accuracy, although they are seldom used.{286}ples illustrate how the clinician can use this method to refine diagnostic decisions at the bedside\\.$', flags=48), 'Simplifying likelihood ratios'),
            5182: BeirTitleDoc('198309074', re.compile('^Introduction: Among the inflammatory mediators involved in the pathogenesis of obesity, the cell adh.{1481}nical inflammation that results from obesity by reducing the cell adhesion molecules and chemokines\\.$', flags=48), 'Adhesion molecules and chemokines: relation to anthropometric, body composition, biochemical and dietary variables'),
        })
        self._test_docs('beir/trec-covid', count=171332, items={
            0: BeirCordDoc('ug7v899j', re.compile('^OBJECTIVE: This retrospective chart review describes the epidemiology and clinical features of 40 pa.{1647}preschool children and that the mortality rate of pneumonia in patients with comorbidities was high\\.$', flags=48), 'Clinical features of culture-proven Mycoplasma pneumoniae infections at King Abdulaziz University Hospital, Jeddah, Saudi Arabia', 'https://www.ncbi.nlm.nih.gov/pmc/articles/PMC35282/', '11472636'),
            9: BeirCordDoc('jg13scgo', re.compile('^This report describes the design and implementation of the Real\\-time Outbreak and Disease Surveillan.{1077} be a resource for implementing, evaluating, and applying new methods of public health surveillance\\.$', flags=48), 'Technical Description of RODS: A Real-time Public Health Surveillance System', 'https://academic.oup.com/jamia/article-pdf/10/5/399/2352016/10-5-399.pdf', '12807803'),
            171331: BeirCordDoc('pnl9th2c', '', 'Vascular Life during the COVID-19 Pandemic Reminds Us to Prepare for the Unexpected', 'https://www.sciencedirect.com/science/article/pii/S1078588420303804?v=s5; https://www.ncbi.nlm.nih.gov/pubmed/32446539/; https://doi.org/10.1016/j.ejvs.2020.04.040; https://api.elsevier.com/content/article/pii/S1078588420303804', '32446539'),
        })
        self._test_docs('beir/webis-touche2020', count=382545, items={
            0: BeirToucheDoc('c67482ba-2019-04-18T13:32:05Z-00000-000', re.compile('^My opponent forfeited every round\\. None of my arguments were answered\\. I don’t like the idea of winn.{293} sold to minors in ANY state\\. A retailer who says it is illegal to sell you them is, frankly, wrong\\.$', flags=48), 'Contraceptive Forms for High School Students', 'CON', 'https://www.debate.org/debates/Contraceptive-Forms-for-High-School-Students/1/'),
            9: BeirToucheDoc('fbe6ad2-2019-04-18T11:12:36Z-00001-000', re.compile('^Why is it that so\\-called christians, Because there is no such a thing as a christian, Have serious t.{315}, All you did was babble on and on and on\\. So in this sense, It was YOU that forfeited\\. Sheesh! Bye\\.$', flags=48), 'The closet dementia of the superior ego god complex, The bible and why you should not believe in god', 'PRO', 'https://www.debate.org/debates/The-closet-dementia-of-the-superior-ego-god-complex-The-bible-and-why-you-should-not-believe-in-god/1/'),
            382544: BeirToucheDoc('671509c8-2019-04-17T11:47:34Z-00007-000', 'Charter schools are exploited most by affable students', 'Charter schools', 'CON', 'http://www.debatepedia.org/en/index.php/Debate:_Charter_schools'),
        })
        self._test_docs('beir/webis-touche2020/v2', count=382545, items={
            0: BeirToucheDoc('c67482ba-2019-04-18T13:32:05Z-00000-000', re.compile('^My opponent forfeited every round\\. None of my arguments were answered\\. I don’t like the idea of winn.{293} sold to minors in ANY state\\. A retailer who says it is illegal to sell you them is, frankly, wrong\\.$', flags=48), 'Contraceptive Forms for High School Students', 'CON', 'https://www.debate.org/debates/Contraceptive-Forms-for-High-School-Students/1/'),
            9: BeirToucheDoc('fbe6ad2-2019-04-18T11:12:36Z-00001-000', re.compile('^Why is it that so\\-called christians, Because there is no such a thing as a christian, Have serious t.{315}, All you did was babble on and on and on\\. So in this sense, It was YOU that forfeited\\. Sheesh! Bye\\.$', flags=48), 'The closet dementia of the superior ego god complex, The bible and why you should not believe in god','PRO', 'https://www.debate.org/debates/The-closet-dementia-of-the-superior-ego-god-complex-The-bible-and-why-you-should-not-believe-in-god/1/'),
            382544: BeirToucheDoc('671509c8-2019-04-17T11:47:34Z-00007-000', 'Charter schools are exploited most by affable students', 'Charter schools', 'CON', 'http://www.debatepedia.org/en/index.php/Debate:_Charter_schools'),
        })
        self._test_docs('beir/cqadupstack/android', count=22998, items={
            0: BeirCqaDoc('51829', re.compile('^I want to send files to android tablet with a application from PC\\. \\- I can send files directly to ta.{188}m \\? \\- How can show my device as a external drive\\? my application that sent files written via Delphi\\.$', flags=48), 'How can show android tablet as a external storage to PC?', ['usb-connection-mode']),
            9: BeirCqaDoc('19394', re.compile('^I bought "Cut the Rope" on my Nexus One cellphone from the Android Market\\. When I open this game on .{51}it to be "Purchased"\\. How can I add my Google account to Kindle Fire\'s Amazon appstore account list\\?$', flags=48), 'How can I use an app purchased from the Market on a Kindle Fire?', ['google-play-store', 'amazon-kindle-fire', 'accounts']),
            22997: BeirCqaDoc('38348', re.compile('^With the growing number of Android devices in all sorts of different form factors \\(dev boards like R.{163}roid\\. For example, having the standard Linux build tools available would let me easily run a server\\.$', flags=48), 'Is there any easy way to get GNU build tools on Android? If not... why not?',['linux', 'development']),
        })
        self._test_docs('beir/cqadupstack/english', count=40221, items={
            0: BeirCqaDoc('11547', re.compile('^An eponym is one way to eternal \\(if posthumous\\) fame\\. But is there a word meaning an eponym someone .{65}oycott_ , Mr Justice _Lynch_ , and Patrick _Hooligan_ would not appreciate their undying notoriety\\.\\)$', flags=48), 'Is there a word meaning "an unwanted eponym"?', ['single-word-requests', 'eponyms']),
            9: BeirCqaDoc('182056', re.compile("^In the following statement, which one is grammatically correct\\? > XYZ caterers \\*\\*is\\*\\* on to somethin.{76} be 'are' as caterers is plural\\. But it has been suggested that I might be wrong\\. What do you think\\?$", flags=48), '"XYZ caterers is.." or "XYZ caterers are.."?', ['grammar', 'grammatical-number']),
            40220: BeirCqaDoc('38346', re.compile('^A colleague and I were having a discussion as to the proper plural form of _abacus_\\. I believe the p.{183}rd that is part of the Arabic language\\. Any opinions or history to this matter would be appreciated\\.$', flags=48), 'Plural of "abacus"', ['meaning', 'etymology', 'grammar', 'latin', 'roots']),
        })
        self._test_docs('beir/cqadupstack/gaming', count=45301, items={
            0: BeirCqaDoc('11542', 'What\'s your Supreme Commander 2 build order. I don\'t just want "6 mass extractors, 2 power and a factory". List of building and units out to the second or third factory, please.', 'Supreme Commander 2 - Build Orders', ['supreme-commander-2']),
            9: BeirCqaDoc('19393', re.compile('^What are the benefits of an assault ship over an interceptor\\? I played some significant time ago, an.{176}So: a\\) What are the main uses of each b\\) Which would most benefit the style of play mentioned above\\?$', flags=48), 'Assault ships v. Interceptors', ['eve-online']),
            45300: BeirCqaDoc('38346', re.compile("^_But you can't have more than one companion\\._ \\*\\*Wrong\\.\\*\\* So I was taking that stupid dog, Barbas, to.{156}ne with the dragon they start attacking each other\\. How can I get them to stop and be friends again\\?$", flags=48), 'How do I make my companions friends?', ['skyrim']),
        })
        self._test_docs('beir/cqadupstack/gis', count=37637, items={
            0: BeirCqaDoc('73399', re.compile("^There is a satellite image it's size is 10 GB and I need to display this image using GeoServer and O.{211} response time using 32 GB satellite image\\. Please advice me how to achieve this\\? Thanks in advance\\.$", flags=48), 'Satellite image display with the help of GeoServer and OpenLayers', ['openlayers', 'geoserver']),
            9: BeirCqaDoc('5983', re.compile('^Has anyone succeeded in programmatically updating metadata in ArcGIS 10\\? Considering using Python/ar.{254} except where they are in conflict in which case the added elements overwrite the existing elements\\.$', flags=48), 'Programmatically edit/update metadata in ArcGIS 10', ['arcobjects', 'arcgis-10.0', 'python', 'c#', 'metadata']),
            37636: BeirCqaDoc('103092', re.compile('^Link: http://projects\\.nytimes\\.com/census/2010/explorer How can I also render that specific kind of m.{121} says its from Google at the bottom right, but then why does it look different from maps\\.google\\.com\\?$', flags=48), 'What map library does this census visualization use?', ['gui']),
        })
        self._test_docs('beir/cqadupstack/mathematica', count=16705, items={
            0: BeirCqaDoc('35237', re.compile("^I'm trying to use `Get` to load some pretty substantial packages from a custom menu in the _Mathemat.{320}` / `MenuItem`\\) that will remove that time constraint so that my command can be executed completely\\.$", flags=48), 'Time constraints on KernelExecute commands or MenuItems?', ['front-end', 'menu']),
            9: BeirCqaDoc('28990', re.compile("^I have multiple data sets, each of which is a 2D matrix\\. I want to construct a new 2D Matrix in whic.{139}ix2\\[i\\]\\[j\\] \\+ \\.\\.\\. \\+ MatrixN\\[i\\]\\[j\\]\\)      I can't quite figure out how to do it in _Mathematica_\\. Thanks$", flags=48), 'Averaging multiple 2D data sets', ['list-manipulation', 'matrix']),
            16704: BeirCqaDoc('34149', re.compile('^I want to add two matrices, the first one containing a 2D vector at each position the other one a li.{675},MB\\},2\\]      This works but is rather slow\\. Is there a faster and maybe more elegant way to do this\\?$', flags=48), 'MapThread Alternatives', ['list-manipulation', 'performance-tuning', 'map']),
        })
        self._test_docs('beir/cqadupstack/physics', count=38316, items={
            0: BeirCqaDoc('110557', re.compile("^Let's discuss about \\$SU\\(3\\)\\$\\. I understand that the most important representations \\(relevant to physi.{732} indices\\)\\. What is the general procedure to represent the generators in an arbitrary representation\\?$", flags=48), 'Representation of SU(3) generators', ['particle-physics', 'group-representations']),
            9: BeirCqaDoc('11546', re.compile('^I have a question about the relation: \\$\\\\exp\\(\\-i \\\\vec\\{\\\\sigma\\} \\\\cdot \\\\hat\\{n\\}\\\\phi/2\\) = \\\\cos\\(\\\\phi/2\\) \\- i .{152}alized for \\$\\\\hat\\{n\\}\\$ being an operator\\? If so how exactly would the expression be different\\? Thanks\\.$', flags=48), 'generalizing spin rotations', ['quantum-mechanics', 'angular-momentum', 'spin']),
            38315: BeirCqaDoc('38347', re.compile("^Let's say a box is moved by attaching a rope to it and pulling with an applied force at a certain an.{528}ined ramp, the above would not work\\. What do I need to do differently to solve this type of problem\\?$", flags=48), 'Overcoming Friction', ['homework', 'newtonian-mechanics', 'friction']),
        })
        self._test_docs('beir/cqadupstack/programmers', count=32176, items={
            0: BeirCqaDoc('228054', re.compile('^I am in the midst of writing a web application for work\\. Everything is from scratch\\. I have been a P.{739}s of speed\\. So, my question is as the title asks, is a client\\-side centric app substantially slower\\?$', flags=48), 'Are (mostly) client-side JavaScript web apps slower or less efficient?', ['javascript', 'node.js', 'ajax', 'browser', 'client-side']),
            9: BeirCqaDoc('127472', re.compile("^I've been developing web apps for a while now and it is standard practice in our team to use agile d.{317}words, when you develop ML and NLP algorithms as a job, do you use agile development in the process\\?$", flags=48), 'Is Agile Development used in Machine Learning and Natural Language Processing?', ['agile', 'development-process', 'machine-learning', 'nlp']),
            32175: BeirCqaDoc('213799', re.compile("^I'm developing a small system with two components: one polls data from an internet resource and tran.{762}he other writes\\? I started writing the code but was wondering if this is a misapplication of SQLite\\.$", flags=48), 'SQLite with two python processes accessing it: one reading, one writing', ['web-development', 'python', 'sql', 'concurrency', 'sqlite']),
        })
        self._test_docs('beir/cqadupstack/stats', count=42269, items={
            0: BeirCqaDoc('110556', re.compile("^I'm a beginner in statistics and R, sorry if this question may seem trivial\\. I've collected data mea.{5246}analysis do you suggest\\?   \\* If yes, how can I interpret the result I got \\(please, in simple terms\\)\\?$", flags=48), 'Is this a case for an ordinal logistic regression? Problems interpreting output', ['r', 'regression', 'logistic', 'interpretation']),
            9: BeirCqaDoc('89379', re.compile('^!\\[enter image description here\\]\\(http://i\\.stack\\.imgur\\.com/qmNwR\\.png\\) The image above represents a hyp.{574} know of a good way to do that\\? If there is a better place to ask this question, please let me know\\.$', flags=48), 'Need subspace partition algorithm, not necessarily a full classifier', ['machine-learning', 'data-mining']),
            42268: BeirCqaDoc('38346', re.compile('^Regression: Wage=b0\\+b1collegegrad, where collegegrad is a dummy variable\\. Suppose you want to estima.{221}nd thus get the true ratio, so the estimator is consistent\\. Am I correct, or am I missing something\\?$', flags=48), 'Consistency of estimator', ['self-study', 'consistency']),
        })
        self._test_docs('beir/cqadupstack/tex', count=68184, items={
            0: BeirCqaDoc('182565', re.compile('^I am using a pgfplots stacked bar to display the aggregated energy demand of a houshold and the asso.{1179}              \\\\legend\\{low price, high price\\}     \\\\end\\{axis\\}     \\\\end\\{tikzpicture\\}     \\\\end\\{document\\}$', flags=48), 'Adding horizontal lines to pgfplots bar', ['pgfplots', 'bar-chart']),
            9: BeirCqaDoc('61123', re.compile('^> \\*\\*Possible Duplicate:\\*\\*   >  Left and right subscript   >  Superscripts before a letter in math I .{128} the subscript O but to be on the left side\\? Is this possible which commands/packages I need to use\\?$', flags=48), 'How to change the side on which the superscript appears?', ['superscripts']),
            68183: BeirCqaDoc('103090', re.compile('^I appreciate it if you let me know the most elegant way to draw a crossed hierarchy such as the foll.{3}ngs:                   X        /\\\\       Y  Z       /\\\\/\\\\      p q  t      q has two parents Y and Z\\.$', flags=48), 'Crossed hierarchy', ['tikz-pgf', 'horizontal-alignment', 'tikz-trees']),
        })
        self._test_docs('beir/cqadupstack/unix', count=47382, items={
            0: BeirCqaDoc('110557', re.compile('^Is there a way to avoid ssh printing warning messages like this\\?               "@@@@@@@@@@@@@@@@@@@@.{196}the remote host identity has changed but I know it is fine and just want to get rid of this warning\\.$', flags=48), 'Force ssh to not to print warnings', ['ssh']),
            9: BeirCqaDoc('110550', 'What is the difference between the red5 versions RC1 and RC2 ? and what does RC mean?', 'What is the difference between red5 RC1 and RC2?', ['broadcast']),
            47381: BeirCqaDoc('38346', re.compile("^I've got my vacation coming in and thought I might use that for something useful\\. Essentially, I've .{2438}stuff used in enterprise security, I'm very ignorant about how things are actually used in practice\\.$", flags=48), 'Getting from proficient to expert', ['shell', 'virtualization', 'storage', 'cluster']),
        })
        self._test_docs('beir/cqadupstack/webmasters', count=17405, items={
            0: BeirCqaDoc('35236', re.compile("^I'm making a website for a small hotel in php\\. The hotel owners want a reservation system that uses .{290}d of buying with paypal\\. Is this possible\\? Does anyone know of an open php system that handles this\\?$", flags=48), 'Hotel Reservation Request Booking Paypal PHP', ['php', 'looking-for-a-script', 'paypal']),
            9: BeirCqaDoc('503', re.compile("^My website used to have sitelinks and now it doesn't\\. It's very possible that it's due to changing t.{219}\\.imgur\\.com/sBaDc\\.jpg\\) What are some things that I can do to improve my chances of getting sitelinks\\?$", flags=48), 'What are the most important things I need to do to encourage Google Sitelinks?', ['seo', 'google', 'sitelinks']),
            17404: BeirCqaDoc('38346', re.compile("^I'm looking for a keyword racking tracker tool for google\\. I have found a lot of them over the inter.{182}ord as my site has hundreds of pages\\. Any recommendation\\? Or do I have to set each URLs per keyword\\?$", flags=48), 'Keyword ranking tracker that works on a per-domain basis', ['seo', 'keywords', 'tools', 'ranking', 'google-ranking']),
        })
        self._test_docs('beir/cqadupstack/wordpress', count=48605, items={
            0: BeirCqaDoc('108998', re.compile("^In a shortcode context, is there any difference here\\?               array\\(             'slideshow' =.{32}         array\\(             'slideshow' => NULL,         \\),       Is there a best practice for that\\?$", flags=48), 'What is the difference between Null vs Empty (Zero Length) string?', ['php', 'plugin-development', 'shortcode']),
            9: BeirCqaDoc('19393', re.compile("^I'm using WP\\-Cufon for font replacements\\. It's adding extra cufon canvas out side of p tags in my pa.{127} it happening\\? How can I solve it\\? I'm having same kind of problem with all\\-in\\-one cufon plugin too\\.$", flags=48), 'WP-Cufon adding extra space in my paragraphs in Firefox and Chrome', ['plugins', 'javascript', 'plugin-all-in-one-cufon']),
            48604: BeirCqaDoc('38344', 'Is there a specific reason why we can find max-width:97.5% instead of 100% in common themes such as Twenty Eleven?', 'Why max-width:97.5% on content images?', ['theme-development', 'css', 'maximized-width']),
        })

    def test_queries(self):
        self._test_queries('beir/arguana', count=1406, items={
            0: GenericQuery('test-environment-aeghhgwpe-pro02a', "Being vegetarian helps the environment  Becoming a vegetarian is an environmentally friendly thing to do. Modern farming is one of the main sources of pollution in our rivers. Beef farming is one of the main causes of deforestation, and as long as people continue to buy fast food in their billions, there will be a financial incentive to continue cutting down trees to make room for cattle. Because of our desire to eat fish, our rivers and seas are being emptied of fish and many species are facing extinction. Energy resources are used up much more greedily by meat farming than my farming cereals, pulses etc. Eating meat and fish not only causes cruelty to animals, it causes serious harm to the environment and to biodiversity. For example consider Meat production related pollution and deforestation  At Toronto’s 1992 Royal Agricultural Winter Fair, Agriculture Canada displayed two contrasting statistics: “it takes four football fields of land (about 1.6 hectares) to feed each Canadian” and “one apple tree produces enough fruit to make 320 pies.” Think about it — a couple of apple trees and a few rows of wheat on a mere fraction of a hectare could produce enough food for one person! [1]  The 2006 U.N. Food and Agriculture Organization (FAO) report concluded that worldwide livestock farming generates 18% of the planet's greenhouse gas emissions — by comparison, all the world's cars, trains, planes and boats account for a combined 13% of greenhouse gas emissions. [2]  As a result of the above point producing meat damages the environment. The demand for meat drives deforestation. Daniel Cesar Avelino of Brazil's Federal Public Prosecution Office says “We know that the single biggest driver of deforestation in the Amazon is cattle.” This clearing of tropical rainforests such as the Amazon for agriculture is estimated to produce 17% of the world's greenhouse gas emissions. [3] Not only this but the production of meat takes a lot more energy than it ultimately gives us chicken meat production consumes energy in a 4:1 ratio to protein output; beef cattle production requires an energy input to protein output ratio of 54:1.  The same is true with water use due to the same phenomenon of meat being inefficient to produce in terms of the amount of grain needed to produce the same weight of meat, production requires a lot of water. Water is another scarce resource that we will soon not have enough of in various areas of the globe. Grain-fed beef production takes 100,000 liters of water for every kilogram of food. Raising broiler chickens takes 3,500 liters of water to make a kilogram of meat. In comparison, soybean production uses 2,000 liters for kilogram of food produced; rice, 1,912; wheat, 900; and potatoes, 500 liters. [4] This is while there are areas of the globe that have severe water shortages. With farming using up to 70 times more water than is used for domestic purposes: cooking and washing. A third of the population of the world is already suffering from a shortage of water. [5] Groundwater levels are falling all over the world and rivers are beginning to dry up. Already some of the biggest rivers such as China’s Yellow river do not reach the sea. [6]  With a rising population becoming vegetarian is the only responsible way to eat.  [1] Stephen Leckie, ‘How Meat-centred Eating Patterns Affect Food Security and the Environment’, International development research center  [2] Bryan Walsh, Meat: Making Global Warming Worse, Time magazine, 10 September 2008 .  [3] David Adam, Supermarket suppliers ‘helping to destroy Amazon rainforest’, The Guardian, 21st June 2009.  [4] Roger Segelken, U.S. could feed 800 million people with grain that livestock eat, Cornell Science News, 7th August 1997.  [5] Fiona Harvey, Water scarcity affects one in three, FT.com, 21st August 2003  [6] Rupert Wingfield-Hayes, Yellow river ‘drying up’, BBC News, 29th July 2004"),
            9: GenericQuery('test-environment-assgbatj-pro01a', 'Animals shouldn’t be harmed  The difference between us and other animals is a matter of degree rather than type [2]. Their bodies resemble ours, as do their ways of conveying meaning. They recoil from pain, appear to express fear of a tormentor, and appear to take pleasure in activities; a point clear to anyone who has observed a pet dog on hearing the word “walk”.  We believe other people experience feelings like us because they are like us in appearance and behaviour. An animal sharing our anatomical, physiological, and behavioural characteristics is surely likely to have feelings like us.  If people have a right to not be harmed, we must ask ourselves what makes animals different? If animals feel what we feel, and suffer like us, to condemn one to testing because of them being of a different species is similar to racism or sexism.[3]'),
            1405: GenericQuery('test-society-epsihbdns-con01a', 'Freedom of movement is an intrinsic human right  Every human being is born with certain rights. These are protected by various charters and are considered inseparable from the human being. The reason for this is a belief that these rights create the fundamental and necessary conditions to lead a human life. Freedom of movement is one of these and has been recognised as such in Article 13 of the Universal Declaration of Human Rights. [1] If a family finds themselves faced with starvation, the only chance they have of survival might be to move to another place where they might live another day. It is inhuman to condemn individuals to death and suffering for the benefit of some nebulous collective theory. While we might pass some of our freedoms to the state, we have a moral right to the freedoms that help us stay alive – in this context freedom of movement is one of those.  [1] General Assembly, “The Universal Declaration of Human Rights”, 10 December 1948,'),
        })
        self._test_queries('beir/climate-fever', count=1535, items={
            0: GenericQuery('0', 'Global warming is driving polar bears toward extinction'),
            9: GenericQuery('21', 'Sea level rise has been slow and a constant, pre-dating industrialization'),
            1534: GenericQuery('3134', 'Over the last decade, heatwaves are five times more likely than if there had been no global warming.'),
        })
        self._test_queries('beir/dbpedia-entity', count=467, items={
            0: GenericQuery('INEX_LD-20120112', 'vietnam war facts'),
            9: GenericQuery('INEX_LD-2012336', '1906 territory Papua island Australian'),
            466: GenericQuery('TREC_Entity-20', 'Scotch whisky distilleries on the island of Islay.'),
        })
        self._test_queries('beir/dbpedia-entity/dev', count=67, items={
            0: GenericQuery('INEX_LD-20120112', 'vietnam war facts'),
            9: GenericQuery('INEX_LD-2012336', '1906 territory Papua island Australian'),
            66: GenericQuery('TREC_Entity-17', 'Chefs with a show on the Food Network.'),
        })
        self._test_queries('beir/dbpedia-entity/test', count=400, items={
            0: GenericQuery('INEX_LD-20120111', 'vietnam war movie'),
            9: GenericQuery('INEX_LD-20120312', 'tango culture countries'),
            399: GenericQuery('TREC_Entity-20', 'Scotch whisky distilleries on the island of Islay.'),
        })
        self._test_queries('beir/fever', count=123142, items={
            0: GenericQuery('75397', 'Nikolaj Coster-Waldau worked with the Fox Broadcasting Company.'),
            9: GenericQuery('76253', 'There is a movie called The Hunger Games.'),
            123141: GenericQuery('81957', 'Trouble with the Curve is a television show.'),
        })
        self._test_queries('beir/fever/dev', count=6666, items={
            0: GenericQuery('137334', 'Fox 2000 Pictures released the film Soul Food.'),
            9: GenericQuery('18708', 'Charles Manson has been proven innocent of all crimes.'),
            6665: GenericQuery('46064', 'The NAACP Image Award for Outstanding Supporting Actor in a Drama Series was first given in 1996.'),
        })
        self._test_queries('beir/fever/test', count=6666, items={
            0: GenericQuery('163803', 'Ukrainian Soviet Socialist Republic was a founding participant of the UN.'),
            9: GenericQuery('134850', 'Ice-T refused to ever make hip-hop music.'),
            6665: GenericQuery('81957', 'Trouble with the Curve is a television show.'),
        })
        self._test_queries('beir/fever/train', count=109810, items={
            0: GenericQuery('75397', 'Nikolaj Coster-Waldau worked with the Fox Broadcasting Company.'),
            9: GenericQuery('76253', 'There is a movie called The Hunger Games.'),
            109809: GenericQuery('152180', 'Susan Sarandon is an award winner.'),
        })
        self._test_queries('beir/fiqa', count=6648, items={
            0: GenericQuery('0', 'What is considered a business expense on a business trip?'),
            9: GenericQuery('14', "What are 'business fundamentals'?"),
            6647: GenericQuery('2399', 'Where do web sites get foreign exchange currency rate / quote information?'),
        })
        self._test_queries('beir/fiqa/dev', count=500, items={
            0: GenericQuery('7208', 'Could an ex-employee of a company find themself stranded with shares they cannot sell (and a tax bill)?'),
            9: GenericQuery('7526', 'First time investor wanting to invest in index funds especially Vanguard'),
            499: GenericQuery('4872', 'Taking a car loan vs cash and effect on credit score'),
        })
        self._test_queries('beir/fiqa/test', count=648, items={
            0: GenericQuery('4641', 'Where should I park my rainy-day / emergency fund?'),
            9: GenericQuery('6715', 'What does it mean if “IPOs - normally are sold with an `underwriting discount` (a built in commission)”'),
            647: GenericQuery('2399', 'Where do web sites get foreign exchange currency rate / quote information?'),
        })
        self._test_queries('beir/fiqa/train', count=5500, items={
            0: GenericQuery('0', 'What is considered a business expense on a business trip?'),
            9: GenericQuery('14', "What are 'business fundamentals'?"),
            5499: GenericQuery('11104', 'Selling a stock for gain to offset other stock loss'),
        })
        self._test_queries('beir/hotpotqa', count=97852, items={
            0: GenericQuery('5ab6d31155429954757d3384', 'What country of origin does House of Cosbys and Bill Cosby have in common?'),
            9: GenericQuery('5adcd29a5542992c1e3a241d', "The 2015 Kids' Choice Sports Awards was hosted by an American footbal quarterback who was born on November 29th of what year?"),
            97851: GenericQuery('5ac132a755429964131be17c', 'Blackfin is a family of processors developed by the company that is headquartered in what city?'),
        })
        self._test_queries('beir/hotpotqa/dev', count=5447, items={
            0: GenericQuery('5ae81fbf55429952e35eaa37', 'Daniel Márcio Fernandes plays for a club founded in which year ?'),
            9: GenericQuery('5a7fbc715542995d8a8ddf08', "The Monkey's Uncle and Benji the Hunted, are what form of entertainment?"),
            5446: GenericQuery('5a8bae0c5542996e8ac889b5', 'The director of "An American Tragedy" emigrated permanently to the United States at what age?'),
        })
        self._test_queries('beir/hotpotqa/test', count=7405, items={
            0: GenericQuery('5a8b57f25542995d1e6f1371', 'Were Scott Derrickson and Ed Wood of the same nationality?'),
            9: GenericQuery('5a8db19d5542994ba4e3dd00', 'Are Local H and For Against both from the United States?'),
            7404: GenericQuery('5ac132a755429964131be17c', 'Blackfin is a family of processors developed by the company that is headquartered in what city?'),
        })
        self._test_queries('beir/hotpotqa/train', count=85000, items={
            0: GenericQuery('5ab6d31155429954757d3384', 'What country of origin does House of Cosbys and Bill Cosby have in common?'),
            9: GenericQuery('5adcd29a5542992c1e3a241d', "The 2015 Kids' Choice Sports Awards was hosted by an American footbal quarterback who was born on November 29th of what year?"),
            84999: GenericQuery('5a7543b155429916b01642cd', 'What is the title of the book that documents the involvement of the president of the BioProducts Division at Archer Daniels Midland in a conspiracy case?'),
        })
        self._test_queries('beir/msmarco', count=509962, items={
            0: GenericQuery('1185869', ')what was the immediate impact of the success of the manhattan project?'),
            9: GenericQuery('186154', 'feeding rice cereal how many times per day'),
            509961: GenericQuery('195199', 'glioma meaning'),
        })
        self._test_queries('beir/msmarco/dev', count=6980, items={
            0: GenericQuery('300674', 'how many years did william bradford serve as governor of plymouth colony?'),
            9: GenericQuery('54544', 'blood diseases that are sexually transmitted'),
            6979: GenericQuery('195199', 'glioma meaning'),
        })
        self._test_queries('beir/msmarco/test', count=43, items={
            0: GenericQuery('19335', 'anthropological definition of environment'),
            9: GenericQuery('156493', 'do goldfish grow'),
            42: GenericQuery('1133167', 'how is the weather in jamaica'),
        })
        self._test_queries('beir/msmarco/train', count=502939, items={
            0: GenericQuery('1185869', ')what was the immediate impact of the success of the manhattan project?'),
            9: GenericQuery('186154', 'feeding rice cereal how many times per day'),
            502938: GenericQuery('405466', 'is carbonic acid soluble'),
        })
        self._test_queries('beir/nfcorpus', count=3237, items={
            0: BeirUrlQuery('PLAIN-3', 'Breast Cancer Cells Feed on Cholesterol', 'http://nutritionfacts.org/2015/07/14/breast-cancer-cells-feed-on-cholesterol/'),
            9: BeirUrlQuery('PLAIN-15', 'Why Do Heart Doctors Favor Surgery and Drugs Over Diet?', 'http://nutritionfacts.org/2015/06/02/why-do-heart-doctors-favor-surgery-and-drugs-over-diet/'),
            3236: BeirUrlQuery('PLAIN-3472', 'How Doctors Responded to Being Named a Leading Killer', 'http://nutritionfacts.org/video/how-doctors-responded-to-being-named-a-leading-killer/'),
        })
        self._test_queries('beir/nfcorpus/dev', count=324, items={
            0: BeirUrlQuery('PLAIN-1', 'Why Deep Fried Foods May Cause Cancer', 'http://nutritionfacts.org/2015/07/21/why-deep-fried-foods-may-cause-cancer/'),
            9: BeirUrlQuery('PLAIN-101', 'How to Treat Multiple Sclerosis With Diet', 'http://nutritionfacts.org/2014/07/22/how-to-treat-multiple-sclerosis-with-diet/'),
            323: BeirUrlQuery('PLAIN-3471', 'Uprooting the Leading Causes of Death', 'http://nutritionfacts.org/video/uprooting-the-leading-causes-of-death/'),
        })
        self._test_queries('beir/nfcorpus/test', count=323, items={
            0: BeirUrlQuery('PLAIN-2', 'Do Cholesterol Statin Drugs Cause Breast Cancer?', 'http://nutritionfacts.org/2015/07/16/do-cholesterol-statin-drugs-cause-breast-cancer/'),
            9: BeirUrlQuery('PLAIN-102', 'Stopping Heart Disease in Childhood', 'http://nutritionfacts.org/2014/07/15/stopping-heart-disease-in-childhood/'),
            322: BeirUrlQuery('PLAIN-3472', 'How Doctors Responded to Being Named a Leading Killer', 'http://nutritionfacts.org/video/how-doctors-responded-to-being-named-a-leading-killer/'),
        })
        self._test_queries('beir/nfcorpus/train', count=2590, items={
            0: BeirUrlQuery('PLAIN-3', 'Breast Cancer Cells Feed on Cholesterol', 'http://nutritionfacts.org/2015/07/14/breast-cancer-cells-feed-on-cholesterol/'),
            9: BeirUrlQuery('PLAIN-15', 'Why Do Heart Doctors Favor Surgery and Drugs Over Diet?', 'http://nutritionfacts.org/2015/06/02/why-do-heart-doctors-favor-surgery-and-drugs-over-diet/'),
            2589: BeirUrlQuery('PLAIN-3474', 'Fish Consumption and Suicide', 'http://nutritionfacts.org/video/fish-consumption-and-suicide/'),
        })
        self._test_queries('beir/nq', count=3452, items={
            0: GenericQuery('test0', 'what is non controlling interest on balance sheet'),
            9: GenericQuery('test9', 'who makes the decisions about what to produce in a market economy'),
            3451: GenericQuery('test3451', 'when will notre dame played michigan state again'),
        })
        self._test_queries('beir/quora', count=15000, items={
            0: GenericQuery('318', 'How does Quora look to a moderator?'),
            9: GenericQuery('784', 'Why should one hate Shahrukh Khan?'),
            14999: GenericQuery('537876', 'How do Russian politics and geostrategy affect Australia and New Zealand?'),
        })
        self._test_queries('beir/quora/dev', count=5000, items={
            0: GenericQuery('318', 'How does Quora look to a moderator?'),
            9: GenericQuery('784', 'Why should one hate Shahrukh Khan?'),
            4999: GenericQuery('537790', 'What are the most interesting books on the side of atheism?'),
        })
        self._test_queries('beir/quora/test', count=10000, items={
            0: GenericQuery('46', 'Which question should I ask on Quora?'),
            9: GenericQuery('616', 'Which are the best books to understand calculus?'),
            9999: GenericQuery('537876', 'How do Russian politics and geostrategy affect Australia and New Zealand?'),
        })
        self._test_queries('beir/scidocs', count=1000, items={
            0: BeirSciQuery('78495383450e02c5fe817e408726134b3084905d', 'A Direct Search Method to solve Economic Dispatch Problem with Valve-Point Effect', ['50306438', '15303316', '1976596'], 2014, ['38e78343cfd5c013decf49e8cf008ddf6458200f'], ['632589828c8b9fca2c3a59e97451fde8fa7d188d', '4cf296b9d4ef79b838dc565e6e84ab9b089613de', '86e87db2dab958f1bd5877dc7d5b8105d6e31e46', '4b031fa8bf63e17e2100cf31ba6e11d8f80ff2a8', 'a718c6ca7a1db49bb2328d43f775783e8ec6f985', 'cf51cfb5b221500b882efee60b794bc11635267e', '6329874126a4e753f98c40eaa74b666d0f14eaba', 'a27b6025d147febb54761345eafdd73954467aca']),
            9: BeirSciQuery('ae0fb9c6ebb8ce12610c477d2388447a13dc4694', 'Distributed Privacy-Preserving Collaborative Intrusion Detection Systems for VANETs', ['49104949', '1709793'], 2018, ['a1e81122931a5e96ced6569d0ee22b174db1ebb7', '96bbb9c86cdd9d19643686f623898367f9efb0bc', '228c40580e888fc9df003a16b8b7abb5d854a6eb', 'ab95903604d7fb8c03148b1a4f56af3c6de6fde1', '4875ac38970c742d6bfa760ca26ab7a629fde8da'], ['24d800e6681a129b7787cbb05d0e224acad70e8d', '216d7c407109f5557ae525b292856c4ab56996ca', '6e63c4a8320be712e3067eef3f042bb3df38a8e1', '49934d08d42ed9e279a82cbad2086377443c8a75', 'b45d9d5957416f363635025630d53bf593d3dd5c', '11861442e7b59669d630aed8c3b5d5290a70687e', '0dacd4593ba6bce441bae37fc3ff7f3b70408ee1', '8ef2a5e3dffb0a155a14575c8333b175b61e0675', '32334506f746e83367cecb91a0ab841e287cd958', '61efdc56bc6c034e9d13a0c99d0b651a78bfc596']),
            999: BeirSciQuery('89e58773fa59ef5b57f229832c2a1b3e3efff37e', 'Analyzing EEG signals to detect unexpected obstacles during walking', ['2492849', '6622542', '2927560', '40259975', '3334492', '46629632'], 2015, ['37512f0a2d5ea940f4debe84593ec2c054126c1e', '5181fe04756a2481d44bad5ec7f26461e41eaca0', '858e561895faadc6d6300948f06fd018a56c6775', '46f3cf9ff98c02b382079ec2d514c47379c3ffaa', '26fc69fb8cc5969b515e3b7d2bdc6ff83f68ac58', 'f9e11a43ccb47b58bc08937750f65d6306e6961a', 'fcd16ea07b9f35a851444f9933ca72535015d46c', '5fc1491937224b215a543196fe2514794b329c03', 'ac9e0bb99f12d697137b2373e1d5ba6f8babf355'], ['f1277592f221ea26fa1d2321a38b64c58b33d75b', '42ad00c8ed436f6b8f0a4a73f55018210181e4a3', '22ff979fafd58acea3b838036fdc55ed60b1a265', 'a20369f96ca4d73fbe25cc9e099b0f9ad57eb4a9', '94485df9a4a975ac8ae32e7f539c8a4f77d88f12', 'a5e6a3fb9bbfc4e494427b4f3a1782b9aefcab92', '4933491737750764aa304288f004f05a06f68704', 'f24a222de1e81b4dd5d9e3a6c5feb4499b095d4d', '57abdefc6e05d475cf5f34d190b8225a74de79f0', '0989bbd8c15f9aac24e8832327df560dc8ec5324']),
        })
        self._test_queries('beir/scifact', count=1109, items={
            0: GenericQuery('0', '0-dimensional biomaterials lack inductive properties.'),
            9: GenericQuery('15', '50% of patients exposed to radiation have activated markers of mesenchymal stem cells.'),
            1108: GenericQuery('1395', 'p16INK4A accumulation is  linked to an abnormal wound response caused by the microinvasive step of advanced Oral Potentially Malignant Lesions (OPMLs).'),
        })
        self._test_queries('beir/scifact/test', count=300, items={
            0: GenericQuery('1', '0-dimensional biomaterials show inductive properties.'),
            9: GenericQuery('51', 'ALDH1 expression is associated with better breast cancer outcomes.'),
            299: GenericQuery('1395', 'p16INK4A accumulation is  linked to an abnormal wound response caused by the microinvasive step of advanced Oral Potentially Malignant Lesions (OPMLs).'),
        })
        self._test_queries('beir/scifact/train', count=809, items={
            0: GenericQuery('0', '0-dimensional biomaterials lack inductive properties.'),
            9: GenericQuery('15', '50% of patients exposed to radiation have activated markers of mesenchymal stem cells.'),
            808: GenericQuery('1407', 'β1/Ketel is able to bind microtubules.'),
        })
        self._test_queries('beir/trec-covid', count=50, items={
            0: BeirCovidQuery('1', 'what is the origin of COVID-19', 'coronavirus origin', "seeking range of information about the SARS-CoV-2 virus's origin, including its evolution, animal source, and first transmission into humans"),
            9: BeirCovidQuery('10', 'has social distancing had an impact on slowing the spread of COVID-19?', 'coronavirus social distancing impact', "seeking specific information on studies that have measured COVID-19's transmission in one or more social distancing (or non-social distancing) approaches"),
            49: BeirCovidQuery('50', 'what is known about an mRNA vaccine for the SARS-CoV-2 virus?', 'mRNA vaccine coronavirus', 'Looking for studies specifically focusing on mRNA vaccines for COVID-19, including how mRNA vaccines work, why they are promising, and any results from actual clinical studies.'),
        })
        self._test_queries('beir/webis-touche2020', count=49, items={
            0: BeirToucheQuery('1', 'Should teachers get tenure?', "A user has heard that some countries do give teachers tenure and others don't. Interested in the reasoning for or against tenure, the user searches for positive and negative arguments. The situation of school teachers vs. university professors is of interest.", "Highly relevant arguments make a clear statement about tenure for teachers in schools or universities. Relevant arguments consider tenure more generally, not specifically for teachers, or, instead of talking about tenure, consider the situation of teachers' financial independence."),
            9: BeirToucheQuery('10', 'Should any vaccines be required for children?', 'Anti-vaccination movements are on the rise, and so are pathogens like measles again. The freedom to not vaccinate paired with rampant disinformation may be a threat to society at large, and for children in particular. A users thus wonders, whether there are vaccines that should be mandatory.', 'Highly relevant arguments name one or more vaccines and reason about the (un)necessity to administer them to children. Relevant arguments talk about vaccination for children in general.'),
            48: BeirToucheQuery('50', 'Should everyone get a universal basic income?', 'Redistribution of wealth is a fundamental concept of many economies and social systems. A key component might be a universal basic income, however, a user wonders whether this truly would help.', 'Highly relevant arguments take a clear stance toward the universal basic income, giving clear premises. Relevant arguments offer only emotional arguments, or talk about minimum wages, mentioning universal basic income only in passing.'),
        })
        self._test_queries('beir/webis-touche2020/v2', count=49, items={
            0: BeirToucheQuery('1', 'Should teachers get tenure?', "A user has heard that some countries do give teachers tenure and others don't. Interested in the reasoning for or against tenure, the user searches for positive and negative arguments. The situation of school teachers vs. university professors is of interest.", "Highly relevant arguments make a clear statement about tenure for teachers in schools or universities. Relevant arguments consider tenure more generally, not specifically for teachers, or, instead of talking about tenure, consider the situation of teachers' financial independence."),
            9: BeirToucheQuery('10', 'Should any vaccines be required for children?', 'Anti-vaccination movements are on the rise, and so are pathogens like measles again. The freedom to not vaccinate paired with rampant disinformation may be a threat to society at large, and for children in particular. A users thus wonders, whether there are vaccines that should be mandatory.', 'Highly relevant arguments name one or more vaccines and reason about the (un)necessity to administer them to children. Relevant arguments talk about vaccination for children in general.'),
            48: BeirToucheQuery('50', 'Should everyone get a universal basic income?', 'Redistribution of wealth is a fundamental concept of many economies and social systems. A key component might be a universal basic income, however, a user wonders whether this truly would help.', 'Highly relevant arguments take a clear stance toward the universal basic income, giving clear premises. Relevant arguments offer only emotional arguments, or talk about minimum wages, mentioning universal basic income only in passing.'),
        })
        self._test_queries('beir/cqadupstack/android', count=699, items={
            0: BeirCqaQuery('11546', 'Android chroot ubuntu - is it possible to get ubuntu to recognise usb devices', ['linux', 'development']),
            9: BeirCqaQuery('20256', 'Does Android hide some amount of RAM from the User?', ['linux', 'development']),
            698: BeirCqaQuery('61210', 'Can you remotely download AndroidLost to your phone if your phone battery is dead?', ['linux', 'development']),
        })
        self._test_queries('beir/cqadupstack/english', count=1570, items={
            0: BeirCqaQuery('19399', 'Is "a wide range of features" singular or plural?', ['meaning', 'etymology', 'grammar', 'latin', 'roots']),
            9: BeirCqaQuery('21616', 'How are "yes" and "no" formatted in sentences?', ['meaning', 'etymology', 'grammar', 'latin', 'roots']),
            1569: BeirCqaQuery('76823', 'When to use articles and when not to?', ['meaning', 'etymology', 'grammar', 'latin', 'roots']),
        })
        self._test_queries('beir/cqadupstack/gaming', count=1595, items={
            0: BeirCqaQuery('82449', 'Can the trophy system protect me against bullets?', ['skyrim']),
            9: BeirCqaQuery('176686', 'Please instruct me on how to light myself on fire', ['skyrim']),
            1594: BeirCqaQuery('146551', 'How can I fix a corrupted solo world?', ['skyrim']),
        })
        self._test_queries('beir/cqadupstack/gis', count=885, items={
            0: BeirCqaQuery('52462', 'Calculating mean upslope aspect from each cell in DEM using Python?', ['gui']),
            9: BeirCqaQuery('12833', 'How to smooth a DEM?', ['gui']),
            884: BeirCqaQuery('104332', 'MODIS MOD13Q1 extract ndvi value', ['gui']),
        })
        self._test_queries('beir/cqadupstack/mathematica', count=804, items={
            0: BeirCqaQuery('35544', 'How to use Automorphisms[] on a graph?', ['list-manipulation', 'performance-tuning', 'map']),
            9: BeirCqaQuery('37414', 'limit calculation step by step', ['list-manipulation', 'performance-tuning', 'map']),
            803: BeirCqaQuery('25260', 'NDSolve with vector function', ['list-manipulation', 'performance-tuning', 'map']),
        })
        self._test_queries('beir/cqadupstack/physics', count=1039, items={
            0: BeirCqaQuery('110554', 'Magnetic field resistance material: are there any?', ['homework', 'newtonian-mechanics', 'friction']),
            9: BeirCqaQuery('12012', 'Is spacetime simply connected?', ['homework', 'newtonian-mechanics', 'friction']),
            1038: BeirCqaQuery('16082', 'How do I find the frictional force using a free body diagram?', ['homework', 'newtonian-mechanics', 'friction']),
        })
        self._test_queries('beir/cqadupstack/programmers', count=876, items={
            0: BeirCqaQuery('88392', 'Why is closure important for JavaScript?', ['web-development', 'python', 'sql', 'concurrency', 'sqlite']),
            9: BeirCqaQuery('210327', "What is the one or the few major changes from Java 6 to Java 7, couldn't JBoss do that already with Java 5?", ['web-development', 'python', 'sql', 'concurrency', 'sqlite']),
            875: BeirCqaQuery('133937', 'Methods to rewrite a program', ['web-development', 'python', 'sql', 'concurrency', 'sqlite']),
        })
        self._test_queries('beir/cqadupstack/stats', count=652, items={
            0: BeirCqaQuery('11546', 'Tool to confirm Gaussian fit', ['self-study', 'consistency']),
            9: BeirCqaQuery('59955', 'Variance of superset from variance of subsets', ['self-study', 'consistency']),
            651: BeirCqaQuery('35719', 'Improvement of regression model', ['self-study', 'consistency']),
        })
        self._test_queries('beir/cqadupstack/tex', count=2906, items={
            0: BeirCqaQuery('197555', 'How can I learn to make my own packages?', ['tikz-pgf', 'horizontal-alignment', 'tikz-trees']),
            9: BeirCqaQuery('57481', 'Aliasing issues using beamer with pdfLaTeX', ['tikz-pgf', 'horizontal-alignment', 'tikz-trees']),
            2905: BeirCqaQuery('84944', 'How I can delete frametitle after pagebreak in mdframed box?', ['tikz-pgf', 'horizontal-alignment', 'tikz-trees']),
        })
        self._test_queries('beir/cqadupstack/unix', count=1072, items={
            0: BeirCqaQuery('103549', 'Yanked USB Key During Move', ['shell', 'virtualization', 'storage', 'cluster']),
            9: BeirCqaQuery('111331', 'Evolution of the shell', ['shell', 'virtualization', 'storage', 'cluster']),
            1071: BeirCqaQuery('20536', 'reformatting output with aligned columns', ['shell', 'virtualization', 'storage', 'cluster']),
        })
        self._test_queries('beir/cqadupstack/webmasters', count=506, items={
            0: BeirCqaQuery('28994', 'Someone else is using our Google Analytics Tracking code number. What do we do?', ['seo', 'keywords', 'tools', 'ranking', 'google-ranking']),
            9: BeirCqaQuery('30705', 'Redirecting from blogger to custom domain', ['seo', 'keywords', 'tools', 'ranking', 'google-ranking']),
            505: BeirCqaQuery('65733', 'Does removing ID from url improve SEO?', ['seo', 'keywords', 'tools', 'ranking', 'google-ranking']),
        })
        self._test_queries('beir/cqadupstack/wordpress', count=541, items={
            0: BeirCqaQuery('120122', "How to enqueue script or style in a theme's template file?", ['theme-development', 'css', 'maximized-width']),
            9: BeirCqaQuery('23263', 'Syntax highlighting for post/page editor', ['theme-development', 'css', 'maximized-width']),
            540: BeirCqaQuery('90939', 'All-in-One Event Calendar: Custom Query - Getting each event Instance', ['theme-development', 'css', 'maximized-width']),
        })

    def test_qrels(self):
        self._test_qrels('beir/arguana', count=1406, items={
            0: TrecQrel('test-environment-aeghhgwpe-pro02a', 'test-environment-aeghhgwpe-pro02b', 1, '0'),
            9: TrecQrel('test-environment-assgbatj-pro01a', 'test-environment-assgbatj-pro01b', 1, '0'),
            1405: TrecQrel('test-society-epsihbdns-con01a', 'test-society-epsihbdns-con01b', 1, '0'),
        })
        self._test_qrels('beir/climate-fever', count=4681, items={
            0: TrecQrel('0', 'Habitat_destruction', 1, '0'),
            9: TrecQrel('9', 'Carbon_dioxide', 1, '0'),
            4680: TrecQrel('3134', 'Global_warming', 1, '0'),
        })
        self._test_qrels('beir/dbpedia-entity/dev', count=5673, items={
            0: TrecQrel('INEX_LD-2009096', '<dbpedia:1889_in_France>', 0, '0'),
            9: TrecQrel('INEX_LD-2009096', '<dbpedia:CityCenter>', 0, '0'),
            5672: TrecQrel('TREC_Entity-17', '<dbpedia:Worst_Cooks_in_America>', 0, '0'),
        })
        self._test_qrels('beir/dbpedia-entity/test', count=43515, items={
            0: TrecQrel('INEX_LD-2009022', '<dbpedia:Afghan_cuisine>', 0, '0'),
            9: TrecQrel('INEX_LD-2009022', '<dbpedia:British_cuisine>', 0, '0'),
            43514: TrecQrel('TREC_Entity-9', '<dbpedia:Émile_Gilbert>', 0, '0'),
        })
        self._test_qrels('beir/fever/dev', count=8079, items={
            0: TrecQrel('137334', 'Soul_Food_(film)', 1, '0'),
            9: TrecQrel('105095', 'Carrie_Mathison', 1, '0'),
            8078: TrecQrel('46064', 'NAACP_Image_Award_for_Outstanding_Supporting_Actor_in_a_Drama_Series', 1, '0'),
        })
        self._test_qrels('beir/fever/test', count=7937, items={
            0: TrecQrel('163803', 'Ukrainian_Soviet_Socialist_Republic', 1, '0'),
            9: TrecQrel('54298', 'Electric_chair', 1, '0'),
            7936: TrecQrel('81957', 'Trouble_with_the_Curve', 1, '0'),
        })
        self._test_qrels('beir/fever/train', count=140085, items={
            0: TrecQrel('75397', 'Fox_Broadcasting_Company', 1, '0'),
            9: TrecQrel('226034', 'Tetris', 1, '0'),
            140084: TrecQrel('152180', 'Susan_Sarandon', 1, '0'),
        })
        self._test_qrels('beir/fiqa/dev', count=1238, items={
            0: TrecQrel('1', '14255', 1, '0'),
            9: TrecQrel('29', '189642', 1, '0'),
            1237: TrecQrel('11023', '579370', 1, '0'),
        })
        self._test_qrels('beir/fiqa/test', count=1706, items={
            0: TrecQrel('8', '566392', 1, '0'),
            9: TrecQrel('42', '331981', 1, '0'),
            1705: TrecQrel('11088', '437100', 1, '0'),
        })
        self._test_qrels('beir/fiqa/train', count=14166, items={
            0: TrecQrel('0', '18850', 1, '0'),
            9: TrecQrel('11', '596427', 1, '0'),
            14165: TrecQrel('11104', '518310', 1, '0'),
        })
        self._test_qrels('beir/hotpotqa/dev', count=10894, items={
            0: TrecQrel('5ae81fbf55429952e35eaa37', '6607768', 1, '0'),
            9: TrecQrel('5ae142a4554299422ee9964a', '1216600', 1, '0'),
            10893: TrecQrel('5a8bae0c5542996e8ac889b5', '690481', 1, '0'),
        })
        self._test_qrels('beir/hotpotqa/test', count=14810, items={
            0: TrecQrel('5a8b57f25542995d1e6f1371', '2816539', 1, '0'),
            9: TrecQrel('5a8e3ea95542995a26add48d', '5382358', 1, '0'),
            14809: TrecQrel('5ac132a755429964131be17c', '644341', 1, '0'),
        })
        self._test_qrels('beir/hotpotqa/train', count=170000, items={
            0: TrecQrel('5ab6d31155429954757d3384', '2921047', 1, '0'),
            9: TrecQrel('5adec8ad55429975fa854f8f', '202525', 1, '0'),
            169999: TrecQrel('5a7543b155429916b01642cd', '20527', 1, '0'),
        })
        self._test_qrels('beir/msmarco/dev', count=7437, items={
            0: TrecQrel('300674', '7067032', 1, '0'),
            9: TrecQrel('54544', '7068203', 1, '0'),
            7436: TrecQrel('195199', '8009377', 1, '0'),
        })
        self._test_qrels('beir/msmarco/test', count=9260, items={
            0: TrecQrel('19335', '1017759', 0, '0'),
            9: TrecQrel('19335', '1274615', 0, '0'),
            9259: TrecQrel('1133167', '977421', 0, '0'),
        })
        self._test_qrels('beir/msmarco/train', count=532751, items={
            0: TrecQrel('1185869', '0', 1, '0'),
            9: TrecQrel('186154', '1160', 1, '0'),
            532750: TrecQrel('405466', '8841735', 1, '0'),
        })
        self._test_qrels('beir/nfcorpus/dev', count=11385, items={
            0: TrecQrel('PLAIN-1', 'MED-2421', 2, '0'),
            9: TrecQrel('PLAIN-1', 'MED-4070', 1, '0'),
            11384: TrecQrel('PLAIN-3471', 'MED-5342', 2, '0'),
        })
        self._test_qrels('beir/nfcorpus/test', count=12334, items={
            0: TrecQrel('PLAIN-2', 'MED-2427', 2, '0'),
            9: TrecQrel('PLAIN-2', 'MED-2434', 1, '0'),
            12333: TrecQrel('PLAIN-3472', 'MED-3627', 1, '0'),
        })
        self._test_qrels('beir/nfcorpus/train', count=110575, items={
            0: TrecQrel('PLAIN-3', 'MED-2436', 1, '0'),
            9: TrecQrel('PLAIN-3', 'MED-2431', 1, '0'),
            110574: TrecQrel('PLAIN-3474', 'MED-4634', 1, '0'),
        })
        self._test_qrels('beir/nq', count=4201, items={
            0: TrecQrel('test0', 'doc0', 1, '0'),
            9: TrecQrel('test6', 'doc63', 1, '0'),
            4200: TrecQrel('test3451', 'doc117680', 1, '0'),
        })
        self._test_qrels('beir/quora/dev', count=7626, items={
            0: TrecQrel('318', '317', 1, '0'),
            9: TrecQrel('399', '364917', 1, '0'),
            7625: TrecQrel('537790', '537789', 1, '0'),
        })
        self._test_qrels('beir/quora/test', count=15675, items={
            0: TrecQrel('46', '134031', 1, '0'),
            9: TrecQrel('187', '188', 1, '0'),
            15674: TrecQrel('537876', '537875', 1, '0'),
        })
        self._test_qrels('beir/scidocs', count=29928, items={
            0: TrecQrel('78495383450e02c5fe817e408726134b3084905d', '632589828c8b9fca2c3a59e97451fde8fa7d188d', 1, '0'),
            9: TrecQrel('78495383450e02c5fe817e408726134b3084905d', '305c45fb798afdad9e6d34505b4195fa37c2ee4f', 0, '0'),
            29927: TrecQrel('89e58773fa59ef5b57f229832c2a1b3e3efff37e', 'dec997b20ebe2b867f68cc5c123d9cb9eafad6bb', 0, '0'),
        })
        self._test_qrels('beir/scifact/test', count=339, items={
            0: TrecQrel('1', '31715818', 1, '0'),
            9: TrecQrel('50', '12580014', 1, '0'),
            338: TrecQrel('1395', '17717391', 1, '0'),
        })
        self._test_qrels('beir/scifact/train', count=919, items={
            0: TrecQrel('0', '31715818', 1, '0'),
            9: TrecQrel('15', '22080671', 1, '0'),
            918: TrecQrel('1407', '29863668', 1, '0'),
        })
        self._test_qrels('beir/trec-covid', count=66336, items={
            0: TrecQrel('1', '005b2j4b', 2, '0'),
            9: TrecQrel('1', '05vx82oo', 0, '0'),
            66335: TrecQrel('50', 'zz8wvos9', 1, '0'),
        })
        self._test_qrels('beir/webis-touche2020', count=2962, items={
            0: TrecQrel('1', '197beaca-2019-04-18T11:28:59Z-00001-000', 4, '0'),
            9: TrecQrel('1', '24e47090-2019-04-18T19:22:46Z-00003-000', 3, '0'),
            2961: TrecQrel('50', '799d051-2019-04-18T11:47:02Z-00000-000', -2, '0'),
        })
        self._test_qrels('beir/webis-touche2020/v2', count=2214, items={
            0: TrecQrel('1', '197beaca-2019-04-18T11:28:59Z-00001-000', 0, '0'),
            9: TrecQrel('1', '4fb4627-2019-04-18T18:47:37Z-00003-000', 1, '0'),
            2213: TrecQrel('50', '4d1037f0-2019-04-18T11:08:29Z-00002-000', 2, '0'),
        })
        self._test_qrels('beir/cqadupstack/android', count=1696, items={
            0: TrecQrel('11546', '18572', 1, '0'),
            9: TrecQrel('82440', '78789', 1, '0'),
            1695: TrecQrel('61210', '61212', 1, '0'),
        })
        self._test_qrels('beir/cqadupstack/english', count=3765, items={
            0: TrecQrel('19399', '102236', 1, '0'),
            9: TrecQrel('19399', '4501', 1, '0'),
            3764: TrecQrel('76823', '31410', 1, '0'),
        })
        self._test_qrels('beir/cqadupstack/gaming', count=2263, items={
            0: TrecQrel('82449', '53562', 1, '0'),
            9: TrecQrel('46138', '42968', 1, '0'),
            2262: TrecQrel('146551', '28158', 1, '0'),
        })
        self._test_qrels('beir/cqadupstack/gis', count=1114, items={
            0: TrecQrel('52462', '49462', 1, '0'),
            9: TrecQrel('46866', '46762', 1, '0'),
            1113: TrecQrel('104332', '104331', 1, '0'),
        })
        self._test_qrels('beir/cqadupstack/mathematica', count=1358, items={
            0: TrecQrel('35544', '14789', 1, '0'),
            9: TrecQrel('48026', '47994', 1, '0'),
            1357: TrecQrel('25260', '26583', 1, '0'),
        })
        self._test_qrels('beir/cqadupstack/physics', count=1933, items={
            0: TrecQrel('110554', '21138', 1, '0'),
            9: TrecQrel('89378', '36242', 1, '0'),
            1932: TrecQrel('16082', '16081', 1, '0'),
        })
        self._test_qrels('beir/cqadupstack/programmers', count=1675, items={
            0: TrecQrel('88392', '203507', 1, '0'),
            9: TrecQrel('145437', '229691', 1, '0'),
            1674: TrecQrel('133937', '27335', 1, '0'),
        })
        self._test_qrels('beir/cqadupstack/stats', count=913, items={
            0: TrecQrel('11546', '66109', 1, '0'),
            9: TrecQrel('57083', '91074', 1, '0'),
            912: TrecQrel('35719', '35716', 1, '0'),
        })
        self._test_qrels('beir/cqadupstack/tex', count=5154, items={
            0: TrecQrel('197555', '12668', 1, '0'),
            9: TrecQrel('89372', '80', 1, '0'),
            5153: TrecQrel('84944', '84946', 1, '0'),
        })
        self._test_qrels('beir/cqadupstack/unix', count=1693, items={
            0: TrecQrel('103549', '2677', 1, '0'),
            9: TrecQrel('103549', '48253', 1, '0'),
            1692: TrecQrel('20536', '17664', 1, '0'),
        })
        self._test_qrels('beir/cqadupstack/webmasters', count=1395, items={
            0: TrecQrel('28994', '53865', 1, '0'),
            9: TrecQrel('11544', '52031', 1, '0'),
            1394: TrecQrel('65733', '65118', 1, '0'),
        })
        self._test_qrels('beir/cqadupstack/wordpress', count=744, items={
            0: TrecQrel('120122', '21561', 1, '0'),
            9: TrecQrel('114225', '78428', 1, '0'),
            743: TrecQrel('90939', '105803', 1, '0'),
        })



if __name__ == '__main__':
    unittest.main()
