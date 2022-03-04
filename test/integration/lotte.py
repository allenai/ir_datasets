import re
import unittest
import ir_datasets
from ir_datasets.formats import GenericDoc, GenericQuery, TrecQrel
from .base import DatasetIntegrationTest


class TestLotte(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('lotte/lifestyle/dev', count=268893, items={
            0: GenericDoc('0', re.compile('^In my experience rabbits are very easy to housebreak\\. They like to pee and poop in the same place ev.{1292}oiding kicking soiled litter out of the box, which is the biggest cause of failure in my experience\\.$', flags=48)),
            9: GenericDoc('9', re.compile('^There was a trick Cesar Milan \\(the Dog Whisperer\\) used to break a puppy of their fear of living in a.{445}sperer: http://movies\\.netflix\\.com/WiMovie/The_Very_Best_of_Dog_Whisperer_with_Cesar_Millan/70270440\\.$', flags=48)),
            268892: GenericDoc('268892', re.compile("^I would say there are two good choices, and several other choices\\. Find another junk yard or search .{117}d flat several years ago and it must not have been very expensive because I can't remember the cost\\.$", flags=48)),
        })
        self._test_docs('lotte/lifestyle/test', count=119461, items={
            0: GenericDoc('0', re.compile('^Normal double\\-acting baking powder makes CO2 \\(thus giving a rising effect\\) in two ways: when it gets.{581}ween mixing and baking less critical, and this is the type most widely available to consumers today\\.$', flags=48)),
            9: GenericDoc('9', 'Try bacon! It smells wonderful cooked on a plank.'),
            119460: GenericDoc('119460', re.compile('^\\[Will try to find some links supporting my answer\\] Although goalies are awarded shutouts when their .{519}is a goal allowed while no goalie in the net, neither the team or the goalie is awarded the shutout\\.$', flags=48)),
        })
        self._test_docs('lotte/recreation/dev', count=263025, items={
            0: GenericDoc('0', re.compile('^Multiclassing no longer takes an XP hit, and your favored class gives you one of two bonuses at ever.{1} level: \\+1 hp \\+1 skill point Advanced Players Guide add other options for specific Race/Class combos$', flags=48)),
            9: GenericDoc('9', re.compile("^Well, if we're only counting the WotC ones, in no particular order: Dark Sun \\-\\- 3\\.x material in Drag.{1609}not listing all the third party settings\\. Lots of those! A full list can also be found on Wikipedia\\.$", flags=48)),
            263024: GenericDoc('263024', re.compile('^Reflected light meters by necessity assume a normal reflectance level\\. At one time this was referred.{493} dress exposure more toward gray\\. Incident metering removes the guesswork as to reflectance effects\\.$', flags=48)),
        })
        self._test_docs('lotte/recreation/test', count=166975, items={
            0: GenericDoc('0', "No, you can fight the sword master without knowing all the comebacks. It's all a matter of luck which insults she throws at you."),
            9: GenericDoc('9', re.compile('^My suggestion would be to install DOSBox, and run it through the emulator\\. I use DOSBox for several .{146}er=S Here is a good guide for getting DOSBox setup: http://vogons\\.zetafleet\\.com/viewtopic\\.php\\?t=2502$', flags=48)),
            166974: GenericDoc('166974', re.compile("^As explained by EW: He's ultimately arrested because Cassie had a backup plan \\(a cache of incriminat.{332}\\. Whatever Cassie sent as proof of her disappearance \\(we never see it\\), the police find her remains\\.$", flags=48)),
        })
        self._test_docs('lotte/science/dev', count=343642, items={
            0: GenericDoc('0', re.compile("^As you move from left to right across a period, the number of protons in the nucleus increases\\. The .{347}lence electrons from the attractive effects of the atom's nucleus, so the atomic radius gets larger\\.$", flags=48)),
            9: GenericDoc('9', re.compile("^It's a very general statement, but it's not always true\\. I'll explain why it's often true, and give .{917}lly as the mix changes from pure silver to pure gold\\. The corresponding phase diagram is as follows:$", flags=48)),
            343641: GenericDoc('343641', re.compile('^Let\'s take a cold, hard \\(quasi\\-Kantian\\) look at what you\'re suggesting\\. So, if there is a one\\-in\\-a\\-b.{1851} to be\\." If we stop making up our minds to be miserable, suffering wretches, that\'s half the battle\\.$', flags=48)),
        })
        self._test_docs('lotte/science/test', count=1694164, items={
            0: GenericDoc('0', "More or Less is a BBC Radio 4 programme about maths and statistics in the news, and there is a free podcast. It's presented by Tim Harford, the Undercover Economist from the Financial Times."),
            9: GenericDoc('9', re.compile("^You can use Binet's formula, described at http://mathworld\\.wolfram\\.com/BinetsFibonacciNumberFormula\\..{5}\\(see also Wikipedia for a proof: http://en\\.wikipedia\\.org/wiki/Binet_formula\\#Closed_form_expression \\)$", flags=48)),
            1694163: GenericDoc('1694163', re.compile('^I think that "morph" is a substantially generic term that it would cover environmental as well as ge.{432}his is a well\\-known concept in entomology and no one will be angry/surprised when you use this term\\.$', flags=48)),
        })
        self._test_docs('lotte/technology/dev', count=1276222, items={
            0: GenericDoc('0', re.compile('^You definitely need some sort of software to filter out the noise\\. Some of the other answers here ca.{142}aker\\. By the way, just in case anyone is curious, these can be used for small desktop speakers, too\\.$', flags=48)),
            9: GenericDoc('9', re.compile("^su is a command to change to another user, either to run a shell or execute a specific command\\. You .{1647}meone leaves, or assuming that it's ok for them to have full access to your systems after you leave\\.$", flags=48)),
            1276221: GenericDoc('1276221', 'Try these formulas: Max: =MAXIFS(B3:B10,B3:B10,"<>#N/A") Min: =MINIFS(B3:B10,B3:B10,"<>#N/A") Sample'),
        })
        self._test_docs('lotte/technology/test', count=638509, items={
            0: GenericDoc('0', re.compile("^One option would be to clone your startup drive to an external disk using something like SuperDuper!.{217}y to run the repair\\. After you're done, re\\-select the internal drive as the Startup Disk and reboot\\.$", flags=48)),
            9: GenericDoc('9', re.compile('^This is so called "Hibernation" \\(my first met in windows\\)\\. When battery dies, the OS dumps whole RAM.{90}p, it loads the information back from HDD to RAM \\(hence you see the progress with those white bars\\)\\.$', flags=48)),
            638508: GenericDoc('638508', re.compile("^First of all, you don't use NAT between private subnets\\. You only use NAT when it is required, most .{1247} 19\\) permitting everything \\- remember that ACL rules are applied on a first\\-hit base, top to bottom\\.$", flags=48)),
        })
        self._test_docs('lotte/writing/dev', count=277072, items={
            0: GenericDoc('0', 'A native speaker would interpret them as having the same meaning. You could say "I\'m ill," or you could say "I\'m sick". "I\'m ill" could be classed as more formal language.'),
            9: GenericDoc('9', re.compile("^From a British perspective, I'm ill is more common and general term for when you're unwell\\. Being si.{1}k can refer to actually throwing up or vomiting, but it can also be used for being generally unwell\\.$", flags=48)),
            277071: GenericDoc('277071', re.compile("^Any investor who gets his hands on too many is targeted by the others, who aren't too concerned with.{442}a\\. Three almost ensures that one of them will figure it out\\. Much better to be faithful to one book\\.$", flags=48)),
        })
        self._test_docs('lotte/writing/test', count=199994, items={
            0: GenericDoc('0', "It's the fifth element after earth, air, fire, and water, so it is presumably superior to those or completing those."),
            9: GenericDoc('9', re.compile('^Here is a good description of when to use shall: \\.\\.\\.shall is used for the future tense with the firs.{761}nd which social echelons using "shall" in statements is actually still practiced by native speakers\\.$', flags=48)),
            199993: GenericDoc('199993', re.compile('^The following is a summary of the entry for the suffix "\\-ee" in the OED: The \\-ee suffix has two func.{526}"bootee", "setee", "goatee", etc\\., and does not fit your context\\. In short \\- modifyee does not work\\.$', flags=48)),
        })
        self._test_docs('lotte/pooled/dev', count=2428854, items={
            0: GenericDoc('0', 'A native speaker would interpret them as having the same meaning. You could say "I\'m ill," or you could say "I\'m sick". "I\'m ill" could be classed as more formal language.'),
            9: GenericDoc('9', re.compile("^From a British perspective, I'm ill is more common and general term for when you're unwell\\. Being si.{1}k can refer to actually throwing up or vomiting, but it can also be used for being generally unwell\\.$", flags=48)),
            2428853: GenericDoc('2428853', re.compile("^I would say there are two good choices, and several other choices\\. Find another junk yard or search .{117}d flat several years ago and it must not have been very expensive because I can't remember the cost\\.$", flags=48)),
        })
        self._test_docs('lotte/pooled/test', count=2819103, items={
            0: GenericDoc('0', "It's the fifth element after earth, air, fire, and water, so it is presumably superior to those or completing those."),
            9: GenericDoc('9', re.compile('^Here is a good description of when to use shall: \\.\\.\\.shall is used for the future tense with the firs.{761}nd which social echelons using "shall" in statements is actually still practiced by native speakers\\.$', flags=48)),
            2819102: GenericDoc('2819102', re.compile('^\\[Will try to find some links supporting my answer\\] Although goalies are awarded shutouts when their .{519}is a goal allowed while no goalie in the net, neither the team or the goalie is awarded the shutout\\.$', flags=48)),
        })

    def test_queries(self):
        self._test_queries('lotte/lifestyle/dev/forum', count=2076, items={
            0: GenericQuery('0', 'Why does my cat keep patting my face?'),
            9: GenericQuery('9', 'Is this normal first day home behavior for my kitten, or should I be concerned?'),
            2075: GenericQuery('2075', 'Direct Pull (V-Brake) vs. Center Pull Cantilevers (pros and cons)'),
        })
        self._test_queries('lotte/lifestyle/dev/search', count=417, items={
            0: GenericQuery('0', 'how much should i feed my 1 year old english mastiff?'),
            9: GenericQuery('9', 'is my corn snake male or female?'),
            416: GenericQuery('416', 'is there a difference between red and clear power steering fluid?'),
        })
        self._test_queries('lotte/lifestyle/test/forum', count=2002, items={
            0: GenericQuery('0', 'OK were all adults here, so really, how on earth should I use a squat toilet?'),
            9: GenericQuery('9', 'I dont know my nationality. How can I visit Denmark?'),
            2001: GenericQuery('2001', 'What is each side of a 4-sided grater for?'),
        })
        self._test_queries('lotte/lifestyle/test/search', count=661, items={
            0: GenericQuery('0', 'are clear pomegranate seeds good to eat?'),
            9: GenericQuery('9', 'is lumpy coconut milk ok?'),
            660: GenericQuery('660', 'is zone allowed in the nba?'),
        })
        self._test_queries('lotte/recreation/dev/forum', count=2002, items={
            0: GenericQuery('0', 'Would the One Ring even work for anyone but Sauron?'),
            9: GenericQuery('9', 'Which 2015 technologies were correctly predicted by Back to the Future II?'),
            2001: GenericQuery('2001', 'Does priority matter in Magic?'),
        })
        self._test_queries('lotte/recreation/dev/search', count=563, items={
            0: GenericQuery('0', 'do bards have to sing?'),
            9: GenericQuery('9', 'do attacks of opportunity stop movement?'),
            562: GenericQuery('562', 'are nikon and minolta lenses interchangeable?'),
        })
        self._test_queries('lotte/recreation/test/forum', count=2002, items={
            0: GenericQuery('0', 'How did they make cars fall apart in old movies?'),
            9: GenericQuery('9', 'Is the title The Last Jedi singular or plural?'),
            2001: GenericQuery('2001', 'Is there any specific reason why female voice actors act for male roles in anime?'),
        })
        self._test_queries('lotte/recreation/test/search', count=924, items={
            0: GenericQuery('0', 'how can you tell if someone blocked you on xbox one?'),
            9: GenericQuery('9', 'are xbox games compatible with ps4?'),
            923: GenericQuery('923', 'are laurel and hardy jewish?'),
        })
        self._test_queries('lotte/science/dev/forum', count=2013, items={
            0: GenericQuery('0', 'Making sense of principal component analysis, eigenvectors & eigenvalues'),
            9: GenericQuery('9', 'Bayesian and frequentist reasoning in plain English'),
            2012: GenericQuery('2012', 'How can I tell if I have simplified my talk too much?'),
        })
        self._test_queries('lotte/science/dev/search', count=538, items={
            0: GenericQuery('0', 'is sudan iv hydrophobic or hydrophilic?'),
            9: GenericQuery('9', 'how many atoms are present in one molecule of urea?'),
            537: GenericQuery('537', 'is both objective and subjective?'),
        })
        self._test_queries('lotte/science/test/forum', count=2017, items={
            0: GenericQuery('0', 'Cooling a cup of coffee with help of a spoon'),
            9: GenericQuery('9', 'Why dont metals bond when touched together?'),
            2016: GenericQuery('2016', 'Why does cracking a joint make noise?'),
        })
        self._test_queries('lotte/science/test/search', count=617, items={
            0: GenericQuery('0', 'mutually exclusive events are independent?'),
            9: GenericQuery('9', 'is tan x a function?'),
            616: GenericQuery('616', 'what is the relationship between polarity and hydrophobicity?'),
        })
        self._test_queries('lotte/technology/dev/forum', count=2003, items={
            0: GenericQuery('0', 'Strikethrough with GitHub Markdown'),
            9: GenericQuery('9', 'GitHub - Whats this Pro tag on my profile?'),
            2002: GenericQuery('2002', 'I have a hardware detection problem, what logs do I need to look into?'),
        })
        self._test_queries('lotte/technology/dev/search', count=916, items={
            0: GenericQuery('0', 'how many devices can you connect to bluetooth?'),
            9: GenericQuery('9', 'do docking stations have mac addresses?'),
            915: GenericQuery('915', 'what does it mean when someone is active but no green dot?'),
        })
        self._test_queries('lotte/technology/test/forum', count=2004, items={
            0: GenericQuery('0', 'Why does man print gimme gimme gimme at 00:30?'),
            9: GenericQuery('9', 'How do I grep for multiple patterns with pattern having a pipe character?'),
            2003: GenericQuery('2003', 'Can I automatically log in to open WiFi that requires web login/password?'),
        })
        self._test_queries('lotte/technology/test/search', count=596, items={
            0: GenericQuery('0', 'which ipods are no longer supported?'),
            9: GenericQuery('9', 'how to change the name of my apple pencil?'),
            595: GenericQuery('595', 'is ping tcp or udp?'),
        })
        self._test_queries('lotte/writing/dev/forum', count=2003, items={
            0: GenericQuery('0', 'The Rules of Writing'),
            9: GenericQuery('9', 'How do I translate into a gendered language where the gender would be a spoiler?'),
            2002: GenericQuery('2002', 'Can I say I Java, or does it have to be I do Java?'),
        })
        self._test_queries('lotte/writing/dev/search', count=497, items={
            0: GenericQuery('0', 'how are you doing lately meaning?'),
            9: GenericQuery('9', 'what is the difference between sign in and sign up?'),
            496: GenericQuery('496', 'can a tv screen be used as a camera?'),
        })
        self._test_queries('lotte/writing/test/forum', count=2000, items={
            0: GenericQuery('0', 'How do you quote a passage that has used [sic] mistakenly?'),
            9: GenericQuery('9', 'Is there a word or phrase for the feeling you get after looking at a word for too long?'),
            1999: GenericQuery('1999', 'Opposite of a diet'),
        })
        self._test_queries('lotte/writing/test/search', count=1071, items={
            0: GenericQuery('0', 'what is the difference between a college and an academy?'),
            9: GenericQuery('9', 'what is the difference between present continuous tense and past continuous tense?'),
            1070: GenericQuery('1070', 'what is the difference between pricey and pricey?'),
        })
        self._test_queries('lotte/pooled/dev/forum', count=10097, items={
            0: GenericQuery('0', 'The Rules of Writing'),
            9: GenericQuery('9', 'How do I translate into a gendered language where the gender would be a spoiler?'),
            10096: GenericQuery('10096', 'Direct Pull (V-Brake) vs. Center Pull Cantilevers (pros and cons)'),
        })
        self._test_queries('lotte/pooled/dev/search', count=2931, items={
            0: GenericQuery('0', 'how are you doing lately meaning?'),
            9: GenericQuery('9', 'what is the difference between sign in and sign up?'),
            2930: GenericQuery('2930', 'is there a difference between red and clear power steering fluid?'),
        })
        self._test_queries('lotte/pooled/test/forum', count=10025, items={
            0: GenericQuery('0', 'How do you quote a passage that has used [sic] mistakenly?'),
            9: GenericQuery('9', 'Is there a word or phrase for the feeling you get after looking at a word for too long?'),
            10024: GenericQuery('10024', 'What is each side of a 4-sided grater for?'),
        })
        self._test_queries('lotte/pooled/test/search', count=3869, items={
            0: GenericQuery('0', 'what is the difference between a college and an academy?'),
            9: GenericQuery('9', 'what is the difference between present continuous tense and past continuous tense?'),
            3868: GenericQuery('3868', 'is zone allowed in the nba?'),
        })

    def test_qrels(self):
        self._test_qrels('lotte/lifestyle/dev/forum', count=12823, items={
            0: TrecQrel('0', '116', 1, '0'),
            9: TrecQrel('1', '9573', 1, '0'),
            12822: TrecQrel('2075', '229252', 1, '0'),
        })
        self._test_qrels('lotte/lifestyle/dev/search', count=1376, items={
            0: TrecQrel('0', '1615', 1, '0'),
            9: TrecQrel('1', '7323', 1, '0'),
            1375: TrecQrel('416', '255775', 1, '0'),
        })
        self._test_qrels('lotte/lifestyle/test/forum', count=10278, items={
            0: TrecQrel('0', '50103', 1, '0'),
            9: TrecQrel('0', '60904', 1, '0'),
            10277: TrecQrel('2001', '29723', 1, '0'),
        })
        self._test_qrels('lotte/lifestyle/test/search', count=1804, items={
            0: TrecQrel('0', '14700', 1, '0'),
            9: TrecQrel('2', '5365', 1, '0'),
            1803: TrecQrel('660', '112612', 1, '0'),
        })
        self._test_qrels('lotte/recreation/dev/forum', count=12752, items={
            0: TrecQrel('0', '130975', 1, '0'),
            9: TrecQrel('0', '135480', 1, '0'),
            12751: TrecQrel('2001', '107742', 1, '0'),
        })
        self._test_qrels('lotte/recreation/dev/search', count=1754, items={
            0: TrecQrel('0', '37577', 1, '0'),
            9: TrecQrel('4', '26042', 1, '0'),
            1753: TrecQrel('562', '224939', 1, '0'),
        })
        self._test_qrels('lotte/recreation/test/forum', count=6947, items={
            0: TrecQrel('0', '156504', 1, '0'),
            9: TrecQrel('2', '137362', 1, '0'),
            6946: TrecQrel('2001', '128682', 1, '0'),
        })
        self._test_qrels('lotte/recreation/test/search', count=1991, items={
            0: TrecQrel('0', '38021', 1, '0'),
            9: TrecQrel('5', '67168', 1, '0'),
            1990: TrecQrel('923', '145431', 1, '0'),
        })
        self._test_qrels('lotte/science/dev/forum', count=12271, items={
            0: TrecQrel('0', '41234', 1, '0'),
            9: TrecQrel('0', '43389', 1, '0'),
            12270: TrecQrel('2012', '245863', 1, '0'),
        })
        self._test_qrels('lotte/science/dev/search', count=1480, items={
            0: TrecQrel('0', '17292', 1, '0'),
            9: TrecQrel('5', '28427', 1, '0'),
            1479: TrecQrel('537', '331634', 1, '0'),
        })
        self._test_qrels('lotte/science/test/forum', count=15515, items={
            0: TrecQrel('0', '1468504', 1, '0'),
            9: TrecQrel('0', '1468548', 1, '0'),
            15514: TrecQrel('2016', '1677617', 1, '0'),
        })
        self._test_qrels('lotte/science/test/search', count=1738, items={
            0: TrecQrel('0', '396636', 1, '0'),
            9: TrecQrel('1', '417869', 1, '0'),
            1737: TrecQrel('616', '1675847', 1, '0'),
        })
        self._test_qrels('lotte/technology/dev/forum', count=15741, items={
            0: TrecQrel('0', '1248849', 1, '0'),
            9: TrecQrel('1', '1274089', 1, '0'),
            15740: TrecQrel('2002', '658976', 1, '0'),
        })
        self._test_qrels('lotte/technology/dev/search', count=2676, items={
            0: TrecQrel('0', '281401', 1, '0'),
            9: TrecQrel('3', '314964', 1, '0'),
            2675: TrecQrel('915', '1270172', 1, '0'),
        })
        self._test_qrels('lotte/technology/test/forum', count=15890, items={
            0: TrecQrel('0', '319429', 1, '0'),
            9: TrecQrel('1', '310098', 1, '0'),
            15889: TrecQrel('2003', '133429', 1, '0'),
        })
        self._test_qrels('lotte/technology/test/search', count=2045, items={
            0: TrecQrel('0', '101347', 1, '0'),
            9: TrecQrel('1', '101391', 1, '0'),
            2044: TrecQrel('595', '637834', 1, '0'),
        })
        self._test_qrels('lotte/writing/dev/forum', count=15098, items={
            0: TrecQrel('0', '113455', 1, '0'),
            9: TrecQrel('0', '113612', 1, '0'),
            15097: TrecQrel('2002', '83828', 1, '0'),
        })
        self._test_qrels('lotte/writing/dev/search', count=1287, items={
            0: TrecQrel('0', '9032', 1, '0'),
            9: TrecQrel('4', '29678', 1, '0'),
            1286: TrecQrel('496', '246118', 1, '0'),
        })
        self._test_qrels('lotte/writing/test/forum', count=12906, items={
            0: TrecQrel('0', '14298', 1, '0'),
            9: TrecQrel('0', '14356', 1, '0'),
            12905: TrecQrel('1999', '179657', 1, '0'),
        })
        self._test_qrels('lotte/writing/test/search', count=3546, items={
            0: TrecQrel('0', '84481', 1, '0'),
            9: TrecQrel('4', '13250', 1, '0'),
            3545: TrecQrel('1070', '19407', 1, '0'),
        })
        self._test_qrels('lotte/pooled/dev/forum', count=68685, items={
            0: TrecQrel('0', '113455', 1, '0'),
            9: TrecQrel('0', '113612', 1, '0'),
            68684: TrecQrel('10096', '2389213', 1, '0'),
        })
        self._test_qrels('lotte/pooled/dev/search', count=8573, items={
            0: TrecQrel('0', '9032', 1, '0'),
            9: TrecQrel('4', '29678', 1, '0'),
            8572: TrecQrel('2930', '2415736', 1, '0'),
        })
        self._test_qrels('lotte/pooled/test/forum', count=61536, items={
            0: TrecQrel('0', '14298', 1, '0'),
            9: TrecQrel('0', '14356', 1, '0'),
            61535: TrecQrel('10024', '2729365', 1, '0'),
        })
        self._test_qrels('lotte/pooled/test/search', count=11124, items={
            0: TrecQrel('0', '84481', 1, '0'),
            9: TrecQrel('4', '13250', 1, '0'),
            11123: TrecQrel('3868', '2812254', 1, '0'),
        })


if __name__ == '__main__':
    unittest.main()
