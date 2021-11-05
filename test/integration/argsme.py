from unittest import main

from ir_datasets.formats import ArgsMeDoc, ArgsMePremise, ArgsMeStance
from test.integration.base import DatasetIntegrationTest


class TestArgsMe(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('argsme/1.0', count=387692, items={
            0: ArgsMeDoc(
                doc_id="c67482ba-2019-04-18T13:32:05Z-00000-000",
                conclusion="Contraceptive Forms for High School Students",
                premises=[
                    ArgsMePremise(
                        text="My opponent forfeited every round. None of my arguments were answered. I don’t like the idea of winning by default, but here we are.Tule: it’s good for students to get involved and address big issues like teen pregnancy. You need to be able to answer arguments like mine and not simply prepare for an abstinence-only type of response. You should also be aware that, in the U.S., condoms may be sold to minors in ANY state. A retailer who says it is illegal to sell you them is, frankly, wrong.",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            1: ArgsMeDoc(
                doc_id="c67482ba-2019-04-18T13:32:05Z-00001-000",
                conclusion="Contraceptive Forms for High School Students",
                premises=[
                    ArgsMePremise(
                        text="How do you propose the school will fund your program? Condoms cost money and checking an \"opt out\" list before handing them out takes time away from staff members whenever they could be doing their actual jobs. Your \"opt out\" option is only be a token to parental authority and would be easily subverted. If everyone in school except a handful of students had access to free condoms, do you not think those students would simply ask their friends to provide them with condoms?",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            387690: ArgsMeDoc(
                doc_id="671509c8-2019-04-17T11:47:34Z-00022-000",
                conclusion="Charter schools",
                premises=[
                    ArgsMePremise(
                        text="Charter schools are damaging private schools",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            387691: ArgsMeDoc(
                doc_id="671509c8-2019-04-17T11:47:34Z-00007-000",
                conclusion="Charter schools",
                premises=[
                    ArgsMePremise(
                        text="Charter schools are exploited most by affable students",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
        }, test_iter_split=False)
        self._test_docs('argsme/1.0-cleaned', count=382545, items={
            0: ArgsMeDoc(
                doc_id="c67482ba-2019-04-18T13:32:05Z-00000-000",
                conclusion="Contraceptive Forms for High School Students",
                premises=[
                    ArgsMePremise(
                        text="My opponent forfeited every round. None of my arguments were answered. I don’t like the idea of winning by default, but here we are.Tule: it’s good for students to get involved and address big issues like teen pregnancy. You need to be able to answer arguments like mine and not simply prepare for an abstinence-only type of response. You should also be aware that, in the U.S., condoms may be sold to minors in ANY state. A retailer who says it is illegal to sell you them is, frankly, wrong.",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            1: ArgsMeDoc(
                doc_id="c67482ba-2019-04-18T13:32:05Z-00001-000",
                conclusion="Contraceptive Forms for High School Students",
                premises=[
                    ArgsMePremise(
                        text="How do you propose the school will fund your program? Condoms cost money and checking an \"opt out\" list before handing them out takes time away from staff members whenever they could be doing their actual jobs. Your \"opt out\" option is only be a token to parental authority and would be easily subverted. If everyone in school except a handful of students had access to free condoms, do you not think those students would simply ask their friends to provide them with condoms?",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            382543: ArgsMeDoc(
                doc_id="671509c8-2019-04-17T11:47:34Z-00022-000",
                conclusion="Charter schools",
                premises=[
                    ArgsMePremise(
                        text="Charter schools are damaging private schools",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            382544: ArgsMeDoc(
                doc_id="671509c8-2019-04-17T11:47:34Z-00007-000",
                conclusion="Charter schools",
                premises=[
                    ArgsMePremise(
                        text="Charter schools are exploited most by affable students",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
        }, test_iter_split=False)
        self._test_docs('argsme/2020-04-01', count=387740, items={
            0: ArgsMeDoc(
                doc_id="Sb38112c8-A443a9828",
                conclusion="school",
                premises=[
                    ArgsMePremise(
                        text="Done.",
                        stance=ArgsMeStance.PRO,
                        annotations=[]
                    )
                ]
            ),
            1: ArgsMeDoc(
                doc_id="Sb38112c8-A48f87c39",
                conclusion="school",
                premises=[
                    ArgsMePremise(
                        text="Let's get this over with.",
                        stance=ArgsMeStance.PRO,
                        annotations=[]
                    )
                ]
            ),
            387738: ArgsMeDoc(
                doc_id="S18eb2e1a-Afe2ebde7",
                conclusion="Single female seniors are particularly challenged.",
                premises=[
                    ArgsMePremise(
                        text="Canada's economy may be doing well, but across the country there are millions of Canadians who live in poverty. This is especially true for women and children, as has been reported across the country today. Single female seniors are particularly challenged. One report provides the example of a single female senior who, having worked all her life to raise her three children, has had to give up her car, buy second-hand clothes and live on combined benefits of $16,000 per year. The United Nations agency for children, UNICEF, reports that in Canada “our children are suffering from unacceptable rates of poverty”. The levels of poverty in this country, especially for women and children, are totally unacceptable. The Leader of the Opposition has recently outlined real and meaningful plans to deal with the issue of poverty in Canada, but from the government we hear nothing. That is an absolute shame. Canadians deserve better.",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            387739: ArgsMeDoc(
                doc_id="S153cd52f-A118dded8",
                conclusion="The recent throne speech said that the government would work with Canadians to ensure that our communities continue to be safe.",
                premises=[
                    ArgsMePremise(
                        text="How can Canadians trust the Liberals when they say they will protect children and then avoid positive action on the possession of child pornography? The recent throne speech said that the government would work with Canadians to ensure that our communities continue to be safe. Its focus will be balanced, combining prevention and a community centred approach with action to deal with serious crime. Child pornography is a serious crime and in response on Friday 300,000 Canadians voiced their community-centred approach through a petition against child pornography insisting the government defend the law. In response, the justice minister accuses Reform members of being scaremongers. Obviously the minister does not feel obligated to the community will, and also has no ability to get cabinet approval for action. Children are the most vulnerable members of society and they deserve the fullest protection of the law. Liberal sentiments delivered in regal fashion do not close legal loopholes or defend families. The poor Liberal justice system will only be improved when the system defenders are replaced by the system changers in the opposition.",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
        }, test_iter_split=False)
        self._test_docs('argsme/2020-04-01/debateorg', count=338620, items={
            0: ArgsMeDoc(
                doc_id="Sb38112c8-A443a9828",
                conclusion="school",
                premises=[
                    ArgsMePremise(
                        text="Done.",
                        stance=ArgsMeStance.PRO,
                        annotations=[]
                    )
                ]
            ),
            1: ArgsMeDoc(
                doc_id="Sb38112c8-A48f87c39",
                conclusion="school",
                premises=[
                    ArgsMePremise(
                        text="Let's get this over with.",
                        stance=ArgsMeStance.PRO,
                        annotations=[]
                    )
                ]
            ),
            338618: ArgsMeDoc(
                doc_id="Sca72da7d-Ae83ab999",
                conclusion="It Should be Legal in the U.S. to Occasionally Hit Someone",
                premises=[
                    ArgsMePremise(
                        text="Thanks to my opponent for instigating this debate, I accept his definitions. ---- It should be pointed out that in some cases it is of course perfectly legal to hit someone. No one is going to be prosecuted for a genuinely playful slap, parents are allowed to discipline minors, you can use reasonable force to defend yourself from attack and some schools still administer occasional corporal punishment under signed consent. [1] There are probably other examples but I will not take the semantic route of invalidating a resolution that calls for the instigation of laws that already exist. I will instead debate my opponent on his opinions. In any case my opponent's definitions make it clear that he advocates delivering blows against just \"some person\" from \"time to time\" which covers just about any instance of provoked or unprovoked hitting. If people were given license to wander around delivering blows to random people who happen to annoy them, the world would be a worse place. I will be proving that in almost all cases a law permitting this kind of hitting is unjust and should not be accepted. ----- 1) Stress relief. Blatantly flawed. Hitting causes more stress than it relieves because being hit is very stressful indeed whereas hitting someone while it may be satisfying does not relieve much. My opponent should provide a source to prove that it can release any stress at all. If you hit someone and then they pull out a knife or a gun, you will find your stress levels increase dramatically. 2) Education. Being hit may teach you not to repeat the exact behaviour that led to being hit but certainly does not teach right from wrong, merely that might is right. Educators agree that self-esteem and confidence are essential to effective learning. Being hit negatively effects these. [2] Hitting someone and not facing the consequences or being compelled to regret it and apologise teaches people that it is OK and even beneficial to behave this way and it certainly isn't. Hitting people will not help you keep friends or hold down a job. 3) Conflict resolution. Fights are less likely to resolve conflicts than exacerbate them, look at the pattern of gang violence where a word out of place can quickly escalate to fists and then even more quickly onto weaponry. People who have been hit may well wish to get some form of revenge. In addition even if it is conceivable that more fights would save on prosecution costs (which I doubt for the reasons above) they can also have a negative economic impact as an injured individual will have to take time off work and will therefore not be contributing to the economy. 4) Exercise. There are far less stressful and more pleasurable forms of exercise even without setting foot in a gym. Sex between consenting adults is a great example. [3] 5) Breeding. By rewarding violence, natural selection will favour the aggressive and the impulsive rather than the enlightened and the thoughtful, not a good end result for species survival. 6) American doctrine. I'm far less informed on this subject than I'm sure my opponent and most readers will be but it is my understanding that an individual's rights are not allowed to infringe on those of another person. A phrase summing this up that I have heard used by many Americans on this site and elsewhere is that \"your right to swing your fist ends where my face begins.\" While I am not suggesting that that is the actual wording of the constitution, I was under the impression that it was a para-phrasing of some of the principles laid out in it. ---- If my opponent really thinks that hitting is acceptable, I can only assume that either he has never taken a good beating himself or he has taken one too many and is suffering from mild brain damage. I'm not a big guy but I'm a grown man and my opponent is a 14 year-old youth, if I lamped him in the head I think he would (rightfully) want me to be punished. ---- That will be all for now. Con. ----- Sources: [1] http://www.corpun.com... [2] http://www.thelearningweb.net... [3] http://www.healthcentral.com...",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            338619: ArgsMeDoc(
                doc_id="Sca72da7d-Adbd84fd2",
                conclusion="It Should be Legal in the U.S. to Occasionally Hit Someone",
                premises=[
                    ArgsMePremise(
                        text="In this debate, I will argue that occasionally hitting someone should be legal in the U.S. Here are some definitions. Hit: \"b: to deliver (as a blow) by action c: to apply forcefully or suddenly\" http://www.merriam-webster.com...[1] Occasionally: \"on occasion : now and then\" http://www.merriam-webster.com... Someone: \"some person : somebody\" http://www.merriam-webster.com... U.S.: the country known as the United States of America Before I state my arguments, I will clarify that I do not mean hitting with intent to cause permanent physical injury or death. I mean an occasional slap upside the head, or a good punch. Of course, there should be other limitations, such as instances were the action becomes addicting. Argument 1) Stress relief. By occasionally slapping/hitting someone, a great deal of stress is released by the person doing the hitting, especially if they are hitting the person causing the stress. This would decrease general feelings of anger and animosity, which could possibly result in criminal activity. Argument 2) Education. By being hit, a person learns instinctively to avoid the action which caused the hitting. Think of the things that could be learned through such a method of teaching! Argument 3) Conflict resolution. By engaging in fistfights instead of going to the police every time there is conflict, citizens will save many taxpayer dollars. Argument 4) Exercise. We all know that obesity is a problem in America. By legalizing hitting, Americans can resolve their problems while toughening up their fat, weakling bodies. This would result in longer lifespans, greater quality of life, and improved confidence. Argument 5) Breeding. By allowing hitting, certain individuals will pwn others. This will aid natural selection, as the wimpier individuals will not find mates. This will greatly improve the awesomeness of the general American population. Argument 6) American doctrine. As Americans, we believe that all Men have the right to Life, Liberty, and the Pursuit of Happiness. By thwacking the head of the annoying person next to me, I pursue Happiness, as the action gives me great pleasure. The person next to me, who may not initially enjoy the thwacking, will ultimately benefit from it; he will cease to be annoying, which will improve his overall chances of survival. Thus he is granted additional Life. My opponent may begin his argument. Good luck.",
                        stance=ArgsMeStance.PRO,
                        annotations=[]
                    )
                ]
            ),
        }, test_iter_split=False)
        self._test_docs('argsme/2020-04-01/debatepedia', count=21197, items={
            0: ArgsMeDoc(
                doc_id="S96f2396e-Aaf079b43",
                conclusion="Mine Ban Treaty (Ottawa Treaty)",
                premises=[
                    ArgsMePremise(
                        text="Casualties in repelling N. Korean invasion would be higher w/o mines",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            1: ArgsMeDoc(
                doc_id="S96f2396e-Afcc9ac26",
                conclusion="Mine Ban Treaty (Ottawa Treaty)",
                premises=[
                    ArgsMePremise(
                        text="Mine Ban Treaty fails to distinguish between different kinds of mines.",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            21195: ArgsMeDoc(
                doc_id="S148bb110-A63b9848c",
                conclusion="Very few sites globally are appropriate for tidal energy",
                premises=[
                    ArgsMePremise(
                        text="\"Tidal Energy\". Unisun.net.: \"Similar to other ocean energies, tidal energy has several prerequisites that make it only available in a small number of regions. For a tidal power plant to produce electricity effectively (about 85% efficiency), it requires a basin or a gulf that has a mean tidal amplitude (the differences between spring and neap tide) of 7 meters or above. It is also desirable to have semi-diurnal tides where there are two high and low tides everyday. Tides out in the ocean have maximum amplitude of about one meter. As you move closer to shore, this can increase to as high as 12 or more. This can depend on local features such as shelving or funneling meaning the tidal range can vary considerably along any given coastline. This can mean that a lot of places just aren't suitable. When planning the location major consideration has to be given to see whether the tides ar high enough and if there is a suitable place for building the site.\"",
                        stance=ArgsMeStance.PRO,
                        annotations=[]
                    )
                ]
            ),
            21196: ArgsMeDoc(
                doc_id="S148bb110-A119d66b0",
                conclusion="Environmental impact of barages is ugly.",
                premises=[
                    ArgsMePremise(
                        text="Barages are fairly massive objects, like Dams, that obstruct the natural flow of water and can, subsequently, have harmful environmental impacts. These effects can be very ugly, causing frustration among locals and possibly reduced property values and tourism.",
                        stance=ArgsMeStance.PRO,
                        annotations=[]
                    )
                ]
            ),
        }, test_iter_split=False)
        self._test_docs('argsme/2020-04-01/debatewise', count=14353, items={
            0: ArgsMeDoc(
                doc_id="S5920cdef-A982becb7",
                conclusion="placebo effect and phenylthiamine",
                premises=[
                    ArgsMePremise(
                        text="But are chocolate eaters happy? [[http://news.bbc.co.uk/2/hi/health/8644016.stm]] research suggests that those who eat regular amounts of chocolate are generally depressive/glum/sad. i dont think that chocolate is for us!and i also dont think that that it makes us happy.because if we eat a chocolate it tastes very delicious but we are not satisfied we keep eating more and more and this causes certain diseases like diabeties occurs to small children ,they spoil there teeth ,worms in the intestines ,and i say that those who eat chocolates they are happy but only for time period because everyone will suffer after having the diseases and so much of fat is also not good health !!! as it increases our colestrol in body.",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            1: ArgsMeDoc(
                doc_id="S5920cdef-Ab688a6bc",
                conclusion="phytonutrients",
                premises=[
                    ArgsMePremise(
                        text="These nutrients are mostly burned off during the making/processing of chocolate.",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            14351: ArgsMeDoc(
                doc_id="Sc47177f2-A3780249",
                conclusion="Hitler is evil",
                premises=[
                    ArgsMePremise(
                        text="i personally think that hitler was madder then mad he killed a lot of people he killed hi German shepored that he loved a lot. he got inicent litle disabled babies killed with out thinking twice so he killed a poupilation of pure Germans. so that means he was out of his mind beacause how can you get babies killed. and in the end he was so scared that he commited suicide. so that proves it that he was a coward a murdrer a madman i mean he should have been in a mental ayslum instead of the fuher of Germany",
                        stance=ArgsMeStance.PRO,
                        annotations=[]
                    )
                ]
            ),
            14352: ArgsMeDoc(
                doc_id="Sc47177f2-A730d8f9e",
                conclusion="NO",
                premises=[
                    ArgsMePremise(
                        text="\u200e:/ Adolf Hitler was not evil he wasn't a murderer he didn't kill the jews directly he ordered people to kill the jews. you might be thinking \" he ordered the soldiers to kill them so he has to be evil.\" well your wrong in my point of view its like this,if the U.S President Or Whoever Is In Power Told You To Jump Off A Bridge Would You Do It?",
                        stance=ArgsMeStance.PRO,
                        annotations=[]
                    )
                ]
            ),
        }, test_iter_split=False)
        self._test_docs('argsme/2020-04-01/idebate', count=13522, items={
            0: ArgsMeDoc(
                doc_id="Sf9294c83-Af186e851",
                conclusion="the War in Iraq was Worth the Cost",
                premises=[
                    ArgsMePremise(
                        text="His removal provides stability and security not only for Iraq but for the Middle East as a region",
                        stance=ArgsMeStance.PRO,
                        annotations=[]
                    )
                ]
            ),
            1: ArgsMeDoc(
                doc_id="Sf9294c83-A9a4e056e",
                conclusion="Saddam Hussein is gone and Iraq is now functioning as one of very few democracies in the Middle East",
                premises=[
                    ArgsMePremise(
                        text="It's important to be clear that this debate is looking at the results of the Iraq war and, by any definition Iraq is in a much more stable and secure position than it was in 2003 when American, British and other international troops arrived in the country. Whatever one thinks of the initial justifications for the war there is no doubt that the country, the region and the world are better and safer places without Saddam Hussein[i]. It is easy to criticize the allies but it is worth bearing in mind that the alternative was leaving in power a man who had committed genocide was a vicious and brutal dictator under whose regime extra-judicial execution and detention, mass-murder and torture were commonplace[ii]. [i] Richard Miniter. “Was the Iraq War Worth It?”. Hudson New York. 2 September 2010. [ii] Interview with Donald Rumsfeld. Inside Politics. NPR. 14 February 2011.",
                        stance=ArgsMeStance.PRO,
                        annotations=[]
                    )
                ]
            ),
            13520: ArgsMeDoc(
                doc_id="Sd6cf79d9-A82ab9dc3",
                conclusion="Socialism leads to a more humane equal society",
                premises=[
                    ArgsMePremise(
                        text="The reasons behind the poverty gap are not purely because of a capitalist expansion; a clear example may be seen at the development of the African region between the 1960. Free market economics also provides the solution to such inequality; labor will gravitate towards companies which provide the best working conditions and wages. For example, while most automobile companies offered two dollars per day as wages, Henry ford offered five, guaranteeing him the best of the best by way of labor. The important point is that the employers do not enslave the workers, the workers are more than free to try to find better employment, be it in better pay, better conditions, easier work, better benefits or more satisfaction.",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            13521: ArgsMeDoc(
                doc_id="Sd6cf79d9-Af8d9e187",
                conclusion="Socialism is a more secure system than the free market in Capitalism",
                premises=[
                    ArgsMePremise(
                        text="In order to avoid economic crisis there is a need to return to a separation of commercial banking from investment banking which was e.g. implemented as legislation in the U.S.A. under the 1933 Glass-Steagall Act (scrapped under President Clinton in the 1990s). It is dangerous to allow banks to get into a position where they can be shut down by pursuing exciting, but high risk investment banking activities such as real estate speculations. The rationale for this separation is that it was a commercial banking crisis which posed the systemic risk, investment banks should be left alone from state interference and left to the influence of the market. \"This leaves a much more limited, and practicable, but still absolutely essential, role for bank supervision and regulation: namely, to ensure that the core commercial banking system is thoroughly sound and adequately capitalised at all times. The crisis can thus be resolved through a separation of the banks since the commercial banking won't be affected when investment banks go bust, the whole system will not be dragged down if only a few investment banks misbehave since commercial banks are the backbone of the economy. Financial crisis doesn't have to be something \"inherent\" in the capitalist system due to overproduction but can be accommodated through some regulations1. 1 Lawson, N. (2009). Capitalism needs a revived Glass-Steagall. Financial Times. Retrieved June 14, 2011 1.",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
        }, test_iter_split=False)
        self._test_docs('argsme/2020-04-01/parliamentary', count=48, items={
            0: ArgsMeDoc(
                doc_id="S1f6b58eb-A5c530110",
                conclusion="I want them to know that their braids, their dreads, their super curly Afro puffs, their weaves, their hijabs, and their head scarves, and all other variety of hairstyles, belong in schools, in the workplace, in the boardroom, and yes, even here on Parliament Hill.",
                premises=[
                    ArgsMePremise(
                        text="This week I have my hair in braids, much like I have had for most of my childhood. However, it has come to my attention that there are young girls here in Canada and other parts of the world who are removed from school, or shamed because of their hairstyle. Body shaming of any woman in any form from the top of her head to the soles of her feet is wrong, irrespective of her hairstyle, the size of her thighs, the size of her hips, the size of her baby bump, the size of her breasts, or the size of her lips. What makes is different makes us unique and beautiful. I will continue to rock these braids for three reasons. Number one, because I am sure everyone will agree, they look pretty dope; number two, in solidarity with women who have been shamed based on their appearance; and number three, and most importantly, in solidarity with young girls and women who look like me and those who do not. I want them to know that their braids, their dreads, their super curly Afro puffs, their weaves, their hijabs, and their head scarves, and all other variety of hairstyles, belong in schools, in the workplace, in the boardroom, and yes, even here on Parliament Hill.",
                        stance=ArgsMeStance.PRO,
                        annotations=[]
                    )
                ]
            ),
            1: ArgsMeDoc(
                doc_id="S1a9db4fc-Acc4206f5",
                conclusion="On November 10, UN Women elected its first executive board.",
                premises=[
                    ArgsMePremise(
                        text="On November 10, UN Women elected its first executive board. This new agency “will work...to improve the status of women and girls”, said United Nations Deputy Secretary-General Asha-Rose Migiro. This entity, charged with ensuring that the UN's commitments to establish gender equality within its institutions are kept, is a competent authority among member states for bringing about this equality. It is troubling to see that Saudi Arabia has a seat on this executive board. The customs and traditions of that country infringe on women's rights daily. It is a country where Nathalie Morin, a Quebecker, is stuck and being held prisoner, along with her children. If Saudi Arabia wants to demonstrate its desire for equality and justice between men and women, then its authorities have to take the first concrete step and let Nathalie Morin and her children return to Quebec.",
                        stance=ArgsMeStance.PRO,
                        annotations=[]
                    )
                ]
            ),
            46: ArgsMeDoc(
                doc_id="S18eb2e1a-Afe2ebde7",
                conclusion="Single female seniors are particularly challenged.",
                premises=[
                    ArgsMePremise(
                        text="Canada's economy may be doing well, but across the country there are millions of Canadians who live in poverty. This is especially true for women and children, as has been reported across the country today. Single female seniors are particularly challenged. One report provides the example of a single female senior who, having worked all her life to raise her three children, has had to give up her car, buy second-hand clothes and live on combined benefits of $16,000 per year. The United Nations agency for children, UNICEF, reports that in Canada “our children are suffering from unacceptable rates of poverty”. The levels of poverty in this country, especially for women and children, are totally unacceptable. The Leader of the Opposition has recently outlined real and meaningful plans to deal with the issue of poverty in Canada, but from the government we hear nothing. That is an absolute shame. Canadians deserve better.",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
            47: ArgsMeDoc(
                doc_id="S153cd52f-A118dded8",
                conclusion="The recent throne speech said that the government would work with Canadians to ensure that our communities continue to be safe.",
                premises=[
                    ArgsMePremise(
                        text="How can Canadians trust the Liberals when they say they will protect children and then avoid positive action on the possession of child pornography? The recent throne speech said that the government would work with Canadians to ensure that our communities continue to be safe. Its focus will be balanced, combining prevention and a community centred approach with action to deal with serious crime. Child pornography is a serious crime and in response on Friday 300,000 Canadians voiced their community-centred approach through a petition against child pornography insisting the government defend the law. In response, the justice minister accuses Reform members of being scaremongers. Obviously the minister does not feel obligated to the community will, and also has no ability to get cabinet approval for action. Children are the most vulnerable members of society and they deserve the fullest protection of the law. Liberal sentiments delivered in regal fashion do not close legal loopholes or defend families. The poor Liberal justice system will only be improved when the system defenders are replaced by the system changers in the opposition.",
                        stance=ArgsMeStance.CON,
                        annotations=[]
                    )
                ]
            ),
        }, test_iter_split=False)


if __name__ == '__main__':
    main()
