import ir_datasets
from ir_datasets.formats import GenericQuery, GenericDoc, TrecQrel
from ir_datasets.datasets.sara import SaraDoc
from .base import DatasetIntegrationTest
import unittest
import re

class TestSara(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('sara', count=129821, items={
            55555: SaraDoc('gladly-united-eagle', "----- Forwarded by Jeff Dasovich/NA/Enron on 12/21/2000 10:04 AM ----- Jeff Dasovich Sent by: Jeff Dasovich 12/20/2000 08:16 PM To: skean@enron.com, Richard Shapiro/NA/Enron@Enron, Susan J Mara/NA/Enron@ENRON, Sandra McCubbin/NA/Enron@Enron, Paul Kaufman/PDX/ECT@ECT, James D Steffes/NA/Enron@Enron, Harry Kingerski/NA/Enron@Enron, Wanda Curry/HOU/EES@EES, Dennis Benevides/HOU/EES@EES, Roger Yang/SFO/EES@EES, Scott Stoness/HOU/EES@EES, Mary Hain/HOU/ECT@ECT, Alan Comnes/PDX/ECT@ECT, Joe Hartsoe/Corp/Enron@ENRON, Sarah Novosel/Corp/Enron@ENRON, Mona L Petrochko/NA/Enron@Enron, Jennifer Rudolph/HOU/EES@EES, Eric Letke/DUB/EES@EES cc: Joseph Alamo/NA/Enron@Enron, Lysa Akin/PDX/ECT@ECT Subject: Call to Discuss California PUC Action We will set up a call-in number to relay to folks any actions the Commission takes tomorrow. The Commission meeting starts at 10 AM PST, but it's unclear when they will take up our issue. Since the press will likely have the place surrounded, they may decide to do that item first. We'll send out a notice with a call-in # and time as soon as we have the information. Best, Jeff", 0),
            12345: SaraDoc('daily-hip-bass', "David: Enclosed is a new draft of the Kennecott amendment letter. Mark and I have discussed the counterparty termination language and have come up with the language that I have high;lighted. Upon your approval, I plan to use the same language for the Duke and Cinnergy letters. Carol St. Clair EB 3892 713-853-3989 (Phone) 713-646-3393 (Fax)", 0),
            5432: SaraDoc('badly-sound-marmot', "Dear UT Team: As you know, we conducted our intern interviews at UT last week. We interviewed almost 50 candidates during round 1 (thanks to Chris Sherman, Jim Cole, Hunter Shively, Dwight Fruge', Stan Dowell, Rick Carson, and Kim Chick), and our round 2 interviewers (Rick Causey, Brent Price, Mark Lindsey, and Mike Deville) have selected the following 11 candidates for a summer internship: Cathy Wang 512-479-7264 Ameet Rane 512-505-2045 Michelle Yee 512-495-3264 Jessica Payne 512-499-8729 Wesley Thoman 512-343-8895 Pranav Gandhi 512-294-4311 (active in student government - elections to be held next Wednesday and Thursday) Daniel Payne 512-472-6739 Kruti Patel 512-356-2321 Rachel Ravanzo 512-689-3814 (also interested in the tax group - will decide between the 2 options, but was extended 1 offer) Vivek Shah 512-495-4066 Vini Adenwala 512-457-8744 I will send cultivation assignments next week, but in the mean time please feel free to call and congratulate these candidates. I have extended verbal offers to each of them and they should receive their offer letters by the end of next week. Thanks to each of our interviewers for their help and long hours! lexi 3-4585", 0),
        })

    def test_queries(self):
        # Test that the dataset 'dummy' has 4 queries, and test the specific queries at indices 0 and 3
        self._test_queries('sara', count=150, items={
            0: GenericQuery('1', 'Politicians that decide the plans from state to state'),
            87: GenericQuery('88', 'Enron procurement'),
            122: GenericQuery('123', 'Senators linked to enron')
        })

    def test_qrels(self):
        # Test that the dataset 'dummy' has 60 qrels, and test the specific qrels at indices 0, 9, and 59
        self._test_qrels('sara', count=800157, items={
            0: TrecQrel(
                query_id='1',
                doc_id= '201645',
                iteration= "0", 
                relevance=0),
            5: TrecQrel(
                query_id='3',
                doc_id= '175389',
                iteration= "0", 
                relevance=0),
            0: TrecQrel(
                query_id='1',
                doc_id= '201645',
                iteration= "0", 
                relevance=0)
        })

if __name__ == '__main__':
    unittest.main()