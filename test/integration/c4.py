import re
import unittest
from ir_datasets.datasets.c4 import C4Doc, MisinfoQuery
from .base import DatasetIntegrationTest


class TestCar(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('c4/en-noclean-tr/trec-misinfo-2021', items={
            0: C4Doc('en.noclean.c4-train.00000-of-07168.0', re.compile('^November 24, 2016 – World News, Breaking News\nWednesday, April 24, 2019\nLatest:\nFitbit introduced “s.{3832}World News, Breaking News\\. All rights reserved\\.\nTheme: ColorMag by ThemeGrill\\. Powered by WordPress\\.$', flags=48), 'http://sevendaynews.com/2016/11/24/', '2019-04-24T16:35:11Z'),
            9: C4Doc('en.noclean.c4-train.00000-of-07168.9', re.compile('^Best Books Market\nBest Books Market\nCategories\nBook\nToy\nfree ftp mac client :: :: эффективные средст.{735}e Eleven Rival Regional Cultures of North America\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\nNext\n© 2019 Best Books Market$', flags=48), 'http://books.amzgr.com/', '2019-04-26T05:44:42Z'),
            99: C4Doc('en.noclean.c4-train.00000-of-07168.99', re.compile('^Volunteers Needed for Assembly for Children \\| Church of God of Prophecy\n\\+1\\.423\\.559\\.5100 info@cogop\\.o.{2453}ture\nSocial Media\nResources\nAssembly\nTreasurer’s Report\nFacebook\nInstagram\nVimeo\nYoutube\nTwitter\nRSS$', flags=48), 'https://cogop.org/blog/volunteers-needed-for-assembly-for-children/', '2019-04-26T08:07:35Z'),
            999: C4Doc('en.noclean.c4-train.00000-of-07168.999', re.compile('^타임폴리오자산운용\nINVESTMENT HIGHLIGHTS\nMulti Manager\nMulti Asset\nMulti Strategy\nTMS\nQAS\nMMS\nCOMPANY\nIntrodu.{300}x\\] 해상도에 최적화 되어 있습니다\\.\nTel\\. \\(02\\) 533\\-8940\nFax\\. \\(02\\) 534\\-3305\nE\\-mail\\. tf@timefolio\\.co\\.kr\n개인정보처리방침 > TOP$', flags=48), 'http://timefolio.co.kr/gallery/gallery_view.php?num=39&page=1&search_keyword=&search_field=', '2019-04-25T06:54:54Z'),
            9999: C4Doc('en.noclean.c4-train.00000-of-07168.9999', re.compile('^Unex Avid Juicy 3/5/7/Ca Metal Ceramic Disc Brake Pad\nJavaScript seems to be disabled in your browse.{10024}rice\\}\\}\nApply\nCancel\n\\{\\{carrier\\.method_title\\}\\}\n\\+ \\{\\{\\$parent\\.currency\\}\\}\\{\\{carrier\\.price\\}\\}\nApply\nCancel\n\\-\\-$', flags=48), 'https://www.bicyclehero.com/us/unex-avid-juicy-3-5-7-ca-metal-ceramic-disc-brake-pad.html', '2019-04-23T16:41:09Z'),
            99999: C4Doc('en.noclean.c4-train.00000-of-07168.99999', re.compile("^The truth about SHA1, SHA\\-256, dual\\-signing and Code Signing Certificates : K Software\nWelcome to th.{5597}ouldn't be helpful\\. Help us improve this article with your feedback\\.\nRelated Articles\nHome Solutions$", flags=48), 'https://support.ksoftware.net/support/solutions/articles/215805-the-truth-about-sha1-sha-256-dual-signing-and-code-signing-certificates-', '2019-04-20T09:00:23Z'),
            999999: C4Doc('en.noclean.c4-train.00006-of-07168.109537', re.compile('^Results \\- Race Walking Association\nHome \\| Fixtures \\| Results \\| Rankings \\| Athletes \\| Clubs \\| Newslet.{400}und points: 0\n2012: 2 races 2,000 metres completed\\.\n\\(c\\) RACE WALKING ASSOCIATION 1907 \\- 2019 Sitemap$', flags=48), 'http://racewalkingassociation.com/AthleteDetails.asp?mode=edit&id=11300&athlete=Emily_Wyman', '2019-04-25T20:24:19Z'),
        })

    def test_queries(self):
        self._test_queries('c4/en-noclean-tr/trec-misinfo-2021', count=50, items={
            0: MisinfoQuery('101', re.compile('ankl.*nitis', flags=48), re.compile('Will.*kle bra.*heal ac.*', flags=48), re.compile('Achil.*kle braces, or both.', flags=48), re.compile('We do no.*professional advice.', flags=48), 'unhelpful', 'https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3134723/'),
            9: MisinfoQuery('110', re.compile('birt.*tment', flags=48), re.compile('Wil.*trol pil.*arian c.*', flags=48), re.compile('Functi.*lth issues, or both.', flags=48), re.compile('We do no.*professional advice.', flags=48), 'unhelpful', 'https://pubmed.ncbi.nlm.nih.gov/24782304/'),
            49: MisinfoQuery('150', re.compile('antiox.*ity', flags=48), re.compile('Wil.*oxida.*ments.*blems.* ', flags=48), re.compile("Coupl.*ether. ", flags=48), re.compile('We do no.*professional advice.', flags=48), 'unhelpful', 'https://pubmed.ncbi.nlm.nih.gov/32851663/'),
        })


if __name__ == '__main__':
    unittest.main()
