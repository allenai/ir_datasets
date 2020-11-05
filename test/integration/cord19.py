import re
import unittest
from ir_datasets.datasets.cord19 import Cord19Doc
from ir_datasets.formats import TrecQrel, TrecQuery, GenericDoc
from .base import DatasetIntegrationTest


class TestCord19(DatasetIntegrationTest):
    def test_cord19_docs(self):
        self._test_docs('cord19', count=192509, items={
            0: Cord19Doc('ug7v899j', 'Clinical features of culture-proven Mycoplasma pneumoniae infections at King Abdulaziz University Hospital, Jeddah, Saudi Arabia', '10.1186/1471-2334-1-6', '2001-07-04', re.compile('^OBJECTIVE: This retrospective chart review describes the epidemiology and clinical features of 40 pa.{1647}preschool children and that the mortality rate of pneumonia in patients with comorbidities was high\\.$', flags=48), re.compile('^Clinical features of culture\\-proven Mycoplasma pneumoniae infections at King Abdulaziz University Ho.{14974}n history\nThe pre\\-publication history for this paper can be accessed here:\n\nPre\\-publication history\n$', flags=48)),
            9: Cord19Doc('jg13scgo', 'Technical Description of RODS: A Real-time Public Health Surveillance System', '10.1197/jamia.m1345', '2003-09-01', re.compile('^This report describes the design and implementation of the Real\\-time Outbreak and Disease Surveillan.{1077} be a resource for implementing, evaluating, and applying new methods of public health surveillance\\.$', flags=48), re.compile('^Technical Description of RODS: A Real\\-time Public Health Surveillance System\n\nThis report describes .{33955}ion is as unwise as having commercial airline pilots taking off without weather forecasts and radar\\.$', flags=48)),
            192508: Cord19Doc('pnl9th2c', 'Vascular Life during the COVID-19 Pandemic Reminds Us to Prepare for the Unexpected', '10.1016/j.ejvs.2020.04.040', '2020-05-12', '', re.compile('^Vascular Life during the COVID\\-19 Pandemic Reminds Us to Prepare for the Unexpected\n\n\n\n\nThe first re.{6007} and waiting for explantation and new revascularisation\\.\n\nFigure 1\nA, anterior; and B, lateral view\\.$', flags=48)),
        })

    def test_cord19_queries(self):
        self._test_queries('cord19/trec-covid', count=50, items={
            0: TrecQuery('1', 'coronavirus origin', 'what is the origin of COVID-19', "seeking range of information about the SARS-CoV-2 virus's origin, including its evolution, animal source, and first transmission into humans"),
            9: TrecQuery('10', 'coronavirus social distancing impact', 'has social distancing had an impact on slowing the spread of COVID-19?', "seeking specific information on studies that have measured COVID-19's transmission in one or more social distancing (or non-social distancing) approaches"),
            49: TrecQuery('50', 'mRNA vaccine coronavirus', 'what is known about an mRNA vaccine for the SARS-CoV-2 virus?', 'Looking for studies specifically focusing on mRNA vaccines for COVID-19, including how mRNA vaccines work, why they are promising, and any results from actual clinical studies.'),
        })

    def test_cord19_qrels(self):
        self._test_qrels('cord19/trec-covid', count=69318, items={
            0: TrecQrel('1', '005b2j4b', 2, '4.5'),
            9: TrecQrel('1', '05vx82oo', 0, '3'),
            69317: TrecQrel('50', 'zz8wvos9', 1, '5'),
        })


if __name__ == '__main__':
    unittest.main()
