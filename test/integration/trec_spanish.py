import re
import unittest
from ir_datasets.datasets.trec_spanish import TrecSpanish3Query, TrecSpanish4Query
from ir_datasets.formats import TrecQrel, TrecDoc
from .base import DatasetIntegrationTest


class TestTrecSpanish(DatasetIntegrationTest):
    def test_trec_spanish_docs(self):
        self._test_docs('trec-spanish', count=120605, items={
            0: TrecDoc('AF940512-0001', '\n\n1948 GMT 94/05/12\n', '<TRAILER lang=sp>\n<DATE>1948 GMT 94/05/12</DATE></TRAILER>\n'),
            9: TrecDoc('AF940512-0010', '\n\n1956 GMT 94/05/12\n', '<TRAILER lang=sp>\n<DATE>1956 GMT 94/05/12</DATE></TRAILER>\n'),
            120604: TrecDoc('SP94-0202972', re.compile('^\n\n\nFirmarían Rusia y EU\n\n\nel tratado START II\n\nFuncionario de EU dice desconocer acuerdo final sobre.{4662}SARME\n  REUNION\n  SECCION INTERNACIONALES\n  FIRMA\n  PERSPECTIVA\n  POLITICA\n  CHINA\n  ARMA ATOMICA\n\n\n$', flags=48), re.compile('^<HEADLINE>\n\nFirmarían Rusia y EU\n</HEADLINE>\n<TEXT>\nel tratado START II\n\nFuncionario de EU dice desc.{4695} REUNION\n  SECCION INTERNACIONALES\n  FIRMA\n  PERSPECTIVA\n  POLITICA\n  CHINA\n  ARMA ATOMICA\n\n</TEXT>\n$', flags=48)),
        })

    def test_trec_spanish_queries(self):
        self._test_queries('trec-spanish/trec3', count=25, items={
            0: TrecSpanish3Query(query_id='1', title_es='Oposición Mexicana al TLC Mexican Opposition to NAFTA ', title_en='Documento contendrá información sobre oposición en México al tratado de libre comercio norteamericano.', description_es=' Document will contain information on opposition in Mexico to the North American Free Trade Agreement. ', description_en='Oposición al TLC en el gobierno mexicano, en medios comerciales, y en el sector privado.  Dificultará esta oposición la implementación plena del TLC?  Documento debe incluir nombres de compañías opuestas al TLC con razones y sectores específicos económicos y empresariales de oposición.', narrative_es=' Opposition to NAFTA in the Mexican Government, in commercial entities, and in the private sector.  Will this opposition make full implementation of NAFTA impossible?  The document should include names of companies, and specific economic and business sectors of opposition, including reasons for their opposition. ', narrative_en=''),
            9: TrecSpanish3Query(query_id='10', title_es='México es importante país de tránsito en la guerra antinarcótica Mexico is an important transit country in the war against narcotics ', title_en='México es importante para los narcotraficantes de Colombia y Perú como punto de entrada para los Estados Unidos.  Como se usan México como país de tránsito?', description_es=' Mexico is important to the narcotraffickers of Colombia and Peru as an entry point for the U.S.  How is Mexico used as a transit country? ', description_en='Documento debe indicar métodos empleados por narcotraficantes para utilizar México como país de tránsito para hacer llegar las drogas ilegales a los Estados Unidos.  Debe incluir ejemplos y locales específicos y medidas para impedir esta actividad.', narrative_es=' The document should indicate methods used by narcotraffickers to utilize Mexico as a transit country for getting drugs into the U.S.  It should include specific examples and locations and measures for stopping this activity. ', narrative_en=''),
            24: TrecSpanish3Query(query_id='25', title_es="Programa de Privatización de Empresas Públicas Mexicanas Program for Privatization of Mexico's Public Enterprises ", title_en='El programa mexicano de privatización es considerado uno de los más exitosos en América Latina.  El documento debe describir el proceso de privatización de empresas públicas en México.', description_es=" Mexico's privatization program is considered one of the most successful in Latin America.  The document should describe the process of privatization of public companies in Mexico. ", description_en='Para ser relevante el documento debe mencionar la empresa pública mexicana privatizada, incluyendo resultados y pronósticos de otros sectores que pueden ser privatizados.', narrative_es=' To be relevant, the document should mention the Mexican public enterprise that has been privatized, including results and predictions of other sectors that might be privatized. ', narrative_en='')
        })
        self._test_queries('trec-spanish/trec4', count=25, items={
            0: TrecSpanish4Query(query_id='26', description_es1='Indicaciones de las relaciones económicas y comerciales de México con los paises europeos.', description_en1='Indications of Mexican economic and trade relations with European countries.', description_es2='Indicaciones de las relaciones económicas y comerciales de México con europa.', description_en2=' Indications of Mexican economic and trade relations with Europe. '),
            9: TrecSpanish4Query(query_id='35', description_es1='Indicaciones de los potenciales y debilidades de las fuerzas aéreas militares de México.', description_en1='Indications of potentials and weaknesses of the Mexican Air Force.', description_es2='Indicaciones de fortalezas y debilidades de las fuerzas aéreas militares de México.', description_en2=' Indications of strengths and weaknesses of the Mexican Air Force. '),
            24: TrecSpanish4Query(query_id='50', description_es1='La fabricación en México de joyas de plata y oro.', description_en1=' Manufacture of gold and silver jewelry in Mexico. ', description_es2='', description_en2='')
        })

    def test_trec_spanish_qrels(self):
        self._test_qrels('trec-spanish/trec3', count=19005, items={
            0: TrecQrel(query_id='1', doc_id='SP94-0000082', relevance=1, iteration='0'),
            9: TrecQrel(query_id='1', doc_id='SP94-0001385', relevance=0, iteration='0'),
            19004: TrecQrel(query_id='25', doc_id='SP94-0202950', relevance=1, iteration='0')
        })
        self._test_qrels('trec-spanish/trec4', count=13109, items={
            0: TrecQrel(query_id='26', doc_id='SP94-0000054', relevance=1, iteration='0'),
            9: TrecQrel(query_id='26', doc_id='SP94-0000700', relevance=0, iteration='0'),
            13108: TrecQrel(query_id='50', doc_id='SP94-0202879', relevance=0, iteration='0')
        })


if __name__ == '__main__':
    unittest.main()
