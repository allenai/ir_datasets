from pathlib import Path
from typing import Iterator
from datamaestro_text.interfaces.trec import parse_qrels
from experimaestro import Config, Option
import datamaestro_text.data.ir as ir
import ir_datasets

# Interface between ir_datasets and datamaestro:
# provides adapted data types


class IRDSId(Config):
    @classmethod
    def __xpmid__(cls):
        return f"ir_datasets.{cls.__qualname__}"

    irds: Option[str]


class AdhocTopics(ir.AdhocTopics, IRDSId):
    def iter(self) -> Iterator[ir.AdhocTopic]:
        """Returns an iterator over topics"""
        ds = ir_datasets.load(self.irds)
        for query in ds.queries_iter():
            yield ir.AdhocTopic(query.query_id, query.text)


class AdhocAssessments(ir.AdhocAssessments, IRDSId):
    def iter(self):
        """Returns an iterator over assessments"""
        ds = ir_datasets.load(self.irds)
        yield from parse_qrels(Path(ds.qrels_path()))


class AdhocDocuments(ir.AdhocDocuments, IRDSId):
    def iter(self) -> Iterator[ir.AdhocDocument]:
        """Returns an iterator over adhoc documents"""
        ds = ir_datasets.load(self.irds)
        for doc in ds.docs_iter():
            yield ir.AdhocDocument(doc.doc_id, doc.text)
            

class Adhoc(ir.Adhoc, IRDSId):
    pass


class AdhocRun(ir.AdhocRun, IRDSId):
    pass


class TrainingTripletsLines(ir.TrainingTripletsLines, IRDSId):
    pass
