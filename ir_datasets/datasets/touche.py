from typing import Dict

from ir_datasets import registry
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import ToucheQueries, ToucheTitleQueries, \
    ToucheComparativeQueries, ToucheCausalQueries, ToucheQrels, \
    ToucheQualityQrels, ToucheQualityComparativeStanceQrels, \
    ToucheControversialStanceQrels, ToucheQualityCoherenceQrels, \
    TouchePassageDocs, BaseQueries, BaseQrels, BaseDocs
from ir_datasets.util import DownloadConfig, home_path, Cache, ZipExtract, \
    GzipExtract

NAME = "touche"

QRELS_DEFS_2020_TASK_1: Dict[int, str] = {
    -2: "spam, non-argument",
    1: "very low relevance",
    2: "low relevance",
    3: "moderate relevance",
    4: "high relevance",
    5: "very high relevance",
}
QRELS_DEFS_2020_TASK_2: Dict[int, str] = {
    0: "not relevant",
    1: "relevant",
    2: "highly relevant",
}
QRELS_DEFS_2021_TASK_1: Dict[int, str] = {
    -2: "spam",
    0: "not relevant",
    1: "relevant",
    2: "highly relevant",
}
QRELS_DEFS_2021_TASK_2: Dict[int, str] = {
    0: "not relevant",
    1: "relevant",
    2: "highly relevant",
}
QRELS_DEFS_2022_TASK_1: Dict[int, str] = {
    0: "not relevant",
    1: "relevant",
    2: "highly relevant",
}
QRELS_DEFS_2022_TASK_2: Dict[int, str] = {
    0: "not relevant",
    1: "relevant",
    2: "highly relevant",
}
QRELS_DEFS_2022_TASK_3: Dict[int, str] = {
    0: "not relevant",
    1: "relevant",
}


def _init():
    base_path = home_path() / NAME

    documentation = YamlDocumentation(f"docs/{NAME}.yaml")
    download_config = DownloadConfig.context(NAME, base_path)

    def cached_download(name: str, extension: str) -> Cache:
        return Cache(
            download_config[name],
            base_path / f"{name}.{extension}"
        )

    def cached_zip_download(name: str, zip_path: str, extension: str) -> Cache:
        return Cache(
            ZipExtract(
                download_config[name],
                zip_path
            ),
            base_path / f"{name}.{extension}"
        )

    def cached_gzip_download(name: str, extension: str) -> Cache:
        return Cache(
            GzipExtract(download_config[name]),
            base_path / f"{name}.{extension}"
        )

    datasets = []

    def register_dataset(
            name: str,
            docs: BaseDocs,
            queries: BaseQueries,
            qrels: BaseQrels,
            doc_key: str,
    ):
        dataset = Dataset(docs, queries, qrels, documentation(doc_key))
        registry.register(name, dataset)
        datasets.append(dataset)

    def register_ongoing_dataset(
            name: str,
            docs: BaseDocs,
            queries: BaseQueries,
            doc_key: str,
    ):
        dataset = Dataset(docs, queries, documentation(doc_key))
        registry.register(name, dataset)
        datasets.append(dataset)

    # Touché 2020
    register_dataset(
        f"argsme/2020-04-01/{NAME}-2020-task-1",
        registry["argsme/2020-04-01"].docs_handler(),
        ToucheQueries(
            cached_zip_download(
                "2020/task-1/queries", "topics-task-1.xml", "xml"
            ),
            namespace=f"argsme/2020-04-01/{NAME}-2020-task-1",
            language="en",
        ),
        ToucheQrels(
            cached_download("2020/task-1/qrels", "qrels"),
            QRELS_DEFS_2020_TASK_1,
        ),
        documentation("2020/task-1"),
    )
    register_dataset(
        f"argsme/2020-04-01/{NAME}-2020-task-1/uncorrected",
        registry["argsme/2020-04-01"].docs_handler(),
        registry[f"argsme/2020-04-01/{NAME}-2020-task-1"].queries_handler(),
        ToucheQrels(
            cached_download(
                "2020/task-1/qrels-argsme-2020-04-01-uncorrected", "qrels"
            ),
            QRELS_DEFS_2020_TASK_1,
            allow_float_score=True,
        ),
        documentation("2020/task-1/argsme-2020-04-01/uncorrected"),
    )
    register_dataset(
        f"argsme/1.0/{NAME}-2020-task-1/uncorrected",
        registry["argsme/1.0"].docs_handler(),
        registry[f"argsme/2020-04-01/{NAME}-2020-task-1"].queries_handler(),
        ToucheQrels(
            cached_download(
                "2020/task-1/qrels-argsme-1.0-uncorrected", "qrels"
            ),
            QRELS_DEFS_2020_TASK_1,
            allow_float_score=True,
        ),
        documentation("2020/task-1/argsme-1.0/uncorrected"),
    )
    register_dataset(
        f"clueweb12/{NAME}-2020-task-2",
        registry["clueweb12"].docs_handler(),
        ToucheQueries(
            cached_zip_download(
                "2020/task-2/queries", "topics-task-2.xml", "xml"
            ),
            namespace=f"clueweb12/{NAME}-2020-task-2",
            language="en",
        ),
        ToucheQrels(
            cached_download("2020/task-2/qrels", "qrels"),
            QRELS_DEFS_2020_TASK_2,
        ),
        documentation("2020/task-2"),
    )

    # Touché 2021
    register_dataset(
        f"argsme/2020-04-01/{NAME}-2021-task-1",
        registry["argsme/2020-04-01"].docs_handler(),
        ToucheTitleQueries(
            cached_zip_download(
                "2021/task-1/queries", "topics-task-1-only-titles.xml", "xml"
            ),
            namespace=f"argsme/2020-04-01/{NAME}-2021-task-1",
            language="en",
        ),
        ToucheQualityQrels(
            cached_download("2021/task-1/qrels-relevance", "qrels"),
            cached_download("2021/task-1/qrels-quality", "qrels"),
            QRELS_DEFS_2021_TASK_1,
        ),
        documentation("2021/task-1"),
    )
    register_dataset(
        f"clueweb12/{NAME}-2021-task-2",
        registry["clueweb12"].docs_handler(),
        ToucheQueries(
            cached_zip_download(
                "2021/task-2/queries", "topics-task2-51-100.xml", "xml"
            ),
            namespace=f"clueweb12/{NAME}-2021-task-2",
            language="en",
        ),
        ToucheQualityQrels(
            cached_download("2021/task-2/qrels-relevance", "qrels"),
            cached_download("2021/task-2/qrels-quality", "qrels"),
            QRELS_DEFS_2021_TASK_2,
        ),
        documentation("2021/task-2"),
    )

    # Touché 2022
    register_dataset(
        f"argsme/2020-04-01/processed/{NAME}-2022-task-1",
        registry["argsme/2020-04-01/processed"].docs_handler(),
        ToucheQueries(
            cached_download("2022/task-1/queries", "xml"),
            namespace=f"argsme/2020-04-01-processed/{NAME}-2022-task-1",
            language="en",
        ),
        ToucheQualityCoherenceQrels(
            cached_download("2022/task-1/qrels-relevance", "qrels"),
            cached_download("2022/task-1/qrels-quality", "qrels"),
            cached_download("2022/task-1/qrels-coherence", "qrels"),
            QRELS_DEFS_2022_TASK_1,
        ),
        documentation("2022/task-1"),
    )
    register_dataset(
        f"clueweb12/{NAME}-2022-task-2",
        TouchePassageDocs(
            cached_gzip_download("2022/task-2/passages", "jsonl"),
            namespace=f"clueweb12/{NAME}-2022-task-2",
            language="en",
            count_hint=868655,
        ),
        ToucheComparativeQueries(
            cached_zip_download(
                "2022/task-2/queries", "topics-task2.xml", "xml"
            ),
            namespace=f"clueweb12/{NAME}-2022-task-2",
            language="en",
        ),
        ToucheQualityComparativeStanceQrels(
            cached_download("2022/task-2/qrels-relevance", "qrels"),
            cached_download("2022/task-2/qrels-quality", "qrels"),
            cached_download("2022/task-2/qrels-stance", "qrels"),
            QRELS_DEFS_2022_TASK_2,
        ),
        documentation("2022/task-2"),
    )
    register_dataset(
        f"clueweb12/{NAME}-2022-task-2/expanded-doc-t5-query",
        TouchePassageDocs(
            cached_gzip_download(
                "2022/task-2/passages-expanded-doc-t5-query", "jsonl"
            ),
            namespace=f"clueweb12/{NAME}-2022-task-2",
            language="en",
            count_hint=868655
        ),
        registry[f"clueweb12/{NAME}-2022-task-2"].queries_handler(),
        registry[f"clueweb12/{NAME}-2022-task-2"].qrels_handler(),
        documentation("2022/task-2/expanded-doc-t5-query"),
    )
    register_dataset(
        f"touche-image/2022-06-13/{NAME}-2022-task-3",
        registry["touche-image/2022-06-13"].docs_handler(),
        ToucheQueries(
            cached_download("2022/task-3/queries", "xml"),
            namespace=f"touche-image/2022-06-13/{NAME}-2022-task-3",
            language="en",
        ),
        ToucheControversialStanceQrels(
            cached_download("2022/task-3/qrels", "qrels"),
            QRELS_DEFS_2022_TASK_3,
        ),
        documentation("2022/task-3"),
    )

    # Touché 2023 (qrels to be released later)
    register_ongoing_dataset(
        f"clueweb22/b/{NAME}-2023-task-1",
        registry["clueweb22/b"].docs_handler(),
        registry[
            f"argsme/2020-04-01/processed/{NAME}-2022-task-1"
        ].queries_handler(),
        documentation("2023/task-1"),
    )
    register_ongoing_dataset(
        f"clueweb22/b/{NAME}-2023-task-2",
        registry["clueweb22/b"].docs_handler(),
        ToucheCausalQueries(
            cached_download("2023/task-2/queries", "xml"),
            namespace=f"clueweb22/b/{NAME}-2023-task-2",
            language="en",
        ),
        documentation("2023/task-2"),
    )
    register_ongoing_dataset(
        f"touche-image/2022-06-13/{NAME}-2023-task-3",
        registry[f"touche-image/2022-06-13/{NAME}-2022-task-3"].docs_handler(),
        ToucheQueries(
            cached_download("2023/task-3/queries", "xml"),
            namespace=f"touche-image/2022-06-13/{NAME}-2023-task-3",
            language="en",
        ),
        documentation("2023/task-3"),
    )

    return datasets


_init()
