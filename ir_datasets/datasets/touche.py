from typing import Dict

from ir_datasets import registry
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import ToucheQueries, ToucheTitleQueries, \
    ToucheComparativeQueries, ToucheQrels, ToucheQualityQrels, \
    ToucheQualityComparativeStanceQrels, ToucheControversialStanceQrels, \
    ToucheQualityCoherenceQrels, TouchePassageDocs
from ir_datasets.util import DownloadConfig, home_path, Cache, ZipExtract, GzipExtract

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

    # Define and create task datasets.
    task_base_datasets = {
        f"argsme/2020-04-01/{NAME}-2020-task-1": Dataset(
            registry["argsme/2020-04-01"].docs_handler(),
            ToucheQueries(
                cached_zip_download("2020/task-1/queries", "topics-task-1.xml", "xml"),
                namespace=f"argsme/2020-04-01/{NAME}-2020-task-1",
                language="en",
            ),
            ToucheQrels(
                cached_download("2020/task-1/qrels", "qrels"),
                QRELS_DEFS_2020_TASK_1,
            ),
            documentation("2020/task-1"),
        ),
        f"clueweb12/{NAME}-2020-task-2": Dataset(
            registry["clueweb12"].docs_handler(),
            ToucheQueries(
                cached_zip_download("2020/task-2/queries", "topics-task-2.xml", "xml"),
                namespace=f"clueweb12/{NAME}-2020-task-2",
                language="en",
            ),
            ToucheQrels(
                cached_download("2020/task-2/qrels", "qrels"),
                QRELS_DEFS_2020_TASK_2,
            ),
            documentation("2020/task-2"),
        ),
        f"argsme/2020-04-01/{NAME}-2021-task-1": Dataset(
            registry["argsme/2020-04-01"].docs_handler(),
            ToucheTitleQueries(
                cached_zip_download("2021/task-1/queries", "topics-task-1-only-titles.xml", "xml"),
                namespace=f"argsme/2020-04-01/{NAME}-2021-task-1",
                language="en",
            ),
            ToucheQualityQrels(
                cached_download("2021/task-1/qrels-relevance", "qrels"),
                cached_download("2021/task-1/qrels-quality", "qrels"),
                QRELS_DEFS_2021_TASK_1,
            ),
            documentation("2021/task-1"),
        ),
        f"clueweb12/{NAME}-2021-task-2": Dataset(
            registry["clueweb12"].docs_handler(),
            ToucheQueries(
                cached_zip_download("2021/task-2/queries", "topics-task2-51-100.xml", "xml"),
                namespace=f"clueweb12/{NAME}-2021-task-2",
                language="en",
            ),
            ToucheQualityQrels(
                cached_download("2021/task-2/qrels-relevance", "qrels"),
                cached_download("2021/task-2/qrels-quality", "qrels"),
                QRELS_DEFS_2021_TASK_2,
            ),
            documentation("2021/task-2"),
        ),
        f"argsme/2020-04-01/processed/{NAME}-2022-task-1": Dataset(
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
        ),
        f"clueweb12/{NAME}-2022-task-2": Dataset(
            TouchePassageDocs(
                cached_gzip_download("2022/task-2/passages", "jsonl"),
                namespace=f"clueweb12/{NAME}-2022-task-2",
                language="en",
                count_hint=868655,
            ),
            ToucheComparativeQueries(
                cached_zip_download("2022/task-2/queries", "topics-task2.xml", "xml"),
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
        ),
        f"touche-image/2022-06-13/{NAME}-2022-task-3": Dataset(
            registry["touche-image/2022-06-13"].docs_handler(),
            ToucheQueries(
                cached_download("2022/task-3/queries", "xml"),
                namespace=f"{NAME}/{NAME}-2022-task-3",
                language="en",
            ),
            ToucheControversialStanceQrels(
                cached_download("2022/task-3/qrels", "qrels"),
                QRELS_DEFS_2022_TASK_3,
            ),
            documentation("2022/task-3"),
        ),
    }
    for name, dataset in task_base_datasets.items():
        registry.register(name, dataset)

    # Define and create task sub-datasets.
    task_sub_datasets = {
        f"argsme/1.0/{NAME}-2020-task-1/uncorrected": Dataset(
            registry["argsme/1.0"].docs_handler(),
            registry[f"argsme/2020-04-01/{NAME}-2020-task-1"].queries_handler(),
            ToucheQrels(
                cached_download("2020/task-1/qrels-argsme-1.0-uncorrected", "qrels"),
                QRELS_DEFS_2020_TASK_1,
                allow_float_score=True,
            ),
            documentation("2020/task-1/argsme-1.0/uncorrected"),
        ),
        f"argsme/2020-04-01/{NAME}-2020-task-1/uncorrected": Dataset(
            registry["argsme/2020-04-01"].docs_handler(),
            registry[f"argsme/2020-04-01/{NAME}-2020-task-1"].queries_handler(),
            ToucheQrels(
                cached_download("2020/task-1/qrels-argsme-2020-04-01-uncorrected", "qrels"),
                QRELS_DEFS_2020_TASK_1,
                allow_float_score=True,
            ),
            documentation("2020/task-1/argsme-2020-04-01/uncorrected"),
        ),
        f"clueweb12/{NAME}-2022-task-2/expanded-doc-t5-query": Dataset(
            TouchePassageDocs(
                cached_gzip_download("2022/task-2/passages-expanded-doc-t5-query", "jsonl"),
                namespace=f"clueweb12/{NAME}-2022-task-2",
                language="en",
                count_hint=868655
            ),
            registry[f"clueweb12/{NAME}-2022-task-2"].queries_handler(),
            registry[f"clueweb12/{NAME}-2022-task-2"].qrels_handler(),
            documentation("2022/task-2/expanded-doc-t5-query"),
        ),
    }
    for name, dataset in task_sub_datasets.items():
        registry.register(name, dataset)

    return task_base_datasets, task_sub_datasets


_init()
