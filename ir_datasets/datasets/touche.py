from typing import Dict

from ir_datasets import registry
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import ToucheQueries, ToucheQrels
from ir_datasets.formats.touche import ToucheQualityQrels
from ir_datasets.util import DownloadConfig, home_path, Cache, ZipExtract

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
QRELS_DEFS_2021_TASK_1_RELEVANCE: Dict[int, str] = {
    -2: "spam",
    0: "not relevant",
    1: "relevant",
    2: "highly relevant",
}
QRELS_DEFS_2021_TASK_1_QUALITY: Dict[int, str] = {
    0: "low quality",
    1: "sufficient quality",
    2: "high quality",
}
QRELS_DEFS_2021_TASK_2_RELEVANCE: Dict[int, str] = {
    0: "not relevant",
    1: "relevant",
    2: "highly relevant",
}
QRELS_DEFS_2021_TASK_2_QUALITY: Dict[int, str] = {
    0: "low quality or no arguments in the document",
    1: "sufficient quality",
    2: "high quality",
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

    # Define and create task datasets.
    task_base_datasets = {
        f"argsme/2020-04-01/{NAME}-2020-task-1": Dataset(
            registry["argsme/2020-04-01"].docs_handler(),
            ToucheQueries(
                cached_zip_download(
                    "2020/task-1/queries",
                    "topics-task-1.xml",
                    "xml"
                ),
                namespace=f"argsme/2020-04-01/{NAME}-2020-task-1/queries",
                language="en",
            ),
            ToucheQrels(
                cached_download("2020/task-1/qrels", "qrels"),
                QRELS_DEFS_2021_TASK_1_RELEVANCE,
            ),
            documentation("2020/task-1"),
        ),
        f"clueweb12/{NAME}-2020-task-2": Dataset(
            registry["clueweb12"].docs_handler(),
            ToucheQueries(
                cached_zip_download(
                    "2020/task-2/queries",
                    "topics-task-2.xml",
                    "xml"
                ),
                namespace=f"clueweb12/{NAME}-2020-task-2/queries",
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
            ToucheQueries(
                cached_zip_download(
                    "2021/task-1/queries",
                    "topics-task-1-only-titles.xml",
                    "xml"
                ),
                namespace=f"argsme/2020-04-01/{NAME}-2021-task-1/queries",
                language="en",
                has_description=False,
            ),
            ToucheQualityQrels(
                ToucheQrels(
                    cached_download("2021/task-1/qrels-relevance", "qrels"),
                    QRELS_DEFS_2021_TASK_1_RELEVANCE,
                ),
                ToucheQrels(
                    cached_download("2021/task-1/qrels-quality", "qrels"),
                    QRELS_DEFS_2021_TASK_1_QUALITY,
                ),
            ),
            documentation("2021/task-1"),
        ),
        f"clueweb12/{NAME}-2021-task-2": Dataset(
            registry["clueweb12"].docs_handler(),
            ToucheQueries(
                cached_zip_download(
                    "2021/task-2/queries",
                    "topics-task2-51-100.xml",
                    "xml"
                ),
                namespace=f"clueweb12/{NAME}-2021-task-2/queries",
                language="en",
            ),
            ToucheQualityQrels(
                ToucheQrels(
                    cached_download("2021/task-2/qrels-relevance", "qrels"),
                    QRELS_DEFS_2021_TASK_2_RELEVANCE,
                ),
                ToucheQrels(
                    cached_download("2021/task-2/qrels-quality", "qrels"),
                    QRELS_DEFS_2021_TASK_2_QUALITY,
                ),
            ),
            documentation("2021/task-2"),
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
                cached_download("2020/task-1/qrels-argsme-1.0-uncorrected",
                                "qrels"),
                QRELS_DEFS_2020_TASK_1,
            ),
            documentation("2020/task-1/argsme-1.0/uncorrected"),
        ),
        f"argsme/2020-04-01/{NAME}-2020-task-1/uncorrected": Dataset(
            registry["argsme/2020-04-01"].docs_handler(),
            registry[f"argsme/2020-04-01/{NAME}-2020-task-1"].queries_handler(),
            ToucheQrels(
                cached_download(
                    "2020/task-1/qrels-argsme-2020-04-01-uncorrected",
                    "qrels"),
                QRELS_DEFS_2020_TASK_1,
            ),
            documentation("2020/task-1/argsme-2020-04-01/uncorrected"),
        ),
    }
    for name, dataset in task_sub_datasets.items():
        registry.register(name, dataset)

    return task_base_datasets, task_sub_datasets


_init()
