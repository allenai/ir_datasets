from typing import Dict, Tuple

from ir_datasets import registry
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import ToucheQueries, TrecQrels, ToucheTrecQrels
from ir_datasets.util import DownloadConfig, home_path, Cache, ZipExtract

NAME = "touche"

CACHE: Dict[str, Tuple[str]] = {
    "2020/task-1/qrels": ("qrels",),
    "2020/task-2/qrels": ("qrels",),
    "2020/task-1/qrels-argsme-1.0-uncorrected": ("qrels",),
    "2020/task-1/qrels-argsme-2020-04-01-uncorrected": ("qrels",),
    "2021/task-1/qrels-relevance": ("qrels",),
    "2021/task-1/qrels-quality": ("qrels",),
    "2021/task-2/qrels-relevance": ("qrels",),
    "2021/task-2/qrels-quality": ("qrels",),
}
CACHE_ZIP: Dict[str, Tuple[str, str]] = {
    "2020/task-1/queries": ("topics-task-1.xml", "xml"),
    "2020/task-2/queries": ("topics-task-2.xml", "xml"),
    "2021/task-1/queries": ("topics-task-1-only-titles.xml", "xml"),
    "2021/task-2/queries": ("topics-task2-51-100.xml", "xml"),
}

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

    # Load cached download configurations.
    cache_files: Dict[str, Cache] = {
        name: Cache(
            download_config[name],
            base_path / f"{name}.{extension}"
        )
        for name, (extension,) in CACHE.items()
    }
    cache_zip_files: Dict[str, Cache] = {
        name: Cache(
            ZipExtract(
                download_config[name],
                zip_path
            ),
            base_path / f"{name}.{extension}"
        )
        for name, (zip_path, extension) in CACHE_ZIP.items()
    }
    cache_files.update(cache_zip_files)

    # Define and create base dataset.
    base_datasets = {
        NAME: Dataset(documentation("_")),
        f"{NAME}/2020": Dataset(documentation("2020")),
        f"{NAME}/2021": Dataset(documentation("2021")),
    }
    for name, dataset in base_datasets.items():
        registry.register(name, dataset)

    # Define and create task base datasets.
    task_base_datasets = {
        f"{NAME}/2020/task-1": Dataset(
            registry["argsme/1.0"].docs_handler(),
            ToucheQueries(
                cache_files["2020/task-1/queries"],
                namespace=f"{NAME}/2020/task-1/queries",
                language="en",
            ),
            TrecQrels(
                cache_files["2020/task-1/qrels"],
                QRELS_DEFS_2021_TASK_1_RELEVANCE,
            ),
            documentation("2020/task-1"),
        ),
        f"{NAME}/2020/task-2": Dataset(
            registry["clueweb12"].docs_handler(),
            ToucheQueries(
                cache_files["2020/task-2/queries"],
                namespace=f"{NAME}/2020/task-2/queries",
                language="en",
            ),
            TrecQrels(
                cache_files["2020/task-2/qrels"],
                QRELS_DEFS_2020_TASK_2,
            ),
            documentation("2020/task-2"),
        ),
        f"{NAME}/2021/task-1": Dataset(
            registry["argsme/2020-04-01"].docs_handler(),
            ToucheQueries(
                cache_files["2021/task-1/queries"],
                namespace=f"{NAME}/2021/task-1/queries",
                language="en",
                has_description=False,
            ),
            documentation("2021/task-1"),
        ),
        f"{NAME}/2021/task-2": Dataset(
            registry["clueweb12"].docs_handler(),
            ToucheQueries(
                cache_files["2021/task-2/queries"],
                namespace=f"{NAME}/2021/task-2/queries",
                language="en",
            ),
            documentation("2021/task-2"),
        ),
    }
    for name, dataset in task_base_datasets.items():
        registry.register(name, dataset)

    # Define and create task sub-datasets.
    task_sub_datasets = {
        f"{NAME}/2020/task-1/argsme-1.0-uncorrected": Dataset(
            registry["argsme/1.0"].docs_handler(),
            registry[f"{NAME}/2020/task-1"].queries_handler(),
            ToucheTrecQrels(
                cache_files["2020/task-1/qrels-argsme-1.0-uncorrected"],
                QRELS_DEFS_2020_TASK_1,
            ),
            documentation("2020/task-1/argsme-1.0-uncorrected"),
        ),
        f"{NAME}/2020/task-1/argsme-2020-04-01-uncorrected": Dataset(
            registry["argsme/2020-04-01"].docs_handler(),
            registry[f"{NAME}/2020/task-1"].queries_handler(),
            ToucheTrecQrels(
                cache_files["2020/task-1/qrels-argsme-2020-04-01-uncorrected"],
                QRELS_DEFS_2020_TASK_1,
            ),
            documentation("2020/task-1/argsme-2020-04-01-uncorrected"),
        ),
        f"{NAME}/2021/task-1/relevance": Dataset(
            registry["argsme/2020-04-01"].docs_handler(),
            registry[f"{NAME}/2021/task-1"].queries_handler(),
            TrecQrels(
                cache_files["2021/task-1/qrels-relevance"],
                QRELS_DEFS_2021_TASK_1_RELEVANCE,
            ),
            documentation("2021/task-1/relevance"),
        ),
        f"{NAME}/2021/task-1/quality": Dataset(
            registry["argsme/2020-04-01"].docs_handler(),
            registry[f"{NAME}/2021/task-1"].queries_handler(),
            TrecQrels(
                cache_files["2021/task-1/qrels-quality"],
                QRELS_DEFS_2021_TASK_1_QUALITY,
            ),
            documentation("2021/task-1/quality"),
        ),
        f"{NAME}/2021/task-2/relevance": Dataset(
            registry["clueweb12"].docs_handler(),
            registry[f"{NAME}/2021/task-2"].queries_handler(),
            TrecQrels(
                cache_files["2021/task-2/qrels-relevance"],
                QRELS_DEFS_2021_TASK_2_RELEVANCE,
            ),
            documentation("2021/task-2/relevance"),
        ),
        f"{NAME}/2021/task-2/quality": Dataset(
            registry["clueweb12"].docs_handler(),
            registry[f"{NAME}/2021/task-2"].queries_handler(),
            TrecQrels(
                cache_files["2021/task-2/qrels-quality"],
                QRELS_DEFS_2021_TASK_2_QUALITY,
            ),
            documentation("2021/task-2/quality"),
        ),
    }
    for name, dataset in task_sub_datasets.items():
        registry.register(name, dataset)

    return base_datasets, task_base_datasets, task_sub_datasets


_init()
