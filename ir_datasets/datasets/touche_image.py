from ir_datasets import registry
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import ToucheImageDocs
from ir_datasets.util import DownloadConfig, home_path, Cache

NAME = "touche-image"


def _init():
    base_path = home_path() / NAME

    documentation = YamlDocumentation(f"docs/{NAME}.yaml")
    download_config = DownloadConfig.context(NAME, base_path)

    base = Dataset(documentation('_'))

    def cached_download(name: str, extension: str) -> Cache:
        return Cache(
            download_config[name],
            base_path / f"{name}.{extension}"
        )

    datasets = {
        f"2022-06-13": Dataset(
            ToucheImageDocs(
                cached_download("2022-06-13/images-main", "zip"),
                cached_download("2022-06-13/images-nodes", "zip"),
                cached_download("2022-06-13/images-png", "zip"),
                namespace=f"{NAME}/2022-06-13",
                language="en",
                count_hint=23841,
            ),
            documentation("2022-06-13"),
        )
    }

    # NOTE: the following datasets are defined in touche.py:
    #  - touche-image/2022-06-13/touche-2022-task-3

    # Register datasets.
    registry.register(NAME, base)
    for name, images in datasets.items():
        registry.register(f'{NAME}/{name}', images)

    return base, datasets


dataset = _init()
