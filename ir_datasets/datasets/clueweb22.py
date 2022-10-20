from ir_datasets import registry
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import ClueWeb22Language, ClueWeb22Subset, \
    ClueWeb22Docs
from ir_datasets.util import DownloadConfig, home_path

NAME = "clueweb22"


def _init():
    documentation = YamlDocumentation(f"docs/{NAME}.yaml")
    base_path = home_path() / NAME
    download = DownloadConfig.context(NAME, base_path)

    registry.register(
        NAME,
        Dataset(documentation("_")),
    )

    for subset in ClueWeb22Subset:
        subset_tag = subset.value.tag
        registry.register(
            f"{NAME}/{subset_tag}",
            Dataset(
                documentation(subset_tag),
                ClueWeb22Docs(NAME, download["docs"], subset)
            ),
        )
        for language in ClueWeb22Language:
            language_tag = f"{subset.value.tag}/{language.value.tag}"
            registry.register(
                f"{NAME}/{language_tag}",
                Dataset(
                    documentation(language_tag),
                    ClueWeb22Docs(NAME, download["docs"], subset, language)
                ),
            )


_init()
