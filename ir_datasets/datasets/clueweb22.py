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
        if subset.value.hide:
            continue
        subset_tag = subset.value.tag
        registry.register(
            f"{NAME}/{subset_tag}",
            Dataset(
                documentation(subset_tag),
                ClueWeb22Docs(
                    name=NAME,
                    source=download["docs"],
                    subset=subset,
                    subset_view=subset,
                )
            ),
        )
        for language in ClueWeb22Language:
            language_tag = f"{subset_tag}/{language.value.tag}"
            registry.register(
                f"{NAME}/{language_tag}",
                Dataset(
                    documentation(language_tag),
                    ClueWeb22Docs(
                        name=NAME,
                        source=download["docs"],
                        subset=subset,
                        subset_view=subset,
                        language=language,
                    )
                ),
            )
        for subset_view in subset.subset_views - {subset}:
            subset_view_tag = f"{subset_tag}/as-{subset_view.value.tag}"
            registry.register(
                f"{NAME}/{subset_view_tag}",
                Dataset(
                    documentation(subset_view_tag),
                    ClueWeb22Docs(
                        name=NAME,
                        source=download["docs"],
                        subset=subset,
                        subset_view=subset_view,
                    )
                ),
            )
            for language in ClueWeb22Language:
                language_tag = f"{subset_view_tag}/{language.value.tag}"
                registry.register(
                    f"{NAME}/{language_tag}",
                    Dataset(
                        documentation(language_tag),
                        ClueWeb22Docs(
                            name=NAME,
                            source=download["docs"],
                            subset=subset,
                            subset_view=subset_view,
                            language=language,
                        )
                    ),
                )


_init()
