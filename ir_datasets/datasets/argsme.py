from itertools import chain
from typing import Dict

from ir_datasets import registry
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import ArgsMeDocs, ArgsMeProcessedDocs, ArgsMeCombinedDocs
from ir_datasets.util import DownloadConfig, home_path, Cache, ZipExtract, TarExtract

NAME = "argsme"

SUBSETS = {
    '1.0': (387692, "en", "args-me.json"),
    '1.0-cleaned': (382545, "en", "args-me-1.0-cleaned.json"),
    '2020-04-01/debateorg': (338620, "en", "debateorg.json"),
    '2020-04-01/debatepedia': (21197, "en", "debatepedia.json"),
    '2020-04-01/debatewise': (14353, "en", "debatewise.json"),
    '2020-04-01/idebate': (13522, "en", "idebate.json"),
    '2020-04-01/parliamentary': (48, "en", "parliamentary.json"),
}

PROCESSED_SUBSETS = {
    '2020-04-01/processed': (365408, "en", "args_processed.csv"),
}

COMBINED_SUBSETS = {
    '2020-04-01': (
        [
            '2020-04-01/debateorg',
            '2020-04-01/debatepedia',
            '2020-04-01/debatewise',
            '2020-04-01/idebate',
            '2020-04-01/parliamentary'
        ],
        387740,
        "en"
    ),
}


def _init():
    base_path = home_path() / NAME

    documentation = YamlDocumentation(f"docs/{NAME}.yaml")
    download_config = DownloadConfig.context(NAME, base_path)

    base = Dataset(documentation('_'))

    # Arguments that can be loaded from Zenodo.
    arguments: Dict[str, ArgsMeDocs] = {
        name: ArgsMeDocs(
            Cache(
                ZipExtract(
                    download_config[name],
                    zip_path
                ),
                base_path / f"{name}.json"
            ),
            namespace=f"{NAME}/{name}",
            language=language,
            count_hint=count_hint
        )
        for name, (count_hint, language, zip_path)
        in SUBSETS.items()
    }

    # Processed arguments that can be loaded from Zenodo.
    processed_arguments: Dict[str, ArgsMeProcessedDocs] = {
        name: ArgsMeProcessedDocs(
            Cache(
                TarExtract(
                    download_config[name],
                    zip_path
                ),
                base_path / f"{name}.json"
            ),
            namespace=f"{NAME}/{name}",
            language=language,
            count_hint=count_hint
        )
        for name, (count_hint, language, zip_path)
        in PROCESSED_SUBSETS.items()
    }

    # Arguments that are combined versions of other subsets.
    combined_arguments: Dict[str, ArgsMeCombinedDocs] = {
        name: ArgsMeCombinedDocs(
            base_path / f"{name}.json",
            [arguments[subset_name] for subset_name in subset_names],
            namespace=f"{NAME}/{name}",
            language=language,
            count_hint=count_hint
        )
        for name, (subset_names, count_hint, language)
        in COMBINED_SUBSETS.items()
    }

    # Wrap in datasets with documentation.
    datasets = {
        name: Dataset(
            arguments,
            documentation(name)
        )
        for name, arguments in chain(
            arguments.items(),
            processed_arguments.items(),
            combined_arguments.items(),
        )
    }

    # NOTE: the following datasets are defined in touche.py:
    #  - argsme/1.0/touche-2020-task-1/uncorrected
    #  - argsme/2020-04-01/touche-2020-task-1
    #  - argsme/2020-04-01/touche-2020-task-1/uncorrected
    #  - argsme/2020-04-01/touche-2021-task-1
    #  - argsme/2020-04-01/processed/touche-2022-task-1

    # Register datasets.
    registry.register(NAME, base)
    for name, arguments in datasets.items():
        registry.register(f'{NAME}/{name}', arguments)

    return base, datasets


dataset = _init()
