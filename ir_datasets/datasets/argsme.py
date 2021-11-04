from ir_datasets import Dataset, registry
from ir_datasets.datasets.base import YamlDocumentation
from ir_datasets.formats.argsme import ArgsMeArguments
from ir_datasets.util import DownloadConfig, home_path, Cache, ZipExtract

NAME = "argsme"

SUBSETS = {
    '2020-04-01/debateorg': (338620, "en"),
    '2020-04-01/debatepedia': (21197, "en"),
    '2020-04-01/debatewise': (14353, "en"),
    '2020-04-01/idebate': (13522, "en"),
    '2020-04-01/parliamentary': (48, "en"),
}


def _init():
    base_path = home_path() / NAME

    documentation = YamlDocumentation(f"docs/{NAME}.yaml")
    download_config = DownloadConfig.context(NAME, base_path)

    base = Dataset(documentation('_'))

    subsets = {
        name: Dataset(
            ArgsMeArguments(
                Cache(
                    ZipExtract(
                        download_config[name],
                        f"{name}.json"
                    ),
                    base_path / f"{name}.json"
                ),
                namespace=f"{NAME}/{name}",
                language=language,
                count_hint=count_hint
            ),
            documentation(name)
        )
        for name, (count_hint, language) in SUBSETS.items()
    }

    registry.register(NAME, base)
    for name, arguments in subsets.items():
        registry.register(f'{NAME}/{name}', arguments)

    return base, subsets


dataset = _init()
