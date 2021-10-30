from . import doc_fifos
from . import export
from . import lookup
from . import list as list_cmd
from . import build_clueweb_warc_indexes
from . import build_download_cache
from . import build_c4_checkpoints
from . import clean
from . import generate_metadata

COMMANDS = {
	'doc_fifos': doc_fifos.main,
    'export': export.main,
    'lookup': lookup.main,
    'list': list_cmd.main,
    'build_clueweb_warc_indexes': build_clueweb_warc_indexes.main,
    'build_c4_checkpoints': build_c4_checkpoints.main,
    'build_download_cache': build_download_cache.main,
    'clean': clean.main,
    'generate_metadata': generate_metadata.main,
}
