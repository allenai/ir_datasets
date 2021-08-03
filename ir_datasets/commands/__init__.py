from . import doc_fifos
from . import export
from . import lookup
from . import list as list_cmd
from . import documentation
from . import build_clueweb_warc_indexes
from . import build_download_cache
from . import build_c4_checkpoints
from . import clean
from . import compute_metadata

COMMANDS = {
	'doc_fifos': doc_fifos.main,
    'export': export.main,
    'lookup': lookup.main,
    'list': list_cmd.main,
    'documentation': documentation.main,
    'build_clueweb_warc_indexes': build_clueweb_warc_indexes.main,
    'build_c4_checkpoints': build_c4_checkpoints.main,
    'build_download_cache': build_download_cache.main,
    'clean': clean.main,
    'compute_metadata': compute_metadata.main,
}
