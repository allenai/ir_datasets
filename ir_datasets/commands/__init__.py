from . import doc_fifos
from . import export
from . import lookup
from . import list as list_cmd
from . import documentation
from . import build_clueweb_warc_indexes

COMMANDS = {
	'doc_fifos': doc_fifos.main,
    'export': export.main,
    'lookup': lookup.main,
    'list': list_cmd.main,
    'documentation': documentation.main,
    'build_clueweb_warc_indexes': build_clueweb_warc_indexes.main,
}
