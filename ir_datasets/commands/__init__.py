from . import export
from . import lookup
from . import list as list_cmd
from . import documentation

COMMANDS = {
    'export': export.main,
    'lookup': lookup.main,
    'list': list_cmd.main,
    'documentation': documentation.main,
}
