# detect if we are imported from the setup procedure (borrowed from numpy code)
try:
    __INGESTIFY_SETUP__
except NameError:
    __INGESTIFY_SETUP__ = False

if not __INGESTIFY_SETUP__:
    from .infra import retrieve_http
    from .source_base import Source, DatasetResource
    from .main import debug_source

__version__ = "0.8.0"
