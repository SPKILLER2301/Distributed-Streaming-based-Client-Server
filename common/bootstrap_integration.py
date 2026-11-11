"""Load A2's documented interface without silently masking a broken installation.

Until transport/bootstrap.py is handed over, the app runs the explicit v1 UDP
baseline. This is not evidence that rubric Step 2 is implemented.
"""

from importlib import import_module


def load_bootstrap():
    try:
        module = import_module("transport.bootstrap")
    except ModuleNotFoundError as exc:
        if exc.name != "transport.bootstrap":
            raise
        return None
    return module.BootstrapServer, module.bootstrap_connect
