import importlib
import os


_adapter_dir = os.path.dirname(__file__)

for filename in os.listdir(_adapter_dir):
    if filename.endswith(".py") and filename not in ("__init__.py",):
        module_name = filename[:-3]
        module = importlib.import_module(f".{module_name}", package=__name__)
        globals()[module_name] = module
