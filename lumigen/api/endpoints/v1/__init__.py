import os
import importlib
import glob

# Dynamically import all modules in the current directory
# excluding __init__.py itself
for module_path in glob.glob(os.path.join(os.path.dirname(__file__), "*.py")):
    module_name = os.path.basename(module_path)[:-3]  # Remove '.py' extension
    if module_name != "__init__":
        module = importlib.import_module(f".{module_name}", package=__package__)
        globals()[module_name] = module  # Make it accessible as v1.module_name