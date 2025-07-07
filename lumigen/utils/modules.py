import importlib
import inspect
import pkgutil
import typing as t
import logging

_logger = logging.getLogger(__name__)


def find_subclasses_in_package(
    package, 
    base_class: t.Type
) -> t.List[t.Type]:
    """
    Finds all subclasses of a base class within a given package and its
    subpackages, using module walking instead of file system scanning.

    Args:
        package: The imported package object to search within (e.g., `import transformers`).
        base_class: The base class to search for.
    
    Returns:
        A list of all subclasses found.
    """
    subclasses = []
    
    if not hasattr(package, '__path__'):
        _logger.error(f"'{package.__name__}' is not a package. Cannot search for modules.")
        return []

    # walk_packages finds all modules in the package
    for importer, modname, ispkg in pkgutil.walk_packages(
        path=package.__path__, 
        prefix=package.__name__ + '.'
    ):
        try:
            # Dynamically import the module
            module = importlib.import_module(modname)
            
            # Inspect the module for classes
            for _, obj in inspect.getmembers(module, inspect.isclass):
                # Check if it's a subclass of base_class but not base_class itself
                if issubclass(obj, base_class) and obj is not base_class:
                    subclasses.append(obj)
        except Exception:
            _logger.error(f"Error loading or inspecting module {modname}", exc_info=True)
            
    return subclasses