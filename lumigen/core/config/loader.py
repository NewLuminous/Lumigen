from __future__ import annotations

import typing as t
from pathlib import Path

from sqlglot.helper import ensure_list

from core.config.root import Config
from utils import merge_dicts
from utils.errors import ConfigError
from utils.yaml import load as yaml_load

C = t.TypeVar("C", bound=Config)


def load_config(
    path: str | Path = "",
    config_name: str = "config",
    config_type: t.Type[C] = Config,
) -> C:
    absolute_path = Path(t.cast(t.Union[str, Path], path)).absolute()
    
    if not path:
        config = load_config_from_paths(
            config_type,
            project_paths=[absolute_path / "config.yml", absolute_path / "config.yaml"],
            config_name=config_name,
        )
    else:
        config = load_config_from_paths(
            config_type,
            project_paths=[absolute_path],
            config_name=config_name,
        )

    return config


def load_configs(
    config: t.Optional[t.Union[str, C]],
    config_type: t.Type[C],
    paths: t.Union[str | Path, t.Iterable[str | Path]],
    root_path: t.Optional[Path] = None,
) -> t.Dict[Path, C]:
    root_path = root_path or Path.home()
    config = config or "config"

    absolute_paths = [
        Path(t.cast(t.Union[str, Path], path)).absolute() for path in ensure_list(paths)
    ]

    if not isinstance(config, str):
        if type(config) != config_type:
            config = convert_config_type(config, config_type)
        return {path: config for path in absolute_paths}

    personal_paths = [
        root_path / "config.yml",
        root_path / "config.yaml",
    ]

    return {
        path: load_config_from_paths(
            config_type,
            project_paths=[path / "config.yml", path / "config.yaml"],
            personal_paths=personal_paths,
            config_name=config,
        )
        for path in absolute_paths
    }


def load_config_from_paths(
    config_type: t.Type[C],
    project_paths: t.Optional[t.List[Path]] = None,
    personal_paths: t.Optional[t.List[Path]] = None,
    config_name: str = "config",
) -> C:
    project_paths = project_paths or []
    personal_paths = personal_paths or []
    visited_folders: t.Set[Path] = set()
    non_python_configs = []

    if not project_paths or not any(path.exists() for path in project_paths):
        raise ConfigError(
            "Project config could not be found."
        )

    for path in [*project_paths, *personal_paths]:
        if not path.exists():
            continue

        if not path.is_file():
            raise ConfigError(f"Path '{path}' must be a file.")

        parent_path = path.parent
        if parent_path in visited_folders:
            raise ConfigError(f"Multiple configuration files found in folder '{parent_path}'.")
        visited_folders.add(parent_path)

        extension = path.name.split(".")[-1].lower()
        if extension in ("yml", "yaml"):
            if config_name != "config":
                raise ConfigError(
                    "YAML configs do not support multiple configs."
                )
            non_python_configs.append(load_config_from_yaml(path))
        else:
            raise ConfigError(
                f"Unsupported config file extension '{extension}' in config file '{path}'."
            )

    if not non_python_configs:
        raise ConfigError(
            "Config could not be found."
        )

    non_python_config_dict = merge_dicts(*non_python_configs)

    non_python_config = config_type.parse_obj(non_python_config_dict)

    return non_python_config


def load_config_from_yaml(path: Path) -> t.Dict[str, t.Any]:
    return yaml_load(path)


def convert_config_type(
    config_obj: Config,
    config_type: t.Type[C],
) -> C:
    return config_type.parse_obj(config_obj.dict())