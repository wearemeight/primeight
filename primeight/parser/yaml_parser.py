from pathlib import Path
import logging

import yaml

from primeight.parser.parser import Parser


logger = logging.getLogger(__name__)


class YamlParser(Parser):

    @staticmethod
    def parse(path: str or Path) -> dict:
        """Load Yaml from disk."""
        config_file = path if isinstance(path, Path) else Path(path)

        if config_file.is_dir():
            raise IsADirectoryError(f"Table yaml '{path}' is a directory.")
        if not config_file.is_file():
            raise FileNotFoundError(f"Table yaml '{path}' not found.")

        yaml_config = config_file.read_text(encoding='utf-8')
        if len(yaml_config) == 0:
            raise EOFError("Yaml file is empty")

        try:
            content = yaml.safe_load(yaml_config)
        except yaml.MarkedYAMLError as err:
            mark = err.problem_mark
            raise SyntaxError(
                f"Table yaml '{path}' is not a valid yaml. "
                f"Error position: {mark['line']}:{mark['column']}"
            )
        except yaml.YAMLError:
            raise SyntaxError(f"Table yaml '{path}' is not a valid yaml.")

        YamlParser.is_valid_config(content)
        return content
