# YamlParser

The `#!python YamlParser` manages all the interactions with Cassandra.

## Import

```python
from primeight.parser import YamlParser
```

## Methods

### parse

Read and parse the contents of a __Yaml__ file.
This method also validates that the Yaml file has the correct format.

__Parameters:__

- _path_ `str or pathlib.Path` __[Required]__: path to the Yaml configuration.

__Return:__ `#!python Dict`
