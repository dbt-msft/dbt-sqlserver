from dataclasses import dataclass

from dbt.adapters.protocol import AdapterConfig


@dataclass
class SQLServerConfigs(AdapterConfig):
    pass
