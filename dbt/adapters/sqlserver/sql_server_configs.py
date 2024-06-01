from dataclasses import dataclass

from dbt.adapters.fabric import FabricConfigs


@dataclass
class SQLServerConfigs(FabricConfigs):
    pass
