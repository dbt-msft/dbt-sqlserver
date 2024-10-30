from dataclasses import dataclass
from typing import Optional, Tuple

from dbt.adapters.fabric import FabricConfigs

from dbt.adapters.sqlserver.relation_configs import SQLServerIndexConfig


@dataclass
class SQLServerConfigs(FabricConfigs):
    indexes: Optional[Tuple[SQLServerIndexConfig]] = None
