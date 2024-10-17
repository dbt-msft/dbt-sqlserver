from dataclasses import dataclass
from typing import Optional

from dbt.adapters.protocol import AdapterConfig

from dbt.adapters.sqlserver.relation_configs import SQLServerIndexConfig


@dataclass
class SQLServerConfigs(AdapterConfig):
    auto_provision_aad_principals: Optional[bool] = False
    indexes: Optional[list[SQLServerIndexConfig]] = None
