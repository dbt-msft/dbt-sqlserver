from dataclasses import dataclass
from typing import Optional

from dbt.adapters.protocol import AdapterConfig


@dataclass
class SQLServerConfigs(AdapterConfig):
    auto_provision_aad_principals: Optional[bool] = False
