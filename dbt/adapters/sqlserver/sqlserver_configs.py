from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from dbt_common.contracts.config.base import MergeBehavior

from dbt.adapters.protocol import AdapterConfig
from dbt.adapters.sqlserver.relation_configs import SQLServerIndexConfig


@dataclass
class SQLServerConfigs(AdapterConfig):
    auto_provision_aad_principals: Optional[bool] = False
    prefer_single_alter_column: Optional[bool] = False
    column_type_expansion_max_rows: int = 1000000
    indexes: Optional[Tuple[SQLServerIndexConfig, ...]] = None
    # false (default) | warn | true - how index reconciliation treats
    # droppable indexes dbt didn't create (YAML may supply bool or str)
    drop_unmanaged_indexes: Optional[Any] = False
    # column-name -> DDM masking-function map for the model-level `masks`
    # surface. MergeBehavior.Update makes it merge key-wise across the config
    # chain (dbt_project.yml +masks defaults, .yml config, in-file config())
    # the same way `meta` composes, rather than the default clobber — so a
    # directory-level default and a per-model tweak combine instead of one
    # replacing the whole dict.
    masks: Optional[Dict[str, Any]] = field(
        default_factory=dict, metadata=MergeBehavior.Update.meta()
    )
