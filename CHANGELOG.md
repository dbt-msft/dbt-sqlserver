# Changelog

### v1.4.0-rc3

Updated connection property to track dbt telemetry

### v1.4.0-rc2

Fixed view rename relation macro.
Bumped required python packages versions.

### v1.4.0-rc1

Requires dbt 1.4.5 and previous versions are not supported by Fabric Data Warehouse. Microsoft is actively releasing/adding T-SQL support. Please raise issues in case of any bugs.

#### DBT Supported features
- All materializations and resource features such as Tables, Views, Seeds, sources, tests and dbt docs are supported.
- Advanced features such as incremental and snapshot features may work but are not planned to support in 1.4.5.

We recommend you to read Microsoft Fabric Data Warehouse [documentation](https://review.learn.microsoft.com/en-us/fabric/data-warehouse/?branch=main) before using the adapter.

#### Important things to consider when using dbt-fabric adapter
- SQL/Basic authentication is not supported by Fabric Data Warehouse. CLI and Service Principal authentication are currently supported.
- Please review the [T-SQL commands](https://review.learn.microsoft.com/en-us/fabric/data-warehouse/tsql-surface-area#limitations) not supported in Fabric Data Warehouse. Some of T-SQL commands such as ALTER TABLE ADD/ALTER/DROP COLUMN, MERGE, TRUNCATE, SP_RENAME are supported by dbt-fabric adapter using CTAS, DROP and CREATE commands.
- Many data types are supported and a few aren't. Please review [this](https://review.learn.microsoft.com/en-us/fabric/data-warehouse/data-types?branch=main) link for supported and unsupported data types.

#### Unsupported features
- nolock
- provisioning and granting access to basic user (sql server authentication)
- CTAS supports select on views/tables with underlying table definition. CREATE TABLE AS SELECT 1 AS Id - is not supported.
- datetime data type
- SP_RENAME
