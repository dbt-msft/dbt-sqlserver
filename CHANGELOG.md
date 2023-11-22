# Changelog

### v1.6.1

## Features

* Fabric DW now supports sp_rename. Starting v1.6.1 sp_rename is metadata operation
* Enabled table clone feature

## Enhancements

* Addressed [Issue 53](https://github.com/microsoft/dbt-fabric/issues/53)
* Added explicit support for [Issue 76 - ActiveDirectoryServicePrincipal authentication](https://github.com/microsoft/dbt-fabric/issues/74)
* Removed port number support in connection string as it is no longer required in Microsoft Fabric DW
* Removed MSI authentication as it does not make sense for Microsoft Fabric.
* Table lock hints are not supported by Fabric DW
* Supported authentication modes are ActiveDirectory* and AZ CLI

### v1.6.0

## Features

* Supporting dbt-core 1.6.2
* Adding limit - new args to adapter.execute() function
* Added tests related to dbt-debug to test --connection parameter
* Added adapter zone tests

## Dependencies

* Bump from pytest==7.4.0 to pytest==7.4.2
* Bump from pre-commit==3.3.3 to 3.4.0
* Bump from dbt-tests-adapter~=1.5.2 to 1.6.2
* Bump from actions@v3 to v4
* Bump from build-push-action@v4.0.0 to 4.2.1
### v1.5.0

Releasing 1.5 version for dbt-cloud integration.

### v1.5.0-rc1

* Upgraded dbt-fabric adapter to match dbt-core & dbt-tests-adapter version 1.5.2.
* Added constraint support to dbt-fabric adapter.
    * Check constraints are not supported.
    * Column & model constraints are not supported in CREATE TABLE command by Microsoft Fabric Data Warehouse. Column and model constraints are implemented by ALTER TABLE ADD Constraints command.
    * user-defined names for constraints are not currently supported. naming is handled by the adapter, until `SP_RENAME` is supported in Fabric
    * Added tests related to constraints.
* Bumped wheel, precommit, docker package versions.


### v1.4.0-rc3

Updated connection property to track dbt telemetry by Microsoft.

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
