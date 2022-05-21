#!/usr/bin/env python3

import asyncio
import os

from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.sql.aio import SqlManagementClient

credential = DefaultAzureCredential()
subscription_id = os.getenv("DBT_AZURE_SUBSCRIPTION_ID")
sql_server_name = os.getenv("DBT_AZURESQL_SERVER").replace(".database.windows.net", "")
database_name = os.getenv("DBT_AZURESQL_DB")
resource_group_name = os.getenv("DBT_AZURE_RESOURCE_GROUP_NAME")


async def resume_azsql() -> bool:
    try:
        client = SqlManagementClient(credential=credential, subscription_id=subscription_id)
        db = await client.databases.get(resource_group_name=resource_group_name, server_name=sql_server_name,
                                        database_name=database_name)
        if db.status == "Paused":
            res = await client.databases.begin_resume(resource_group_name=resource_group_name,
                                                      server_name=sql_server_name, database_name=database_name)
            print("Resuming SQL Database")
            await res.wait()
        elif db.status in ("Pausing", "Resuming"):
            print(f"SQL Database is {db.status}, waiting a minute and trying again")
            await asyncio.sleep(60)
            return True
        else:
            print(f"SQL Database is already {db.status}")

        return False
    finally:
        await client.close()


async def main():
    if await resume_azsql():
        await main()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
