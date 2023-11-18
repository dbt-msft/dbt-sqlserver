import os

import pytest
from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase
from dbt.tests.util import run_dbt


class TestSeedBatchSizeMaxSQLServer(SeedConfigBase):
    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        return {
            "five_columns.csv": """seed_id,first_name,email,ip_address,birthday
1,Larry,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
2,Larry,lperkins1@toplist.cz,64.210.133.162,1978-05-09 04:15:14
3,Anna,amontgomery2@miitbeian.gov.cn,168.104.64.114,2011-10-16 04:07:57"""
        }

    def test_max_batch_size(self, project, logs_dir):
        run_dbt(["seed"])
        with open(os.path.join(logs_dir, "dbt.log"), "r") as fp:
            logs = "".join(fp.readlines())

        assert "Inserting batches of 400 records" in logs


class TestSeedBatchSizeCustomSQLServer(SeedConfigBase):
    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        return {
            "six_columns.csv": """seed_id,first_name,last_name,email,ip_address,birthday
1,Larry,King,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
2,Larry,Perkins,lperkins1@toplist.cz,64.210.133.162,1978-05-09 04:15:14
3,Anna,Montgomery,amontgomery2@miitbeian.gov.cn,168.104.64.114,2011-10-16 04:07:57"""
        }

    def test_custom_batch_size(self, project, logs_dir):
        run_dbt(["seed"])
        with open(os.path.join(logs_dir, "dbt.log"), "r") as fp:
            logs = "".join(fp.readlines())

        assert "Inserting batches of 350 records" in logs
