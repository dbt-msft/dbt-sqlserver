import os
import re

import yaml
from dbt.tests.adapter.dbt_debug.test_dbt_debug import BaseDebug, BaseDebugProfileVariable
from dbt.tests.util import run_dbt


class TestDebugFabric(BaseDebug):
    def test_ok(self, project):
        run_dbt(["debug"])
        assert "ERROR" not in self.capsys.readouterr().out

    def test_nopass(self, project):
        run_dbt(["debug", "--target", "nopass"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+profiles\.yml file"), "ERROR invalid")

    def test_wronguser(self, project):
        run_dbt(["debug", "--target", "wronguser"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+Connection test"), "ERROR")

    def test_empty_target(self, project):
        run_dbt(["debug", "--target", "none_target"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+output 'none_target'"), "misconfigured")


class TestDebugProfileVariableFabric(BaseDebugProfileVariable):
    pass


class TestDebugInvalidProjectFabric(BaseDebug):
    def test_empty_project(self, project):
        with open("dbt_project.yml", "w") as f:  # noqa: F841
            pass

        run_dbt(["debug", "--profile", "test"], expect_pass=False)
        splitout = self.capsys.readouterr().out.split("\n")
        self.check_project(splitout)

    def test_badproject(self, project):
        update_project = {"invalid-key": "not a valid key so this is bad project"}

        with open("dbt_project.yml", "w") as f:
            yaml.safe_dump(update_project, f)

        run_dbt(["debug", "--profile", "test"], expect_pass=False)
        splitout = self.capsys.readouterr().out.split("\n")
        self.check_project(splitout)

    def test_not_found_project(self, project):
        run_dbt(["debug", "--project-dir", "nopass"], expect_pass=False)
        splitout = self.capsys.readouterr().out.split("\n")
        self.check_project(splitout, msg="ERROR not found")

    def test_invalid_project_outside_current_dir(self, project):
        # create a dbt_project.yml
        project_config = {"invalid-key": "not a valid key in this project"}
        os.makedirs("custom", exist_ok=True)
        with open("custom/dbt_project.yml", "w") as f:
            yaml.safe_dump(project_config, f, default_flow_style=True)
        run_dbt(["debug", "--project-dir", "custom"], expect_pass=False)
        splitout = self.capsys.readouterr().out.split("\n")
        self.check_project(splitout)
