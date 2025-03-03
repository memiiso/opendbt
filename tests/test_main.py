import os
import sys
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from base_dbt_test import BaseDbtTest
from opendbt import OpenDbtCli
from opendbt.__main__ import main


class TestOpenDbtCliMain(BaseDbtTest):

    # @patch("opendbt.OpenDbtCli.invoke")
    # def test_main_with_env_vars(self, mock_invoke):
    #     os.environ["DBT_PROJECT_DIR"] = self.DBTCORE_DIR.as_posix()
    #     # os.environ["DBT_PROFILES_DIR"] = self.DBTCORE_DIR.as_posix()
    #     main()
    #     mock_invoke.assert_called_once()
    #     if "DBT_PROJECT_DIR" in os.environ:
    #         del os.environ["DBT_PROJECT_DIR"]
    #     if "DBT_PROFILES_DIR" in os.environ:
    #         del os.environ["DBT_PROFILES_DIR"]

    @patch("opendbt.__main__.OpenDbtCli")
    @patch("opendbt.__main__.OpenDbtCli.invoke")
    def test_main_with_project_dir_arg(self, mock_cli, mock_cli_invoke):
        # Test with --project-dir argument
        test_project_dir = self.DBTFINANCE_DIR.resolve()
        test_profiles_dir = self.DBTFINANCE_DIR.resolve()
        sys.argv = ["main.py", "--project-dir", str(test_project_dir), "--profiles-dir", str(test_profiles_dir), "ls"]
        main()
        mock_cli.assert_called_once()
        mock_cli:OpenDbtCli
        self.assertEqual(mock_cli.profiles_dir, test_profiles_dir)
        project_dir = mock_cli.call_args[1]["project_dir"]
        profiles_dir = mock_cli.call_args[1]["profiles_dir"]
        self.assertEqual(project_dir, test_project_dir)
        self.assertEqual(profiles_dir, test_profiles_dir)
        self.assertEqual(mock_cli_invoke.call_args[1]["project_dir"], "ls")

    @patch("opendbt.OpenDbtCli.invoke")
    def test_main_with_profiles_dir_arg(self, mock_invoke):
        # Test with --profiles-dir argument
        test_profiles_dir = Path("./test_profiles")
        sys.argv = ["main.py", "--profiles-dir", str(test_profiles_dir)]
        main()
        mock_invoke.assert_called_once()
        profiles_dir = mock_invoke.call_args[1]["profiles_dir"]
        self.assertEqual(profiles_dir, test_profiles_dir)

    @patch("opendbt.OpenDbtCli.invoke")
    def test_main_with_env_vars(self, mock_invoke):
        # Test with environment variables
        os.environ["DBT_PROJECT_DIR"] = "./env_project"
        os.environ["DBT_PROFILES_DIR"] = "./env_profiles"
        main()
        mock_invoke.assert_called_once()
        project_dir = mock_invoke.call_args[1]["project_dir"]
        profiles_dir = mock_invoke.call_args[1]["profiles_dir"]
        self.assertEqual(project_dir, Path("./env_project"))
        self.assertEqual(profiles_dir, Path("./env_profiles"))

    @patch("opendbt.OpenDbtCli.invoke")
    def test_main_arg_override_env_vars(self, mock_invoke):
        # Test that command-line arguments override environment variables
        os.environ["DBT_PROJECT_DIR"] = "./env_project"
        os.environ["DBT_PROFILES_DIR"] = "./env_profiles"
        test_project_dir = Path("./arg_project")
        test_profiles_dir = Path("./arg_profiles")

        sys.argv = ["main.py", "--project-dir", str(test_project_dir), "--profiles-dir", str(test_profiles_dir)]
        main()
        mock_invoke.assert_called_once()
        project_dir = mock_invoke.call_args[1]["project_dir"]
        profiles_dir = mock_invoke.call_args[1]["profiles_dir"]
        self.assertEqual(project_dir, test_project_dir)
        self.assertEqual(profiles_dir, test_profiles_dir)

    @patch("opendbt.OpenDbtCli.invoke")
    def test_main_with_known_args(self, mock_invoke):
        test_project_dir = Path("./test_project")
        sys.argv = ["main.py", "--project-dir", str(test_project_dir), "run","--select", "my_model"]
        main()
        mock_invoke.assert_called_once()
        project_dir = mock_invoke.call_args[1]["project_dir"]
        args= mock_invoke.call_args[0][0]
        self.assertEqual(project_dir, test_project_dir)
        self.assertEqual(args, ["run", "--select", "my_model"])

    @patch("opendbt.OpenDbtCli.invoke")
    def test_main_with_args(self, mock_invoke):
        main()
        mock_invoke.assert_called_once()

    @patch("opendbt.OpenDbtCli")
    def test_main_default(self, mock_cli):
        # Test default behavior (no arguments, no environment variables)
        main()
        mock_cli.assert_called_once()
        print("==============")
        print(type(mock_cli))
        print("==============")
        print("==============")
        project_dir = mock_cli.call_args[1]["project_dir"]
        profiles_dir = mock_cli.call_args[1]["profiles_dir"]
        self.assertEqual(project_dir, Path.cwd())
        self.assertIsNone(profiles_dir)