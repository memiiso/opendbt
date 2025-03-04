import os
import sys
from pathlib import Path
from unittest.mock import patch, Mock

from base_dbt_test import BaseDbtTest
from opendbt.__main__ import main


class TestOpenDbtCliMain(BaseDbtTest):

    @patch("opendbt.__main__.OpenDbtCli")
    def test_main_with_project_dir_arg(self, mock_cli):
        test_project_dir = self.DBTFINANCE_DIR.resolve()
        test_profiles_dir = self.DBTFINANCE_DIR.resolve()
        sys.argv = ["main.py", "--project-dir", str(test_project_dir), "--profiles-dir", str(test_profiles_dir), "ls"]
        mock_instance = Mock(project_dir=test_project_dir, profiles_dir=test_profiles_dir)
        mock_cli.return_value = mock_instance
        main()
        mock_cli.assert_called_once_with(project_dir=test_project_dir, profiles_dir=test_profiles_dir)
        mock_instance.invoke.assert_called_once_with(args=['ls'])
