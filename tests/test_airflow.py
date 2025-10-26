import os
import shutil
import time
import unittest
from pathlib import Path
from time import sleep

import requests
from testcontainers.compose import DockerCompose


# Environment variable to control when Airflow Docker tests run
# Set RUN_AIRFLOW_TESTS=1 to enable these tests
SKIP_AIRFLOW_TESTS = not os.getenv("RUN_AIRFLOW_TESTS")


class AirflowTestBase(unittest.TestCase):
    """Base class with common helper methods for Airflow tests"""

    _compose: DockerCompose = None
    resources_dir = Path(__file__).parent.joinpath('resources')
    base_url: str = None

    @classmethod
    def _wait_for_airflow(cls, timeout=120):
        """Wait for Airflow to be healthy"""
        start = time.time()
        print(f"Waiting for Airflow to be ready at {cls.base_url}...")
        while time.time() - start < timeout:
            try:
                response = requests.get(f"{cls.base_url}/health", timeout=5)
                if response.status_code == 200:
                    print(f"✓ Airflow is ready!")
                    return
            except Exception as e:
                pass
            time.sleep(3)
        raise TimeoutError(f"Airflow did not start in {timeout} seconds")

    @classmethod
    def _copy_plugin_config(cls, plugin_file):
        """Copy specified plugin config to active location"""
        source = cls.resources_dir / 'airflow/plugins' / plugin_file
        dest = cls.resources_dir / 'airflow/plugins/airflow_dbtdocs_page.py'
        shutil.copy(source, dest)
        print(f"✓ Copied plugin config: {plugin_file}")


@unittest.skipIf(SKIP_AIRFLOW_TESTS, "Airflow Docker tests disabled. Set RUN_AIRFLOW_TESTS=1 to enable")
class TestAirflowLegacyMode(AirflowTestBase):
    """Test single-project legacy mode (backward compatibility)"""

    @classmethod
    def setUpClass(cls):
        print("\n" + "="*60)
        print("Testing Legacy Single-Project Mode")
        print("="*60)

        # Copy legacy plugin config
        cls._copy_plugin_config('airflow_dbtdocs_page_legacy.py')

        # Start Airflow
        os.chdir(cls.resources_dir.joinpath('airflow').as_posix())
        cls._compose = DockerCompose(
            cls.resources_dir.joinpath('airflow').as_posix(),
            compose_file_name="docker-compose.yaml"
        )
        cls._compose.stop()
        cls._compose.start()

        port = cls._compose.get_service_port('airflow', 8080)
        cls.base_url = f"http://localhost:{port}"

        print(f"\nAirflow URLs:")
        print(f"  Home: {cls.base_url}/home")
        print(f"  DBT Docs: {cls.base_url}/dbt/dbt_docs_index.html")

        # Wait for Airflow
        cls._wait_for_airflow()

    @classmethod
    def tearDownClass(cls):
        print("\n" + "="*60)
        print("Stopping Legacy Mode Test")
        print("="*60)
        if cls._compose:
            cls._compose.stop()

    def test_legacy_index_page_accessible(self):
        """Test that DBT docs index page loads in legacy mode"""
        print("\n→ Testing index page accessibility...")
        response = requests.get(f"{self.base_url}/dbt/dbt_docs_index.html", timeout=10)
        self.assertEqual(response.status_code, 200)
        self.assertIn("<!DOCTYPE html>", response.text)
        print("  ✓ Index page accessible")

    def test_legacy_manifest_json(self):
        """Test manifest.json is accessible"""
        print("\n→ Testing manifest.json...")
        response = requests.get(f"{self.base_url}/dbt/manifest.json", timeout=10)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("metadata", data)
        self.assertIn("nodes", data)
        print(f"  ✓ Manifest contains {len(data.get('nodes', {}))} nodes")

    def test_legacy_catalog_json(self):
        """Test catalog.json is accessible"""
        print("\n→ Testing catalog.json...")
        response = requests.get(f"{self.base_url}/dbt/catalog.json", timeout=10)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("metadata", data)
        print("  ✓ Catalog accessible")

    def test_legacy_catalogl_json(self):
        """Test catalogl.json (enhanced catalog) is accessible"""
        print("\n→ Testing catalogl.json...")
        response = requests.get(f"{self.base_url}/dbt/catalogl.json", timeout=10)
        self.assertEqual(response.status_code, 200)
        print("  ✓ Enhanced catalog accessible")

    def test_legacy_no_projects_endpoint(self):
        """Test that /projects endpoint doesn't exist or returns legacy mode"""
        print("\n→ Testing /projects endpoint in legacy mode...")
        response = requests.get(f"{self.base_url}/dbt/projects", timeout=10)
        # In legacy mode, projects endpoint should still work but show legacy_mode=true
        if response.status_code == 200:
            data = response.json()
            # If endpoint exists, it should indicate legacy mode
            self.assertTrue(data.get("legacy_mode", False))
            print("  ✓ Projects endpoint shows legacy_mode=true")


@unittest.skipIf(SKIP_AIRFLOW_TESTS, "Airflow Docker tests disabled. Set RUN_AIRFLOW_TESTS=1 to enable")
class TestAirflowMultiProjectMode(AirflowTestBase):
    """Test multi-project mode with project switching"""

    @classmethod
    def setUpClass(cls):
        print("\n" + "="*60)
        print("Testing Multi-Project Mode")
        print("="*60)

        # Copy multi-project plugin config
        cls._copy_plugin_config('airflow_dbtdocs_page_multi.py')

        # Start Airflow
        os.chdir(cls.resources_dir.joinpath('airflow').as_posix())
        cls._compose = DockerCompose(
            cls.resources_dir.joinpath('airflow').as_posix(),
            compose_file_name="docker-compose.yaml"
        )
        cls._compose.stop()
        cls._compose.start()

        port = cls._compose.get_service_port('airflow', 8080)
        cls.base_url = f"http://localhost:{port}"

        print(f"\nAirflow URLs:")
        print(f"  Home: {cls.base_url}/home")
        print(f"  DBT Docs: {cls.base_url}/dbt/dbt_docs_index.html")
        print(f"  Projects API: {cls.base_url}/dbt/projects")
        print(f"  DBTCore: {cls.base_url}/dbt/dbt_docs_index.html?project=dbtcore")
        print(f"  DBTFinance: {cls.base_url}/dbt/dbt_docs_index.html?project=dbtfinance")

        # Wait for Airflow
        cls._wait_for_airflow()

    @classmethod
    def tearDownClass(cls):
        print("\n" + "="*60)
        print("Stopping Multi-Project Mode Test")
        print("="*60)
        if cls._compose:
            cls._compose.stop()

    def test_projects_endpoint(self):
        """Test /dbt/projects endpoint returns both projects"""
        print("\n→ Testing /projects endpoint...")
        response = requests.get(f"{self.base_url}/dbt/projects", timeout=10)
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIn("projects", data)
        self.assertIn("current", data)
        self.assertIn("legacy_mode", data)

        self.assertFalse(data["legacy_mode"], "Should not be in legacy mode")

        # Check both projects are listed
        project_names = [p["name"] for p in data["projects"]]
        self.assertIn("dbtcore", project_names)
        self.assertIn("dbtfinance", project_names)

        print(f"  ✓ Found {len(project_names)} projects: {', '.join(project_names)}")

        # Check validation info
        for project in data["projects"]:
            self.assertIn("is_valid", project)
            self.assertIn("has_manifest", project)
            self.assertIn("has_index", project)
            print(f"  ✓ {project['name']}: valid={project['is_valid']}, manifest={project['has_manifest']}")

    def test_default_project_loads(self):
        """Test that default project (dbtcore) loads without ?project= param"""
        print("\n→ Testing default project loads...")
        response = requests.get(f"{self.base_url}/dbt/dbt_docs_index.html", timeout=10)
        self.assertEqual(response.status_code, 200)
        self.assertIn("<!DOCTYPE html>", response.text)
        print("  ✓ Default project (dbtcore) loads successfully")

    def test_switch_to_dbtfinance(self):
        """Test switching to dbtfinance project via ?project= parameter"""
        print("\n→ Testing project switch to dbtfinance...")
        response = requests.get(f"{self.base_url}/dbt/dbt_docs_index.html?project=dbtfinance", timeout=10)
        self.assertEqual(response.status_code, 200)
        self.assertIn("<!DOCTYPE html>", response.text)
        print("  ✓ dbtfinance project loads successfully")

    def test_manifest_json_with_project_param(self):
        """Test manifest.json returns correct data for specific project"""
        print("\n→ Testing manifest.json with project parameter...")

        # Get dbtcore manifest
        response_core = requests.get(f"{self.base_url}/dbt/manifest.json?project=dbtcore", timeout=10)
        self.assertEqual(response_core.status_code, 200)
        manifest_core = response_core.json()
        nodes_core = len(manifest_core.get("nodes", {}))

        # Get dbtfinance manifest
        response_finance = requests.get(f"{self.base_url}/dbt/manifest.json?project=dbtfinance", timeout=10)
        self.assertEqual(response_finance.status_code, 200)
        manifest_finance = response_finance.json()
        nodes_finance = len(manifest_finance.get("nodes", {}))

        print(f"  ✓ dbtcore manifest: {nodes_core} nodes")
        print(f"  ✓ dbtfinance manifest: {nodes_finance} nodes")

        # They should be different (different number of models)
        self.assertNotEqual(nodes_core, nodes_finance, "Manifests should have different node counts")

    def test_catalog_json_with_project_param(self):
        """Test catalog.json works with project parameter"""
        print("\n→ Testing catalog.json with project parameter...")

        response_core = requests.get(f"{self.base_url}/dbt/catalog.json?project=dbtcore", timeout=10)
        self.assertEqual(response_core.status_code, 200)

        response_finance = requests.get(f"{self.base_url}/dbt/catalog.json?project=dbtfinance", timeout=10)
        self.assertEqual(response_finance.status_code, 200)

        print("  ✓ Both project catalogs accessible")

    def test_invalid_project_returns_404(self):
        """Test that invalid project name returns 404"""
        print("\n→ Testing invalid project handling...")
        response = requests.get(f"{self.base_url}/dbt/dbt_docs_index.html?project=nonexistent", timeout=10)
        self.assertEqual(response.status_code, 404)
        print("  ✓ Invalid project returns 404 as expected")

    def test_ui_has_project_selector(self):
        """Test that UI includes project selector dropdown"""
        print("\n→ Testing UI has project selector...")
        response = requests.get(f"{self.base_url}/dbt/dbt_docs_index.html", timeout=10)
        self.assertEqual(response.status_code, 200)

        # Check for project selector elements
        self.assertIn("project-selector", response.text)
        self.assertIn("fa-folder-open", response.text)  # Icon
        self.assertIn('/dbt/projects', response.text)   # API endpoint call

        print("  ✓ Project selector dropdown present in UI")


@unittest.skip("Manual test")
class TestAirflowBase(unittest.TestCase):
    """
    Test class used to do airflow tests.
    uses airflow docker image and mounts current code into it.
    login is disabled all users can access the UI as Admin. Airflow is set up as Public
    """
    _compose: DockerCompose = None
    resources_dir = Path(__file__).parent.joinpath('resources')

    @classmethod
    def setUpClass(cls):
        os.chdir(cls.resources_dir.joinpath('airflow').as_posix())
        cls._compose = DockerCompose(cls.resources_dir.joinpath('airflow').as_posix(),
                                     compose_file_name="docker-compose.yaml",
                                     # build=True,
                                     docker_command_path='podman'
                                     )
        cls._compose.stop()
        cls._compose.start()
        print(f"http://localhost:{cls._compose.get_service_port('airflow', 8080)}/home")
        print(f"http://localhost:{cls._compose.get_service_port('airflow', 8080)}/dbtdocs")
        print(f"http://localhost:{cls._compose.get_service_port('airflow', 8080)}/dbtdocs/perf_info.json")

    @classmethod
    def tearDownClass(cls):
        print("Running tearDownClass")
        if cls._compose:
            cls._compose.stop()

    def __exit__(self, exc_type, exc_val, traceback):
        if self._compose:
            self._compose.stop()

    def test_start_airflow_local_and_wait(self):
        """
        used to deploy the code inside docker airflow locally. UI login is disabled and made public!
        useful to run local airflow with the new code changes and check the changes in airflow ui
        while its running all the code changes are reflected in airflow after short time.
        :return:
        """
        sleep(99999999)
