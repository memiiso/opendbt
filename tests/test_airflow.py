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
        import subprocess
        start = time.time()
        counter = 0
        print(f"Waiting for Airflow to be ready at {cls.base_url}...")

        while time.time() - start < timeout:
            try:
                response = requests.get(f"{cls.base_url}/health", timeout=5)
                if response.status_code == 200:
                    print(f"✓ Airflow is ready!")

                    # Show container logs for debugging
                    print("\n" + "="*60)
                    print("AIRFLOW CONTAINER LOGS (last 10 lines):")
                    print("="*60)
                    try:
                        logs = subprocess.run(
                            ["docker", "logs", "--tail", "10", "airflow"],
                            capture_output=True, text=True, timeout=10
                        )
                        print(logs.stdout)
                        if logs.stderr:
                            print("STDERR:", logs.stderr)
                    except Exception as e:
                        print(f"Could not fetch logs: {e}")
                    print("="*60 + "\n")

                    return
            except Exception as e:
                pass
            time.sleep(5)
            counter += 5
            print(f"Waited for {counter} sec")

        # Timeout - show logs before failing
        print("\n" + "="*60)
        print("TIMEOUT - AIRFLOW CONTAINER LOGS:")
        print("="*60)
        try:
            logs = subprocess.run(
                ["docker", "logs", "--tail", "100", "airflow"],
                capture_output=True, text=True, timeout=10
            )
            print(logs.stdout)
            if logs.stderr:
                print("STDERR:", logs.stderr)
        except Exception as e:
            print(f"Could not fetch logs: {e}")
        print("="*60 + "\n")

        raise TimeoutError(f"Airflow did not start in {timeout} seconds")

    # Removed _copy_plugin_config - no longer needed with env var approach


@unittest.skipIf(SKIP_AIRFLOW_TESTS, "Airflow Docker tests disabled. Set RUN_AIRFLOW_TESTS=1 to enable")
class TestAirflowLegacyMode(AirflowTestBase):
    """Test single-project legacy mode (backward compatibility)"""

    @classmethod
    def setUpClass(cls):
        import subprocess

        print("\n" + "="*60)
        print("Testing Legacy Single-Project Mode")
        print("="*60)

        # Set environment variable for single-project mode
        os.environ['AIRFLOW_PLUGIN_MODE'] = 'single'

        # Start Airflow (first clean up old containers to avoid cached files)
        os.chdir(cls.resources_dir.joinpath('airflow').as_posix())
        cls._compose = DockerCompose(
            cls.resources_dir.joinpath('airflow').as_posix(),
            compose_file_name="docker-compose.yaml",
            build=True  # Force rebuild to ensure fresh image with updated opendbt code
        )
        cls._compose.start()

        cls.base_url = "http://localhost:8080"

        print(f"\nAirflow URLs:")
        print(f"  Home: {cls.base_url}/home")
        print(f"  DBT Docs: {cls.base_url}/dbt/dbt_docs_index.html")

        # Wait for Airflow
        cls._wait_for_airflow()

        # Debug: Check files inside container
        print("\n" + "="*60)
        print("CONTAINER FILE SYSTEM CHECK:")
        print("="*60)

        commands_to_check = [
            ("Plugin file", "docker exec airflow ls -la /opt/airflow/plugins/"),
            ("Plugin content", "docker exec airflow head -20 /opt/airflow/plugins/airflow_dbtdocs_page.py"),
            ("Opendbt version", "docker exec airflow pip show opendbt"),
            ("Target directory", "docker exec airflow ls -la /opt/dbtcore/target/"),
        ]

        for desc, cmd in commands_to_check:
            print(f"\n→ {desc}:")
            try:
                result = subprocess.run(
                    cmd.split(), capture_output=True, text=True, timeout=10
                )
                print(result.stdout)
                if result.stderr:
                    print(f"STDERR: {result.stderr}")
            except Exception as e:
                print(f"  Error: {e}")

        print("="*60 + "\n")

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
        url = f"{self.base_url}/dbt/dbt_docs_index.html"
        print(f"  URL: {url}")

        response = requests.get(url, timeout=10)
        print(f"  Status: {response.status_code}")
        print(f"  Headers: {dict(response.headers)}")

        if response.status_code != 200:
            print(f"  Response body (first 500 chars):\n{response.text[:500]}")

            # Check if plugin is loaded
            print("\n  Checking plugin status...")
            try:
                plugin_check = requests.get(f"{self.base_url}/api/v1/plugins", timeout=5)
                print(f"  Plugins API status: {plugin_check.status_code}")
                if plugin_check.status_code == 200:
                    print(f"  Plugins: {plugin_check.text[:500]}")
            except Exception as e:
                print(f"  Could not check plugins: {e}")

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


@unittest.skipIf(SKIP_AIRFLOW_TESTS, "Airflow Docker tests disabled. Set RUN_AIRFLOW_TESTS=1 to enable")
class TestAirflowMultiProjectMode(AirflowTestBase):
    """Test multi-project mode with project switching"""

    @classmethod
    def setUpClass(cls):
        print("\n" + "="*60)
        print("Testing Multi-Project Mode")
        print("="*60)

        # Set environment variable for multi-project mode
        os.environ['AIRFLOW_PLUGIN_MODE'] = 'multi'

        # Start Airflow (first clean up old containers to avoid cached files)
        os.chdir(cls.resources_dir.joinpath('airflow').as_posix())
        cls._compose = DockerCompose(
            cls.resources_dir.joinpath('airflow').as_posix(),
            compose_file_name="docker-compose.yaml",
            build=True  # Force rebuild to ensure fresh image with updated opendbt code
        )
        cls._compose.start()

        cls.base_url = "http://localhost:8080"

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
