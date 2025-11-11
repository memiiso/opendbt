import os
import shutil
import time
import unittest
from pathlib import Path
from time import sleep

import requests
import yaml
from testcontainers.compose import DockerCompose


# Environment variable to control when Airflow Docker tests run
# Set RUN_AIRFLOW_TESTS=1 to enable these tests
SKIP_AIRFLOW_TESTS = not os.getenv("RUN_AIRFLOW_TESTS")

# Constants
SEPARATOR = "=" * 60
REQUEST_TIMEOUT = 10
PROJECT_CORE = "dbtcore"
PROJECT_FINANCE = "dbtfinance"


class TestAirflowBase(unittest.TestCase):
    """
    Base class for Airflow tests using testcontainers.
    Uses airflow docker image and mounts current code into it.
    Login is disabled - all users can access the UI as Admin. Airflow is set up as Public.
    """
    _compose: DockerCompose = None
    resources_dir = Path(__file__).parent.joinpath('resources')
    compose_dir = resources_dir / 'airflow'
    base_url: str = None

    @classmethod
    def _print_separator(cls, message=None):
        """Print separator line with optional message"""
        print(f"\n{SEPARATOR}")
        if message:
            print(message)
            print(SEPARATOR)

    @classmethod
    def _dbt_url(cls, path):
        """Construct DBT endpoint URL"""
        return f"{cls.base_url}/dbt/{path}"

    @classmethod
    def _setup_airflow(cls, title: str, pre_start_hook=None, post_start_hook=None):
        """
        Setup Airflow for tests with common initialization.

        Args:
            title: Title to display in output
            pre_start_hook: Optional callable to run before starting compose (e.g., for YAML modification)
            post_start_hook: Optional callable to run after container is up (e.g., for debugging container state)
        """
        cls._print_separator(f"Testing {title}")

        # Run pre-start hook if provided (e.g., YAML modification)
        if pre_start_hook:
            pre_start_hook(cls)

        # Start Airflow
        os.chdir(cls.compose_dir.as_posix())
        cls._compose = DockerCompose(
            cls.compose_dir.as_posix(),
            compose_file_name="docker-compose.yaml",
            build=True  # Force rebuild to ensure fresh image with updated opendbt code
        )
        cls._compose.start()

        # Use testcontainers built-in methods to get dynamic host/port
        host = cls._compose.get_service_host("airflow", 8080)
        port = cls._compose.get_service_port("airflow", 8080)
        cls.base_url = f"http://{host}:{port}"

        # Wait for Airflow using testcontainers built-in wait
        print(f"Waiting for Airflow to be ready at {cls.base_url}...")
        cls._compose.wait_for(f"http://{host}:{port}/health")
        print(f"✓ Airflow is ready!")

        # Run post-start hook if provided (e.g., debug container)
        if post_start_hook:
            post_start_hook(cls)

    @classmethod
    def _teardown_airflow(cls, title: str):
        """Teardown Airflow after tests"""
        cls._print_separator(f"Stopping {title}")
        if cls._compose:
            cls._compose.stop()

    @classmethod
    def _run_debug_commands(cls, commands_to_check):
        """Run docker exec debug commands and print output"""
        import subprocess

        cls._print_separator("CONTAINER FILE SYSTEM CHECK:")
        for desc, cmd in commands_to_check:
            print(f"\n→ {desc}:")
            try:
                result = subprocess.run(
                    cmd.split(), capture_output=True, text=True, timeout=REQUEST_TIMEOUT
                )
                print(result.stdout)
                if result.stderr:
                    print(f"STDERR: {result.stderr}")
            except Exception as e:
                print(f"  Error: {e}")

    @classmethod
    def _print_urls(cls, *url_descriptions):
        """Print formatted URLs"""
        print(f"\nAirflow URLs:")
        for desc, path in url_descriptions:
            print(f"  {desc}: {cls._dbt_url(path)}")

    def _test_json_endpoint(self, endpoint_name, expected_keys=None, project_param=None):
        """
        Test a JSON endpoint with common assertions.

        Args:
            endpoint_name: Name of the endpoint (e.g., 'manifest.json')
            expected_keys: Optional list of keys to check in response
            project_param: Optional project parameter to add to URL

        Returns:
            Tuple of (response, data)
        """
        print(f"\n→ Testing {endpoint_name}...")
        url = self._dbt_url(endpoint_name)
        if project_param:
            url += f"?project={project_param}"

        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        self.assertEqual(response.status_code, 200)

        data = response.json()
        if expected_keys:
            for key in expected_keys:
                self.assertIn(key, data)

        return response, data

    def _test_html_endpoint(self, path, project_param=None):
        """Test an HTML endpoint loads successfully"""
        url = self._dbt_url(path)
        if project_param:
            url += f"?project={project_param}"

        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        self.assertEqual(response.status_code, 200)
        self.assertIn("<!DOCTYPE html>", response.text)
        return response


@unittest.skipIf(SKIP_AIRFLOW_TESTS, "Airflow Docker tests disabled. Set RUN_AIRFLOW_TESTS=1 to enable")
class TestAirflowSingleProjectMode(TestAirflowBase):
    """Test single-project mode (backward compatibility)"""

    @classmethod
    def setUpClass(cls):
        def debug_container(cls):
            """Debug hook to check container file system (runs after container is up)"""
            cls._print_separator("CONTAINER FILE SYSTEM CHECK:")
            commands_to_check = [
                ("Plugin file", "docker exec airflow ls -la /opt/airflow/plugins/"),
                ("Plugin content", "docker exec airflow head -20 /opt/airflow/plugins/airflow_dbtdocs_page.py"),
                ("Opendbt version", "docker exec airflow pip show opendbt"),
                ("Target directory", "docker exec airflow ls -la /opt/dbtcore/target/"),
            ]
            cls._run_debug_commands(commands_to_check)
            print(SEPARATOR + "\n")

        cls._setup_airflow(title='Single-Project Mode', post_start_hook=debug_container)
        cls._print_urls(
            ("Home", "../home"),
            ("DBT Docs", "dbt_docs_index.html")
        )

    @classmethod
    def tearDownClass(cls):
        cls._teardown_airflow("Single Project Mode Test")

    def test_single_index_page_accessible(self):
        """Test that DBT docs index page loads in single-project mode"""
        print("\n→ Testing index page accessibility...")
        url = self._dbt_url("dbt_docs_index.html")
        print(f"  URL: {url}")

        response = requests.get(url, timeout=REQUEST_TIMEOUT)
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

    def test_single_project_manifest_json(self):
        """Test manifest.json is accessible"""
        _, data = self._test_json_endpoint("manifest.json", expected_keys=["metadata", "nodes"])
        print(f"  ✓ Manifest contains {len(data.get('nodes', {}))} nodes")

    def test_single_project_catalog_json(self):
        """Test catalog.json is accessible"""
        self._test_json_endpoint("catalog.json", expected_keys=["metadata"])
        print("  ✓ Catalog accessible")

    def test_single_project_catalogl_json(self):
        """Test catalogl.json (enhanced catalog) is accessible"""
        self._test_json_endpoint("catalogl.json")
        print("  ✓ Enhanced catalog accessible")


@unittest.skipIf(SKIP_AIRFLOW_TESTS, "Airflow Docker tests disabled. Set RUN_AIRFLOW_TESTS=1 to enable")
class TestAirflowMultiProjectMode(TestAirflowBase):
    """Test multi-project mode with project switching"""

    @classmethod
    def setUpClass(cls):
        cls._setup_airflow(title='Multi-Project Mode')
        cls._print_urls(
            ("Home", "../home"),
            ("DBT Docs", "dbt_docs_index.html"),
            ("Projects API", "projects"),
            ("DBTCore", f"dbt_docs_index.html?project={PROJECT_CORE}"),
            ("DBTFinance", f"dbt_docs_index.html?project={PROJECT_FINANCE}")
        )

    @classmethod
    def tearDownClass(cls):
        cls._teardown_airflow("Multi-Project Mode Test")

    def test_projects_endpoint(self):
        """Test /dbt/projects endpoint returns both projects from list Variable"""
        _, data = self._test_json_endpoint("projects", expected_keys=["projects", "current"])

        # Check both projects are listed (from list of paths)
        project_names = [p["name"] for p in data["projects"]]
        self.assertEqual(len(project_names), 2, "Should have exactly two projects from list")
        self.assertIn(PROJECT_CORE, project_names)
        self.assertIn(PROJECT_FINANCE, project_names)

        print(f"  ✓ Found {len(project_names)} projects: {', '.join(project_names)}")

        # Check validation info
        for project in data["projects"]:
            self.assertIn("is_valid", project)
            self.assertIn("has_manifest", project)
            self.assertIn("has_custom_index", project)
            print(f"  ✓ {project['name']}: valid={project['is_valid']}, manifest={project['has_manifest']}")

    def test_default_project_loads(self):
        """Test that default project (dbtcore) loads without ?project= param"""
        print("\n→ Testing default project loads...")
        self._test_html_endpoint("dbt_docs_index.html")
        print(f"  ✓ Default project ({PROJECT_CORE}) loads successfully")

    def test_switch_to_dbtfinance(self):
        """Test switching to dbtfinance project via ?project= parameter"""
        print("\n→ Testing project switch to dbtfinance...")
        self._test_html_endpoint("dbt_docs_index.html", project_param=PROJECT_FINANCE)
        print(f"  ✓ {PROJECT_FINANCE} project loads successfully")

    def test_manifest_json_with_project_param(self):
        """Test manifest.json returns correct data for specific project"""
        print("\n→ Testing manifest.json with project parameter...")

        # Get dbtcore manifest
        _, manifest_core = self._test_json_endpoint("manifest.json", project_param=PROJECT_CORE)
        nodes_core = len(manifest_core.get("nodes", {}))

        # Get dbtfinance manifest
        _, manifest_finance = self._test_json_endpoint("manifest.json", project_param=PROJECT_FINANCE)
        nodes_finance = len(manifest_finance.get("nodes", {}))

        print(f"  ✓ {PROJECT_CORE} manifest: {nodes_core} nodes")
        print(f"  ✓ {PROJECT_FINANCE} manifest: {nodes_finance} nodes")

        # They should be different (different number of models)
        self.assertNotEqual(nodes_core, nodes_finance, "Manifests should have different node counts")

    def test_catalog_json_with_project_param(self):
        """Test catalog.json works with project parameter"""
        print("\n→ Testing catalog.json with project parameter...")

        self._test_json_endpoint("catalog.json", project_param=PROJECT_CORE)
        self._test_json_endpoint("catalog.json", project_param=PROJECT_FINANCE)

        print("  ✓ Both project catalogs accessible")

    def test_invalid_project_returns_404(self):
        """Test that invalid project name returns 404"""
        print("\n→ Testing invalid project handling...")
        response = requests.get(self._dbt_url("dbt_docs_index.html") + "?project=nonexistent", timeout=REQUEST_TIMEOUT)
        self.assertEqual(response.status_code, 404)
        print("  ✓ Invalid project returns 404 as expected")

    def test_ui_has_project_selector(self):
        """Test that UI includes project selector dropdown"""
        print("\n→ Testing UI has project selector...")
        response = self._test_html_endpoint("dbt_docs_index.html")

        # Check for project selector elements
        self.assertIn("project-selector", response.text)
        self.assertIn("fa-folder-open", response.text)  # Icon
        self.assertIn('/dbt/projects', response.text)   # API endpoint call

        print("  ✓ Project selector dropdown present in UI")


@unittest.skipIf(SKIP_AIRFLOW_TESTS, "Airflow Docker tests disabled. Set RUN_AIRFLOW_TESTS=1 to enable")
class TestAirflowSingleStringVariable(TestAirflowBase):
    """Test single string path from Variable (not a list)"""

    @classmethod
    def setUpClass(cls):
        def modify_compose_for_single_string(cls):
            """Modify docker-compose.yaml to use single string Variable instead of list"""
            compose_file = cls.compose_dir / "docker-compose.yaml"
            cls.backup_file = cls.compose_dir / "docker-compose.yaml.backup"

            print("\n→ Modifying docker-compose.yaml...")
            # Backup original file
            shutil.copy(compose_file, cls.backup_file)

            # Load and modify YAML
            with open(compose_file, 'r') as f:
                compose_config = yaml.safe_load(f)

            # Change Variable from list to single string
            compose_config['services']['airflow']['command'] = (
                'bash -c "airflow db init && '
                f'airflow variables set opendbt_docs_projects \'\"/opt/{PROJECT_CORE}/target\"\' && '
                'airflow standalone"'
            )

            # Write modified YAML
            with open(compose_file, 'w') as f:
                yaml.dump(compose_config, f, default_flow_style=False, sort_keys=False)

            print("  ✓ Modified Variable to single string")

        cls._setup_airflow(title='Single String Path from Variable', pre_start_hook=modify_compose_for_single_string)
        cls._print_urls(
            ("Home", "../home"),
            ("DBT Docs", "dbt_docs_index.html"),
            ("Projects API", "projects")
        )

    @classmethod
    def tearDownClass(cls):
        cls._teardown_airflow("Single String Variable Test")

        # Restore original docker-compose.yaml from backup
        print("\n→ Restoring docker-compose.yaml...")
        compose_file = cls.compose_dir / "docker-compose.yaml"

        if hasattr(cls, 'backup_file') and cls.backup_file.exists():
            shutil.copy(cls.backup_file, compose_file)
            cls.backup_file.unlink()
            print("  ✓ Restored from backup")

    def test_projects_endpoint_single_string(self):
        """Test /dbt/projects endpoint with single string Variable"""
        _, data = self._test_json_endpoint("projects", expected_keys=["projects", "current"])

        # Should have exactly one project
        project_names = [p["name"] for p in data["projects"]]
        self.assertEqual(len(project_names), 1, "Should have exactly one project")
        self.assertIn(PROJECT_CORE, project_names)

        print(f"  ✓ Found 1 project: {project_names[0]}")

    def test_single_project_loads(self):
        """Test that single project loads correctly"""
        print("\n→ Testing single project loads...")
        self._test_html_endpoint("dbt_docs_index.html")
        print(f"  ✓ Single project ({PROJECT_CORE}) loads successfully")

    def test_manifest_json_works(self):
        """Test manifest.json is accessible"""
        _, manifest = self._test_json_endpoint("manifest.json", expected_keys=["nodes"])
        print(f"  ✓ Manifest accessible with {len(manifest.get('nodes', {}))} nodes")

    def test_catalog_json_works(self):
        """Test catalog.json is accessible"""
        self._test_json_endpoint("catalog.json")
        print("  ✓ Catalog accessible")


@unittest.skip("Manual test")
class TestAirflowManualDev(TestAirflowBase):
    """
    Manual test class for local development.
    Used to deploy code inside docker airflow locally for testing.
    UI login is disabled and made public.
    """

    @classmethod
    def setUpClass(cls):
        cls._setup_airflow("Manual Development Mode")

    @classmethod
    def tearDownClass(cls):
        cls._teardown_airflow("Manual Development Mode")

    def test_start_airflow_local_and_wait(self):
        """
        Starts Airflow locally with current code changes.
        While running, all code changes are reflected in airflow after a short time.
        Useful for manual testing in Airflow UI.
        """
        print(f"\n{'='*60}")
        print(f"Airflow is running at: {self.base_url}")
        print(f"DBT Docs: {self._dbt_url('dbt_docs_index.html')}")
        print(f"{'='*60}\n")
        sleep(99999999)
