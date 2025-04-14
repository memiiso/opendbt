import os
import unittest
from pathlib import Path
from time import sleep

from testcontainers.compose import DockerCompose


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
