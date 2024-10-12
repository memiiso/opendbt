import subprocess
import unittest
from pathlib import Path
from time import sleep

from testcontainers.compose import DockerCompose
from testcontainers.core.waiting_utils import wait_for_logs


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
        cls._compose = DockerCompose(cls.resources_dir.joinpath('airflow').as_posix(),
                                     compose_file_name="docker-compose.yaml",
                                     build=True
                                     )
        cls._compose.stop()
        cls._compose.start()
        # cls._compose.wait_for(url="http://localhost:8080/health")
        wait_for_logs(cls._compose, 'Added Permission menu access on Configurations')
        wait_for_logs(cls._compose, 'Added user admin')

    @classmethod
    def tearDownClass(cls):
        print("Running tearDownClass")
        if cls._compose:
            cls._compose.stop()

    def __exit__(self, exc_type, exc_val, traceback):
        if self._compose:
            self._compose.stop()

    def _get_service_port(self, service, port):
        port_cmd = self._compose.docker_compose_command() + ["port", service, str(port)]
        output = subprocess.check_output(port_cmd, cwd=self._compose.filepath).decode("utf-8")
        result = str(output).rstrip().split(":")
        if len(result) != 2:
            raise Exception(f"Unexpected service info {output}. expecting `host:1234`")
        return result[-1]

    def test_start_airflow_local_and_wait(self):
        """
        used to deploy the code inside docker airflow locally. UI login is disabled and made public!
        useful to run local airflow with the new code changes and check the changes in airflow ui
        while its running all the code changes are reflected in airflow after short time.
        :return:
        """
        print(f"http://localhost:{self._get_service_port('airflow', 8080)}/home")
        print(f"http://localhost:{self._get_service_port('airflow', 8080)}/dbtdocsview")

        sleep(99999999)
