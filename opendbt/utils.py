import os
import subprocess

from opendbt.logger import OpenDbtLogger


class Utils(object):

    @staticmethod
    def runcommand(command: list, shell=False):
        logger = OpenDbtLogger()

        logger.log.info("Working dir is %s" % os.getcwd())
        logger.log.info("Running command (shell=%s) `%s`" % (shell, " ".join(command)))
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1,
                              universal_newlines=True, shell=shell) as p:
            for line in p.stdout:
                if line:
                    print(line.strip())

        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, p.args)
