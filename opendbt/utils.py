import subprocess

class Utils(object):

    @staticmethod
    def runcommand(command: list, shell=False):
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1,
                              universal_newlines=True, shell=shell) as p:
            for line in p.stdout:
                if line:
                    print(line.strip())

        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, p.args)
