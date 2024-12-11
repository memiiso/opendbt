import importlib
import subprocess


class Utils:

    @staticmethod
    def runcommand(command: list, shell=False):
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1,
                              universal_newlines=True, shell=shell) as p:
            for line in p.stdout:
                if line:
                    print(line.strip())

        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, p.args)

    @staticmethod
    def import_module_attribute_by_name(module_name: str):
        if "." not in module_name:
            raise ValueError(f"Unexpected module name: `{module_name}` ,"
                             f"Expecting something like:`my.sample.library.MyClass` or `my.sample.library.my_method`")

        __module, __attribute = module_name.rsplit('.', 1)
        try:
            _adapter_module = importlib.import_module(__module)
            _adapter_attribute = getattr(_adapter_module, __attribute)
            return _adapter_attribute
        except ModuleNotFoundError as mnfe:
            raise Exception(f"Provided module not found, provided: {module_name}") from mnfe
