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

    @staticmethod
    def merge_dicts(dict1: dict, dict2: dict) -> dict:
        """
        Recursively merges dict2 into dict1, when both values exists dict1 value retained
        Returns:
            A new dictionary representing the merged result.
        """
        merged = dict1.copy()

        for key, value in dict2.items():
            if key in merged:
                # Check if both values are dictionary-like (mappings)
                if isinstance(merged[key], dict) and isinstance(value, dict):
                    # Both are dicts, recurse
                    merged[key] = Utils.merge_dicts(merged[key], value)
                else:
                    # Add dict2 value if dict2 value is not exists
                    if merged.get(key, None):
                        continue
                    else:
                        merged[key] = value
            else:
                # Key not in dict1, simply add it
                merged[key] = value

        return merged

    @staticmethod
    def lowercase_dict_keys(input_dict: dict, recursive: bool = False):
        if not isinstance(input_dict, dict):
            return input_dict

        new_dict = {}
        for key, value in input_dict.items():
            if isinstance(value, dict) and recursive:
                value = Utils.lowercase_dict_keys(value)
            if isinstance(key, str):
                key = key.lower()

            new_dict[key] = value

        return new_dict
