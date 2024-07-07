import os
import platform

from dbt import version


def print_info():
    _str = f"name:{os.name}, system:{platform.system()} release:{platform.release()}"
    _str += f"\npython version:{platform.python_version()}, dbt:{version.__version__}"
    print(_str)


def model(dbt, session):
    dbt.config(materialized="executepython")
    print("==================================================")
    print("========IM LOCALLY EXECUTED PYTHON MODEL==========")
    print("==================================================")
    print_info()
    print("==================================================")
    print("===============MAKE DBT GREAT AGAIN===============")
    print("==================================================")
    return None
