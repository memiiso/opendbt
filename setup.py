import os
import pathlib

from setuptools import setup, find_packages

setup_py_dir = pathlib.Path(__file__).parent
os.chdir(setup_py_dir)

setup(
    name='opendbt',
    entry_points={
        'console_scripts': [
            'opendbt = opendbt:main',
        ],
    },
    version='0.5.0',
    packages=find_packages(),
    author="Memiiso Organization",
    description='Python opendbt',
    long_description=pathlib.Path(__file__).parent.joinpath("README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    url='https://github.com/memiiso/opendbt',
    download_url='https://github.com/memiiso/opendbt/archive/master.zip',
    include_package_data=True,
    license="Apache License 2.0",
    test_suite='tests',
    install_requires=["dbt-duckdb>=1.6", "sqlfluff", "sqlfluff-templater-dbt"],
    extras_require={
        "airflow": ["apache-airflow"],
        "test": ["testcontainers>=3.7,<4.9"],
    },
    python_requires='>=3.8'
)
