import argparse
import os
from pathlib import Path

from opendbt import OpenDbtCli


def main():
    parser = argparse.ArgumentParser(description="OpenDBT CLI")
    parser.add_argument(
        "--project-dir",
        default=os.environ.get("DBT_PROJECT_DIR"),
        help="Path to the dbt project directory. Defaults to the DBT_PROJECT_DIR environment variable or the current working directory.",
    )
    parser.add_argument(
        "--profiles-dir",
        default=os.environ.get("DBT_PROFILES_DIR"),
        help="Path to the dbt profiles directory. Defaults to the DBT_PROFILES_DIR environment variable.",
    )
    ns, args = parser.parse_known_args()
    project_dir = Path(ns.project_dir) if ns.project_dir else Path.cwd()
    profiles_dir = Path(ns.profiles_dir) if ns.profiles_dir else None

    OpenDbtCli(project_dir=project_dir, profiles_dir=profiles_dir).invoke(args=args)


if __name__ == "__main__":
    main()
