import argparse
from pathlib import Path

from opendbt import OpenDbtCli, default_project_dir, default_profiles_dir


def main():
    parser = argparse.ArgumentParser(description="OpenDBT CLI")
    parser.add_argument(
        "--project-dir",
        default=None,
        help="Path to the dbt project directory. Defaults to the DBT_PROJECT_DIR environment variable or the current working directory.",
    )
    parser.add_argument(
        "--profiles-dir",
        default=None,
        help="Path to the dbt profiles directory. Defaults to the DBT_PROFILES_DIR environment variable.",
    )
    ns, args = parser.parse_known_args()
    project_dir = Path(ns.project_dir) if ns.project_dir else default_project_dir()
    profiles_dir = Path(ns.profiles_dir) if ns.profiles_dir else default_profiles_dir()

    OpenDbtCli(project_dir=project_dir, profiles_dir=profiles_dir).invoke(args=args)


if __name__ == "__main__":
    main()
