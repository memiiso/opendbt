import argparse
import os
from pathlib import Path

from opendbt import OpenDbtCli


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--project-dir', default=None)
    ns, args = p.parse_known_args()
    if ns.project_dir is None:
        ns.project_dir = os.environ.get('DBT_PROJECT_DIR', Path.cwd())

    OpenDbtCli(project_dir=Path(ns.project_dir)).invoke(args=args)


if __name__ == "__main__":
    main()
