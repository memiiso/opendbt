import argparse

from opendbt import OpenDbtCli


def main():
    p = argparse.ArgumentParser()
    _, args = p.parse_known_args()
    OpenDbtCli.run(args=args)


if __name__ == "__main__":
    main()
