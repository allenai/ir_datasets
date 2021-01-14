import sys
import argparse
import ir_datasets
from ir_datasets.commands.export import DEFAULT_EXPORTERS


_logger = ir_datasets.log.easy()


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets list', description='Lists available datasets.')
    parser.set_defaults(out=sys.stdout)
    args = parser.parse_args(args)

    for dataset in sorted(ir_datasets.registry):
        args.out.write(f'{dataset}\n')


if __name__ == '__main__':
    main(sys.argv[1:])
