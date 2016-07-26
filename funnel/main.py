import os
import sys
import argparse
import logging
import yaml

import cwltool.main

from command import CommandPipeline
from gce import GCEPipeline
from tes import TESPipeline

def main(args):
    parser = arg_parser()
    newargs = parser.parse_args(args)
    
    if newargs.gce is not None:
        with open(newargs.gce) as handle:
            config = yaml.load(handle.read())
            pipeline = GCEPipeline(config)
    elif newargs.tes is not None:
        with open(newargs.tes) as handle:
            config = yaml.load(handle.read())
            pipeline = TESPipeline(config)
    else:
        config = {}
        pipeline = CommandPipeline(config)

    cwltool.main.main(args=newargs, executor=pipeline.executor, makeTool=pipeline.make_tool)

def arg_parser():    # type: () -> argparse.ArgumentParser
    parser = argparse.ArgumentParser(description='Arvados executor for Common Workflow Language')

    parser.add_argument("--basedir", type=str,
                        help="Base directory used to resolve relative references in the input, default to directory of input object file or current directory (if inputs piped/provided on command line).")
    parser.add_argument("--outdir", type=str, default=os.path.abspath('.'),
                        help="Output directory, default current directory")
    parser.add_argument("--conformance-test", action="store_true", default=False)

    parser.add_argument("--eval-timeout",
                        help="Time to wait for a Javascript expression to evaluate before giving an error, default 20s.",
                        type=float,
                        default=20)
    parser.add_argument("--version", action="store_true", help="Print version and exit")
        
    parser.add_argument("--gce", default=None, help="Google Compute Config")
        
    parser.add_argument("--tes", default=None, help="Task Execution System Config")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument("--verbose", action="store_true", help="Default logging")
    exgroup.add_argument("--quiet", action="store_true", help="Only print warnings and errors.")
    exgroup.add_argument("--debug", action="store_true", help="Print even more logging")

    parser.add_argument("--tool-help", action="store_true", help="Print command line help for tool")

    parser.add_argument("--project-uuid", type=str, help="Project that will own the workflow jobs, if not provided, will go to home project.")
    parser.add_argument("--ignore-docker-for-reuse", action="store_true",
                        help="Ignore Docker image version when deciding whether to reuse past jobs.",
                        default=False)

    parser.add_argument("workflow", type=str, nargs="?", default=None, help="The workflow to execute")
    parser.add_argument("job_order", nargs=argparse.REMAINDER, help="The input object to the workflow.")

    return parser

if __name__ == '__main__':    
    sys.exit(main(sys.argv[1:]))

