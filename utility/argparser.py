from optparse import OptionParser


def recstep_argument_parser():
    parser = OptionParser()

    parser.add_option(
        "--config",
        type="string",
        dest="config",
        help="the path of the file specifiying configurations",
    )
    parser.add_option(
        "--program", type="string", dest="program", help="datalog program path"
    )
    parser.add_option(
        "--input", type="string", dest="input", help="the input dir of datalog program"
    )
    parser.add_option(
        "--jobs",
        type="int",
        dest="jobs",
        help="the number of workers/threads to use",
    )
    parser.add_option(
        "--mode",
        type="string",
        dest="mode",
        default="network",
        help="network or interactive",
    )

    (options, _) = parser.parse_args()

    return options


recstep_argument_parser()
