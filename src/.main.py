#!/usr/bin/env python3

import json
import os
import sys
import getopt


def main():
    hosts = ""
    config_name = ""
    no_fault = ""
    # Options
    options = "h:c:n"

    # Long options
    long_options = ["Hosts", "Config", "NoFault"]

    arguments, values = getopt.getopt(sys.argv[1:], options, long_options)

    for currentArgument, currentValue in arguments:
        if currentArgument in ("-h", "--Hosts"):
            hosts = f"-H {currentValue}"

        elif currentArgument in ("-c", "--Config"):
            config_name = currentValue

        elif currentArgument in ("-n", "--NoFault"):
            no_fault = currentArgument
        else:
            print("Usage: python main.py -c|--Config <config_file> [-h|--Hosts host(,host)*] [-n|--NoFault]")
            exit()

    if config_name == "":
        print("Usage: python main.py -c|--Config <config_file> [-h|--Hosts host(,host)*] [-n|--NoFault]")
        exit()

    config = json.load(open(config_name))

    if not os.path.exists("input/" + config["input"]):
        print("Input file does not exist")

        exit()

    if not isinstance(config["partitions"], int) or config["partitions"] < 1:
        print("Partitions must be a positive integer")
        exit()

    if not isinstance(config["operations"], list) or len(config["operations"]) < 1:
        print("Operations must be a list with at least one element")
        exit()

    for op in config["operations"]:
        if not "operator" in op.keys():
            print("Operator missing")
            exit()
        elif op["operator"] not in ["map", "reduce", "changeKey"]:
            print("Invalid operator")
            exit()
        if not "function" in op.keys():
            print("Function missing")
            exit()
        elif op["function"] not in ["ADD", "SUB", "MUL", "DIV"]:
            print("Invalid function")
            exit()
        if op["operator"] != "reduce" and not "arg" in op.keys():
            print("Argument missing")
            exit()
        elif op["operator"] != "reduce" and not isinstance(op["arg"], int):
            print("Argument must be an integer")
            exit()
        if op["function"] == "DIV" and op["arg"] == 0:
            print("Division by zero!")
            exit()

    # Start MPI
    print("Starting MPI env")

    os.system(
        f'mpirun -oversubscribe -n {config["partitions"] + 1} {hosts} python dataflow.py -c {config_name} {no_fault}')


if __name__ == "__main__":
    main()
