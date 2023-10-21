from mpi4py import MPI
import json
import csv
import numpy as np
import random
from functions import eval_change_op, eval_map_op, eval_reduce_op
import sys
import getopt


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    config_name = ""
    fault_tolerance = True

    # Options
    options = "c:n"

    # Long options
    long_options = ["Config", "NoFault"]

    arguments, values = getopt.getopt(sys.argv[1:], options, long_options)

    for currentArgument, currentValue in arguments:

        if currentArgument in ("-c", "--Config"):
            config_name = currentValue

        elif currentArgument in ("-n", "--NoFault"):
            fault_tolerance = False

    partitions = 0
    operations = []

    input_ds_per_partition = []
    input_ds = []

    counts = []
    displacements = []
    input_dt = None

    if rank == 0:
        config = json.load(open(config_name))
        partitions = config["partitions"]
        operations = config["operations"]

        input_file = open("input/" + config["input"], 'r')
        row_count = 0

        input_ds_per_partition = [[] for i in range(partitions)]

        for row in csv.reader(input_file):
            row_count = row_count + 1
            key = int(row[0]) % partitions
            input_ds_per_partition[key].append([int(row[0]), int(row[1])])

        input_file.close()

        input_ds = [item for sublist in input_ds_per_partition for item in sublist]

        counts = [0]
        displacements = [0]

        for i in range(partitions):
            counts.append(len(input_ds_per_partition[i]))
            displacements.append(displacements[i] + counts[i])

        # print("Counts: ", counts)
        # print("Displacements: ", displacements)

        counts = [x * 2 for x in counts]
        displacements = [x * 2 for x in displacements]

        input_ds = np.array(input_ds)
        input_dt = input_ds.dtype

    counts = comm.bcast(counts, root=0)
    input_dt = comm.bcast(input_dt, root=0)
    num_op = comm.bcast(len(operations), root=0)
    recv_buf = np.empty(counts[rank], dtype=input_dt)

    mpi_dt = MPI.INT.Create_contiguous(2)
    mpi_dt.Commit()

    comm.Scatterv([input_ds, counts, displacements, mpi_dt], [recv_buf, mpi_dt], root=0)

    recv_buf = recv_buf.reshape(-1, 2).tolist()

    # Send all the opertions to do
    operations = comm.bcast(operations, root=0)

    dump = {
        "values": recv_buf
    }

    # Save initial values and operations into a file for fault-tollerance
    if fault_tolerance:
        json.dump(dump, open(f'dumps/rank_{rank}_dump.json', 'w'))

    indexes = [0 for i in range(partitions)]

    if rank == 0:
        MPI.Wtime()

        while sum(indexes) < (num_op - 1) * partitions:
            status = MPI.Status()
            index = comm.recv(status=status)

            source = status.Get_source()
            if index == -1:
                if fault_tolerance:
                    comm.send(indexes[source - 1], source)
                    comm.send(operations, source)
                else:
                    comm.send(0, source)
                    comm.send(operations, source)
                    comm.send(input_ds[int(displacements[source] / 2):int(displacements[source] + counts[source] / 2)],
                              source)
            else:
                indexes[source - 1] = index

    else:
        i = 0

        while i < num_op:
            # if fail restore the lastest operation and redone it
            op = operations[i]

            if op["operator"] == "map":
                recv_buf = eval_map_op(recv_buf, op["function"], op["arg"])
            if op["operator"] == "reduce":
                recv_buf = eval_reduce_op(recv_buf, op["function"])
            if op["operator"] == "changeKey":
                recv_buf = eval_change_op(recv_buf, op["function"], op["arg"])

            fail = random.randint(0, 1000)

            if fail == 0:
                print(f'Rank {rank} failed')
                comm.send(-1, 0)

                # receive the last ok index from coord
                i = comm.recv(source=0)

                # restore the last value
                if fault_tolerance:
                    file = json.load(open(f'dumps/rank_{rank}_dump.json'))
                    recv_buf = file["values"]
                    operations = comm.recv(source=0)

                    print("Received operations: ", operations)

                else:
                    operations = comm.recv(source=0)
                    recv_buf = comm.recv(source=0)

                print(f'Rank {rank} restored')
            else:
                # can move to the next op
                comm.send(i, 0)

                # save the succeful result
                if fault_tolerance:
                    dump["values"] = recv_buf
                    json.dump(dump, open(f'dumps/rank_{rank}_dump.json', 'w'))

                i += 1

    # gather all the final results

    comm.Barrier()

    counts = comm.gather(len(recv_buf) * 2, root=0)

    displacements = [0, 0]

    cursor = 0
    for i in range(1, partitions):
        cursor += counts[i]
        displacements.append(cursor)

    if rank == 0:
        recv_buf = np.empty(sum(counts), input_dt)
    else:
        recv_buf = np.array(recv_buf)

    comm.Barrier()

    comm.Gatherv([recv_buf, mpi_dt], [recv_buf, counts, displacements, mpi_dt], root=0)

    recv_buf = recv_buf.reshape(-1, 2).tolist()

    if rank == 0:
        print("Result: ", recv_buf)
        print(f"Elapsed time {MPI.Wtime()}")


if __name__ == "__main__":
    main()
