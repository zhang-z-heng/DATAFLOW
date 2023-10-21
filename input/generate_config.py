import json
import random
import sys

functions = ["ADD", "SUB", "MUL", "DIV"]
operators = ["map", "changeKey", "reduce"]
operations = []

bound = 1000
max_val = 10

if len(sys.argv) == 3:
    bound = int(sys.argv[1])
    max_val = int(sys.argv[2])

for i in range(bound):
    function = random.choice(functions)
    operator = random.choice(operators)
    if operator == "reduce" and function not in ["ADD", "MUL"]:
        function = random.choice(["ADD", "MUL"])
    arg = random.randint(0, max_val)

    if function == "DIV" and arg == 0:
        arg = 1

    operation = {
        "operator": operator,
        "function": function
    }

    if function != "reduce":
        operation["arg"] = arg

    operations.append(operation)

config = {
    "partitions": 2,
    "input": "input.csv",
    "operations": operations
}

with open("config.json", "w") as f:
    json.dump(config, f, indent=4)