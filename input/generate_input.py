import random
import sys

bound = 1000
max_val = 1000

if len(sys.argv) == 3:
    bound = int(sys.argv[1])
    max_val = int(sys.argv[2])

lines = []
for i in range(bound):
    a = random.randint(1, max_val)
    b = random.randint(1, max_val)
    lines.append(f"{a},{b}")

with open("input.csv", "w") as f:
    f.write("\n".join(lines))