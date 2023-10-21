from functools import reduce

def eval_map_op(recv_buf, op, arg) -> list:
    if op == "ADD":
        return list(map(lambda x: [x[0], x[1] + arg], recv_buf))
    elif op == "MUL":
        return list(map(lambda x: [x[0], x[1] * arg], recv_buf))
    elif op == "DIV":
        return list(map(lambda x: [x[0], int(x[1] / arg)], recv_buf))
    elif op == "SUB":
        return list(map(lambda x: [x[0], x[1] - arg], recv_buf))

def eval_reduce_op(recv_buf, op) -> list:
    # Group elements by key
    grouped = {}
    for key, value in recv_buf:
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(value)

    # Apply operator to values of elements with the same key
    result = []
    if op == "ADD":
        for key, values in grouped.items():
            result.append([key, sum(values)])
    elif op == "MUL":
        for key, values in grouped.items():
            result.append([key, reduce(lambda x, y: x * y, values)])
    """
    elif op == "DIV": # Forse da togliere (non è commutativa)
        for key, values in grouped.items():
            result.append([key, int(reduce(lambda x, y: x / y, values))])
    elif op == "SUB": # Forse da togliere (non è commutativa)
        for key, values in grouped.items():
            result.append([key, reduce(lambda x, y: x - y, values)])
    """
    return result

def eval_change_op(recv_buf, op, arg) -> list:
    if op == "ADD":
        return list(map(lambda x: [x[0] + arg, x[1]], recv_buf))
    elif op == "MUL":
        return list(map(lambda x: [x[0] * arg, x[1]], recv_buf))
    elif op == "DIV":
        return list(map(lambda x: [int(x[0] / arg), x[1]], recv_buf))
    elif op == "SUB":
        return list(map(lambda x: [x[0] - arg, x[1]], recv_buf))