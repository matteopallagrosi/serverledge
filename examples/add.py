def handler(params, context):
    print(f"Invoked add with input: {params}")
    return int(params["n"]) + int(params["m"])