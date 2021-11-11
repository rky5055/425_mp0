if __name__ == "__main__":
    event = { "path": "a", "c": "d" }
    match event:
        case { "path": "a", **v }:
            print(event)
            print(v)
        case _:
            pass