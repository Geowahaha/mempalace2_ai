import sys

def main():
    path = "d:/dexter_pro_v3_fixed/dexter_pro_v3_fixed/execution/mt5_executor.py"
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    target = """                    alt = {
                        "action": req["action"],
                        "symbol": req["symbol"],
                        "position": req["position"],
                        "volume": req["volume"],
                        "type": req["type"],
                        "price": req["price"],
                        "deviation": req["deviation"],
                        "type_filling": tf,
                    }
                    res2, rc2, bcomment2 = _parse_res(_send_req(alt))"""

    replacement = """                    alt = {
                        "action": req["action"],
                        "symbol": req["symbol"],
                        "position": req["position"],
                        "volume": req["volume"],
                        "type": req["type"],
                        "price": req["price"],
                        "deviation": req["deviation"],
                        "type_filling": tf,
                    }
                    if "magic" in req:
                        alt["magic"] = req["magic"]
                    if "type_time" in req:
                        alt["type_time"] = req["type_time"]
                    res2, rc2, bcomment2 = _parse_res(_send_req(alt))"""

    if target in content:
        content = content.replace(target, replacement)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        print("Replaced successfully")
    elif target.replace('\r\n', '\n') in content.replace('\r\n', '\n'):
        # Normalise line endings
        content = content.replace('\r\n', '\n')
        target = target.replace('\r\n', '\n')
        replacement = replacement.replace('\r\n', '\n')
        content = content.replace(target, replacement)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        print("Replaced successfully (with normalized line endings)")
    else:
        print("Target not found. Looking at snippet around target:")
        idx = content.find("alt = {")
        if idx != -1:
            print(content[idx:idx+500])

if __name__ == "__main__":
    main()
