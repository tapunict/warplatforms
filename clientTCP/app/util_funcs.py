import os 


def toBool(str):
    return False if str.lower() == "false" else True

def strOrNone(str):
    return None if str.lower() == "none" else str

def setEnvVar(name, value):
    try:
        os.environ[name] = value
        return True
    except Exception as e:
        print(f"(setEnvVar): {e}")
        return False