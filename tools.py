
def FormatString(string):
    valid_characteres = "abcdefghijklmnopqrstuvwxyzABCDEF GHIJKLMNOPQRSTUVWXYZ,¡:?@*_-!1234567890"
    new_string = ''.join(c for c in string if c in valid_characteres)

    if new_string == "nan":
        return ""
    else:
        return new_string

def FormatInt(string):
    valid_characteres = "-1234567890"
    new_int = ''.join(c for c in string if c in valid_characteres)

    if new_int == "nan" or new_int == "":
        return 0
    else:
        return int(new_int)

def FormatIntUnsigned(string):
    valid_characteres = "1234567890"
    new_int = ''.join(c for c in string if c in valid_characteres)

    if new_int == "nan" or new_int == "":
        return 0
    else:
        return int(new_int)
