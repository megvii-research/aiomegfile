import typing as T


def parse_quantity(quantity: T.Union[str, int]) -> int:
    """
    Parse kubernetes canonical form quantity like 200Mi to a int number.
    Supported SI suffixes:
    base1024: Ki | Mi | Gi | Ti | Pi | Ei
    base1000: "" | k | M | G | T | P | E

    (International System of units; See: http://physics.nist.gov/cuu/Units/binary.html)

    Input:
    quantity: string. kubernetes canonical form quantity

    Returns:
    Int

    Raises:
    ValueError on invalid or unknown input
    """
    if isinstance(quantity, int):
        return quantity

    exponents = {"K": 1, "k": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}

    number = quantity
    suffix = None
    if len(quantity) >= 2 and quantity[-1] == "i":
        if quantity[-2] in exponents:
            number = quantity[:-2]
            suffix = quantity[-2:]
    elif len(quantity) >= 1 and quantity[-1] in exponents:
        number = quantity[:-1]
        suffix = quantity[-1:]

    try:
        number = int(number)
    except ValueError:
        raise ValueError("Invalid number format: {}".format(number))

    if suffix is None:
        return number

    if suffix.endswith("i"):
        base = 1024
    else:
        base = 1000

    # handle SI inconsistency
    if suffix == "ki":
        raise ValueError("{} has unknown suffix".format(quantity))

    exponent = int(exponents[suffix[0]])
    return number * (base**exponent)  # pytype: disable=bad-return-type


def parse_boolean(value: T.Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.lower() in ("true", "yes", "1")


def fullname(o):
    klass = o.__class__
    module = klass.__module__
    if module == "builtins":
        return klass.__qualname__  # avoid outputs like 'builtins.str'
    return module + "." + klass.__qualname__


def get_human_size(num_bytes: int) -> str:
    """Return a human-readable size string.

    :param num_bytes: Size in bytes.
    :return: Human-readable size.
    """
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)}{unit}"
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{int(num_bytes)}B"
