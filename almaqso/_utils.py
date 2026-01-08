import numpy as np


def parse_selection_string(selection_string: str) -> list[int]:
    """
    Parses a CASA-style selection string and returns a sorted list of integers.

    Args:
        The selection string (e.g., "0~11;20,24").

    Returns:
        A sorted list of unique integers.
    """
    if not selection_string or not isinstance(selection_string, str):
        return []

    selected_indices = set()

    # First, replace all semicolons with commas to standardize the delimiter.
    standardized_str = selection_string.replace(";", ",")
    # Then, split the string by the single, standardized delimiter.
    items = standardized_str.split(",")

    for item in items:
        item = item.strip()
        if not item:
            continue

        # The rest of the logic is exactly the same
        if "<" in item:
            try:
                val = int(item.replace("<", ""))
                selected_indices.update(range(val))
            except ValueError:
                raise ValueError(f"Invalid specification '{item}' was ignored.")
        elif "~" in item:
            try:
                start_str, end_str = item.split("~")
                start = int(start_str)
                end = int(end_str)
                selected_indices.update(range(start, end + 1))
            except ValueError:
                raise ValueError(f"Invalid range specification '{item}' was ignored.")
        else:
            try:
                index = int(item)
                selected_indices.add(index)
            except ValueError:
                raise ValueError(f"Invalid specification '{item}' was ignored.")

    return sorted(set(selected_indices))


def parse_selection(selection_input: list[int] | int | str) -> list[int]:
    """
    Parses a selection input that can be either a single integer or a list of integers.

    Args:
        selection_input: The selection input.

    Returns:
        list[int]: A list of integers.
    """
    if isinstance(selection_input, int):
        return [selection_input]
    elif isinstance(selection_input, list):
        return sorted(set(selection_input))
    elif isinstance(selection_input, str):
        return parse_selection_string(selection_input)
    else:
        raise ValueError("Invalid selection input type.")


def in_source_list(source_name: str, source_list: list[str]) -> bool:
    """
    Checks if a source name is in the provided source list, case-insensitively.

    Args:
        source_name: The source name to check.
        source_list: The list of source names.

    Returns:
        bool: True if the source name is in the list, False otherwise.
    """
    # if source_list is empty, return True (all sources are included)
    if not source_list:
        return True

    # Check case-insensitively
    return source_name.lower() in (s.lower() for s in source_list)


def parse_source_list(source_input: str | list[str]) -> list[str]:
    """
    Parses a source input that can be either a comma-separated string or a list of strings.

    Args:
        source_input: The source input.

    Returns:
        list[str]: A list of source names.
    """
    ret: list[str] = list(np.unique(source_input).astype(str))
    # Remove "" entries
    ret = [s for s in ret if s != ""]
    return ret
