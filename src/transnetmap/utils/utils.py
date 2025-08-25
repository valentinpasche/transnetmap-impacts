# -*- coding: utf-8 -*-

def convert_to_pg_array(path) -> str:
    """
    Converts a list of integers to a PostgreSQL array string.

    Parameters
    ----------
    path : list of int or numpy array
        A list or numpy array containing integer values representing a path.

    Returns
    -------
    str
        A string representation of the path in PostgreSQL array format, e.g., '{1,2,3}'.

    Examples
    --------
    >>> convert_to_pg_array([1, 2, 3])
    '{1,2,3}'

    >>> import numpy as np
    >>> convert_to_pg_array(np.array([4, 5, 6]))
    '{4,5,6}'

    Notes
    -----
    - This function is typically used to format data before writing it to a PostgreSQL database.
    - Paths must be lists or numpy arrays containing integers.
    """
    return '{' + ','.join(map(str, path)) + '}'


def spinner(message, stop_event) -> None:
    """
    Displays a rotary loading indicator in the console.

    This function creates a spinner animation in the console to indicate progress.
    The animation stops when the `stop_event` is set.

    Parameters
    ----------
    message : str
        A message to display alongside the spinner.
    stop_event : threading.Event
        An event used to signal when the spinner should stop. The spinner runs
        indefinitely until the event is set.

    Returns
    -------
    None

    Examples
    --------
    >>> import threading
    >>> import time
    >>> stop_event = threading.Event()
    >>> spinner_thread = threading.Thread(target=spinner, args=("Processing", stop_event))
    >>> spinner_thread.start()
    >>> time.sleep(5)  # Simulate some processing time
    >>> stop_event.set()
    >>> spinner_thread.join()

    Notes
    -----
    - The spinner runs in a separate thread, allowing other tasks to execute in parallel.
    - The spinner clears its line in the console when it stops.
    """
    import itertools
    import time
    
    for frame in itertools.cycle(['|', '/', '-', '\\']):
        if stop_event.is_set():
            break
        print(f'\r{message} {frame}', end='', flush=True)
        time.sleep(0.5)
    print('\r' + ' ' * (len(message) + 2), end='\r', flush=True)  # Clear line after spinner stops


def to_engineering_notation(number) -> str:
    """
    Converts a number to engineering notation (multiples of 10^3).
    Returns a string with the value and the corresponding suffix (e.g., '3.2k', '1.5M').
    """
    if number == 0:
        return "0"
    
    # Define suffixes for engineering notation
    suffixes = ["", "k", "M", "G", "T", "P"]
    magnitude = max(0, min(len(suffixes) - 1, int((len(str(int(abs(number)))) - 1) // 3)))
    scaled_number = number / (10 ** (3 * magnitude))
    return f"{scaled_number:.3g}{suffixes[magnitude]}"


def cap_first(s) -> str:
    return s[:1].upper() + s[1:]


def remove_duplicates_preserve_order(lst) -> list:
    """
    Removes duplicate elements from a list while preserving the original order.

    Useful for cases where unique values are required, but their order of appearance matters
    (e.g. for controlling the display order of map layers or popup fields).

    Parameters
    ----------
    lst : list
        A list of elements (e.g., strings, numbers) that may contain duplicates.

    Returns
    -------
    list
        A new list with duplicates removed, maintaining the original order of the first occurrence.

    Examples
    --------
    >>> remove_duplicates_preserve_order(["a", "b", "a", "c", "b"])
    ['a', 'b', 'c']
    """
    seen = set()
    return [x for x in lst if not (x in seen or seen.add(x))]


def wrap_text_at_space(text: str, max_line_length: int) -> str:
    """
    Inserts <br> tags into `text` at the nearest space after `max_line_length`.
    Prevents breaking words and supports long paragraphs.

    Parameters
    ----------
    text : str
        The text to wrap.
    max_line_length : int
        Target max length per line.

    Returns
    -------
    str
        HTML-compatible text with <br> tags inserted.
    """
    words = text.split()
    wrapped_lines = []
    current_line = ""

    for word in words:
        if len(current_line + " " + word) <= max_line_length:
            current_line += " " + word if current_line else word
        else:
            wrapped_lines.append(current_line)
            current_line = word

    if current_line:
        wrapped_lines.append(current_line)

    return "<br>".join(wrapped_lines)


# ===========================
if __name__ == "__main__":
    
    to_engineering_notation(0)
    to_engineering_notation(500000000)
    to_engineering_notation(-50000)
    
    