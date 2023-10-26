from enum import Enum


# ANSI color codes for printing to the terminal
class Colors(Enum):
    RESET = 0
    BLACK = 30
    RED = 31
    GREEN = 32
    YELLOW = 33
    BLUE = 34
    MAGENTA = 35
    CYAN = 36
    WHITE = 37
    DEFAULT = 39
    BRIGHT_BLACK = 90
    BRIGHT_RED = 91
    BRIGHT_GREEN = 92
    BRIGHT_YELLOW = 93
    BRIGHT_BLUE = 94
    BRIGHT_MAGENTA = 95
    BRIGHT_CYAN = 96
    BRIGHT_WHITE = 97


def printc(message: str, color: Colors, end_color: Colors = Colors.RESET):
    """Print a message to the terminal in color.

    After printing reset the color by default.

    Parameters
    ----------
    message :
        The message to print.
    color :
        The color to print the message in.
    end :
        The color future messages should be printed in.
    """
    print(f"\033[{color.value}m{message}\033[{end_color.value}m")
