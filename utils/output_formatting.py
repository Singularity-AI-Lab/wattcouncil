"""
Terminal Output Formatting Utilities

Provides colored output, icons, and formatting helpers for beautiful terminal output.

Features:
- 🎨 ANSI color codes (green, orange, red, blue, etc.)
- ✨ Unicode icons for visual clarity
- 📐 Formatted headers, sections, and messages
- ⚡ Reusable across all test scripts and pipelines
"""

# Logging utilities
import logging

# File output helpers
from pathlib import Path
from datetime import datetime


# ANSI Color Codes
class Colors:
    """ANSI color codes for terminal output."""
    GREEN = '\033[92m'
    ORANGE = '\033[93m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    RESET = '\033[0m'
    DIM = '\033[2m'
    UNDERLINE = '\033[4m'

# Unicode Icons
class Icons:
    """Unicode icons for visual clarity."""
    # Status
    SUCCESS = '✅'
    WARNING = '⚠️ '
    ERROR = '❌'
    INFO = 'ℹ️ '
    
    # Actions
    ROCKET = '🚀'
    GEAR = '⚙️ '
    SAVE = '💾'
    LOAD = '📂'
    
    # Roles
    AUDIT = '🔍'
    CEO = '👨‍💼'
    GENERATOR = '🤖'
    
    # Content
    FAMILY = '👨‍👩‍👧‍👦'
    WEATHER = '🌤️ '
    GLOBE = '🌍'
    WORK = '💼'
    ENERGY = '⚡'
    HOME = '🏠'
    
    # Stats
    STATS = '📊'
    CHART = '📈'
    CLOCK = '⏱️ '
    CHECK = '✓'
    CROSS = '✗'
    ARROW = '→'
    DOT = '•'

class ColoredFormatter(logging.Formatter):
    """
    Custom logging formatter with colors.
    
    Usage:
        handler = logging.StreamHandler()
        handler.setFormatter(ColoredFormatter('%(levelname)s: %(message)s'))
        logging.basicConfig(level=logging.WARNING, handlers=[handler])
    """
    
    COLORS = {
        'DEBUG': Colors.DIM,
        'INFO': Colors.RESET,
        'WARNING': Colors.ORANGE,
        'ERROR': Colors.RED,
        'CRITICAL': Colors.BOLD + Colors.RED
    }
    
    def format(self, record):
        color = self.COLORS.get(record.levelname, Colors.RESET)
        record.levelname = f"{color}{record.levelname}{Colors.RESET}"
        return super().format(record)


def save_test_output(stage_name: str, output: str, output_dir: str = "tests/outputs") -> Path:
    """
    Save test output to timestamped file.
    
    Args:
        stage_name: Stage name (e.g., "stage1", "stage2")
        output: Output content to save
        output_dir: Output directory (default: "tests/outputs")
    
    Returns:
        Path to saved file
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_path / f"{stage_name}_{timestamp}.json"
    
    with open(output_file, "w") as f:
        f.write(output)
    
    return output_file

def print_test_summary(stage_name: str, attempt: int, data_summary: dict, output_file: Path):
    """
    Print final test summary after acceptance.
    
    Args:
        stage_name: Stage name
        attempt: Number of attempts taken
        data_summary: Dict with summary info (e.g., {"families": 5, "types": [...]})
        output_file: Path to saved output file
    """
    print_section("Test Results", Icons.STATS)
    print_success(f"PASSED in {attempt} attempt(s)")
    
    for key, value in data_summary.items():
        if isinstance(value, list) and len(value) > 3:
            # Truncate long lists
            display = f"{', '.join(map(str, value[:3]))}..."
        else:
            display = str(value)
        print_info(f"{key.replace('_', ' ').title()}: {display}")
    
    print_info(f"Output saved: {colored(str(output_file), Colors.CYAN)}")
    print_header(f"{Icons.SUCCESS} TEST COMPLETED SUCCESSFULLY")

def colored(text: str, color: str) -> str:
    """
    Apply color to text.
    
    Args:
        text: Text to color
        color: Color code from Colors class
    
    Returns:
        Colored text with reset code
    """
    return f"{color}{text}{Colors.RESET}"

def print_header(title: str, width: int = 70):
    """
    Print a prominent header.
    
    Args:
        title: Header title
        width: Width of header (default: 70)
    """
    print(f"\n{colored('═' * width, Colors.CYAN)}")
    print(f"{colored(title.center(width), Colors.BOLD + Colors.CYAN)}")
    print(f"{colored('═' * width, Colors.CYAN)}\n")

def print_section(title: str, icon: str = '', width: int = 70):
    """
    Print a section header.
    
    Args:
        title: Section title
        icon: Optional icon prefix
        width: Width of separator (default: 70)
    """
    full_title = f"{icon} {title}" if icon else title
    print(f"\n{colored(full_title, Colors.BOLD + Colors.BLUE)}")
    print(f"{colored('─' * width, Colors.BLUE)}")

def print_success(message: str, indent: int = 0):
    """
    Print success message in green.
    
    Args:
        message: Message to print
        indent: Indentation level (default: 0)
    """
    prefix = "  " * indent
    print(f"{prefix}{colored(f'{Icons.SUCCESS} {message}', Colors.GREEN)}")

def print_warning(message: str, indent: int = 0):
    """
    Print warning message in orange.
    
    Args:
        message: Message to print
        indent: Indentation level (default: 0)
    """
    prefix = "  " * indent
    print(f"{prefix}{colored(f'{Icons.WARNING} {message}', Colors.ORANGE)}")

def print_error(message: str, indent: int = 0):
    """
    Print error message in red.
    
    Args:
        message: Message to print
        indent: Indentation level (default: 0)
    """
    prefix = "  " * indent
    print(f"{prefix}{colored(f'{Icons.ERROR} {message}', Colors.RED)}")

def print_info(message: str, indent: int = 0, dim: bool = True):
    """
    Print info message.
    
    Args:
        message: Message to print
        indent: Indentation level (default: 0)
        dim: Use dim color (default: True)
    """
    prefix = "  " * indent
    if dim:
        print(f"{prefix}{colored(message, Colors.DIM)}")
    else:
        print(f"{prefix}{message}")

def print_progress(message: str, icon: str = Icons.CLOCK):
    """
    Print progress/working message.
    
    Args:
        message: Progress message
        icon: Icon to use (default: clock)
    """
    print(f"  {colored(f'{icon} {message}...', Colors.CYAN)}", end=" ", flush=True)

def print_result(status: str):
    """
    Print result after progress (call after print_progress).
    
    Args:
        status: Result status (e.g., "DONE", "FAILED")
    """
    if status in ["DONE", "OK", "SUCCESS", "PASSED"]:
        print(colored(f"{Icons.CHECK} {status}", Colors.GREEN))
    elif status in ["FAILED", "ERROR"]:
        print(colored(f"{Icons.CROSS} {status}", Colors.RED))
    else:
        print(colored(status, Colors.ORANGE))

def format_severity(severity: str) -> str:
    """
    Format severity level with color.
    
    Args:
        severity: Severity level (LOW, MEDIUM, HIGH)
    
    Returns:
        Colored severity string
    """
    if severity == 'LOW':
        return colored(f"{severity} {Icons.SUCCESS}", Colors.GREEN)
    elif severity == 'MEDIUM':
        return colored(f"{severity} {Icons.WARNING}", Colors.ORANGE)
    elif severity == 'HIGH':
        return colored(f"{severity} {Icons.ERROR}", Colors.RED)
    else:
        return severity

def format_decision(decision: str) -> str:
    """
    Format CEO decision with color.
    
    Args:
        decision: Decision type (ACCEPT, REGENERATE_PARTIAL, REGENERATE_FULL)
    
    Returns:
        Colored decision string
    """
    if decision == 'ACCEPT':
        return colored(f"{decision} {Icons.SUCCESS}", Colors.GREEN)
    elif decision in ['REGENERATE_PARTIAL', 'REGENERATE_FULL']:
        return colored(f"{decision} {Icons.WARNING}", Colors.ORANGE)
    else:
        return colored(f"{decision} {Icons.ERROR}", Colors.RED)
