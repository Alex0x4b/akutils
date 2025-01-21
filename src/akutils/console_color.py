from rich.console import Console

console = Console()


def print_orange(message: str, **kwargs):
    """
    print_orange(message="hello world")

    Usage with warning
    ------------------
    warnings.warn = print_orange  # Override default warn
    warnings.warn(message="hello world")
    """
    console.print(f"[yellow]{message}[/yellow]")
