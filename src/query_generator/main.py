import typer
from rich import print

app = typer.Typer(name="Query Generation")


def square(x: int) -> int:
  return x**2


@app.command()
def drive() -> None:
  a = 2
  print(f"[green] Some green {square(a)} text[/green]")


@app.command()
def stop() -> None:
  print("[red] Some red text[/red]")


if __name__ == "__main__":
  app()
