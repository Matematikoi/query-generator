import typer
from rich import print 

app = typer.Typer(name = 'Query Generation')

@app.command()
def drive():
    print("[green] Some green text[/green]")

@app.command()
def stop():
    print("[red] Some red text[/red]")

if __name__ == '__main__':
    app()
