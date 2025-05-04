# Index
1. [Installation](#installation)
1. [Execution](#execution)


# Installation
We use [pixi](https://pixi.sh/latest/#installation) to install 
the necessary packages to install all the necessary packages. Once 
installed you can activate a shell with 
```bash
pixi shell -e dev
```
To activate the shell that has all the packages for python. To access
the python binary you can run 
```bash
which python
```

With pixi you are also installing our linter, formatter and our libraries
for testing. You can access this tasks as a pixi task. 
```bash
pixi run format # Formats src and test files
pixi run check  # Checks lint rules in src
pixi run typing # Uses mypy to validate types
pixi run lint   # Runs format, check and typing
pixi run test   # Runs tests
pixi run main   # Runs main endpoint
pixi run commit # Commits changes after checking lint and test
```

# Execution
## Main program
For documentation you may run
```bash
pixi run main --help
```
This will show several endpoints that you may understand by running

```bash
pixi run main snowflake --help
```