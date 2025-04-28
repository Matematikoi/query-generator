from rich import print


def show_dev_warning(dev: bool) -> None:
  """
  Shows a warning if the dev flag is set to true.
  This is used to test the code in development mode.
  """
  if dev:
    print(
      "Running on [red] development mode [/red]"
      "This means that results are not valid and only for testing "
      "purposes"
    )
  else:
    print(
      "Running on [green] production mode [/green]"
      "\nThis will run using the full dataset and the results are valid"
    )
