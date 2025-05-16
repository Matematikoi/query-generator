def is_float(s: str) -> bool:
  try:
    float(s)
    return True
  except ValueError:
    return False
