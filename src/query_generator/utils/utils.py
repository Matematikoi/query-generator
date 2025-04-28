import random

import numpy as np


def set_seed() -> None:
  seed = 80
  np.random.seed(seed)
  random.seed(seed)
