import numpy as np
import random


def set_seed():
  seed = 80
  np.random.seed(seed)
  random.seed(seed)
