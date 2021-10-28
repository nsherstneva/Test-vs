## %%
import re

import numpy as np
import pandas as pd

ss = 'Hello !!!'

s1 = re.search('ll*', ss).group()

print('{} - {}'.format(ss, s1))



