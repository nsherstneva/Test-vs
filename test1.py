## %%
import re

import numpy as np
import pandas as pd

ss = 'Hello'

s1 = re.search('ll*', ss).group()

print('{} - {}'.format(ss, s1))

people = {
    "first": ["Corey"],
    "last": ["Schafer"],
    "email": ["CoreyMSchafer@gmail.com"]
}
df = pd.DataFrame(people)

print(df)


def numers():
    """[function]
    """

    for i in range(0, 10):
        print(i)

