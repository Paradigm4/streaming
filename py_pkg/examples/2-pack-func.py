"""Usage:

> python 2-pack-func.py
[(0, 0, 0, (255, 1.))]

Setup:

> pip install scidb-py
"""

import dill
import numpy
import scidbpy
import scidbstrm
import sys


db = scidbpy.connect()


def get_first(df):
    return df.head(1)


# Serialize (pack) and Upload function to SciDB
ar_fun = db.input(upload_data=scidbstrm.pack_func(get_first),
                  upload_schema=scidbpy.Schema.fromstring(
                      '<x:binary not null>[i]')).store()


que = db.stream(
    'build(<x:double>[i=1:5], i)',
    """'python{major} -uc "
import scidbstrm

map_fun = scidbstrm.read_func()
scidbstrm.map(map_fun)

"'""".format(major=sys.version_info.major),
    "'format=feather'",
    "'types=double'",
    '_sg({}, 0)'.format(ar_fun.name)  # Array with Serialized function
)

print(que.fetch(as_dataframe=False))
