# BEGIN_COPYRIGHT
#
# Copyright (C) 2017-2021 Paradigm4 Inc.
# All Rights Reserved.
#
# scidbbridge is a plugin for SciDB, an Open Source Array DBMS
# maintained by Paradigm4. See http://www.paradigm4.com/
#
# scidbbridge is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as
# published by the Free Software Foundation.
#
# scidbbridge is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY
# KIND, INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See the
# AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public
# License along with scidbbridge. If not, see
# <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT

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
    '_sg({}, 0)'.format(ar_fun.name),  # Array with Serialized function
    """'python{major} -uc "
import scidbstrm

map_fun = scidbstrm.read_func()
scidbstrm.map(map_fun)

"'""".format(major=sys.version_info.major),
    "format:'feather'",
    "types:'double'"
)

print(que.fetch(as_dataframe=False))
