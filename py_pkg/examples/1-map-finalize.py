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

# Setup
> iquery --afl --query "
    store(
      apply(
        build(<x:int64 not null>[i=1:10:0:5], i),
        y, double(i) * 10 + .1,
        z, 'foo' + string(i)),
      foo)"
Query was executed successfully

> iquery --afl --query "scan(foo)"
{i} x,y,z
{1} 1,10.1,'foo1'
{2} 2,20.1,'foo2'
{3} 3,30.1,'foo3'
{4} 4,40.1,'foo4'
{5} 5,50.1,'foo5'
{6} 6,60.1,'foo6'
{7} 7,70.1,'foo7'
{8} 8,80.1,'foo8'
{9} 9,90.1,'foo9'
{10} 10,100.1,'foo10'


# Adjust the path to `1-map-finalize.py` script to match your setup
> iquery --afl --query "
    stream(
      foo,
      'python3 -u /stream/py_pkg/examples/1-map-finalize.py',
      format:'feather',
      types:('int64','double','string'),
      names:('x','y','info'))"
{instance_id,chunk_no,value_no} x,y,info
{0,0,0} 15,150.5,'local'
{0,1,0} 15,150.5,'total'
{1,0,0} 40,400.5,'local'
{1,1,0} 40,400.5,'total'


# Cleanup
> iquery --afl --query "remove(foo)"

"""

import scidbstrm
import pandas


df_all = None


# Compute local sum
def map_fun(df):
    global df_all

    if df_all is None:
        df_all = df
    else:
        df_all.append(df)

    se_sum = df.sum()
    df_sum = pandas.DataFrame({'x': [se_sum['x']],
                               'y': [se_sum['y']],
                               'z': ['local']})
    return df_sum


# Compute total sum
def finalize_fun():
    global df_all

    if df_all is None:
        return None

    se_sum = df_all.sum()
    df_sum = pandas.DataFrame({'x': [se_sum['x']],
                               'y': [se_sum['y']],
                               'z': ['total']})
    return df_sum


scidbstrm.map(map_fun, finalize_fun)
