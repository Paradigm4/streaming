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

import numpy
import pytest
import scidbpy


@pytest.fixture(scope='module')
def db():
    return scidbpy.connect()


# SciDB Supported Types
scidb_types = (
    'int64',
    'double',
    'string',
    'binary',
)


# NumPy Type Map: SciDB Type -> NumPy Type
np_type_map = {
    'string': 'O',
    'binary': 'O',
}


# Python Type Map: SciDB Type -> Python Type
py_type_map = {
    'int64': 'int',
    'double': 'float',
    'string': 'str',
    'binary': 'bytes',
}


@pytest.mark.parametrize(('scidb_ty', 'name'), [
    (scidb_ty, name)
    for scidb_ty in scidb_types
    for name in (None, 'foo')
])
def test_one_chunk(db, scidb_ty, name):
    if scidb_ty == 'binary':
        input_ar = db.input(
            '<x:binary not null>[i=0:2:0:3]',
            upload_data=numpy.array([str(i).encode() for i in range(3)],
                                    dtype='object')).store()
        input = input_ar.name
    else:
        input = 'build(<x:{scidb_ty}>[i=0:2:0:3], {scidb_ty}(i))'.format(
            scidb_ty=scidb_ty)

    res = db.iquery("""
        stream(
          {input},
          'python3 -u /stream/tests/scripts/one_chunk.py',
          format:'feather',
          types:'{scidb_ty}'{names})""".format(
              input=input,
              scidb_ty=scidb_ty,
              names='' if name is None else ", names:'{}'".format(name)),
                    fetch=True,
                    as_dataframe=False)
    assert numpy.array_equal(
        res, numpy.array(
            [(1, 0, i, (255, eval('{}({})'.format(
                py_type_map.get(scidb_ty, scidb_ty),
                str(i).encode() if scidb_ty == 'binary' else i))))
             for i in range(3)],
            dtype=[('instance_id', '<i8'),
                   ('chunk_no', '<i8'),
                   ('value_no', '<i8'),
                   ('a0' if name is None else name,
                    [('null', 'u1'),
                     ('val', np_type_map.get(scidb_ty, scidb_ty))])]))


@pytest.mark.parametrize(('scidb_ty', 'name'), [
    (scidb_ty, name)
    for scidb_ty in scidb_types
    for name in (None, 'foo')
])
def test_any_chunks(db, scidb_ty, name):
    if scidb_ty == 'binary':
        input_ar = db.input(
            '<x:binary not null>[i=0:9:0:3]',
            upload_data=numpy.array([str(i).encode() for i in range(10)],
                                    dtype='object')).store()
        input = input_ar.name
    else:
        input = 'build(<x:{scidb_ty}>[i=0:9:0:3], {scidb_ty}(i))'.format(
            scidb_ty=scidb_ty)

    res = db.iquery("""
        stream(
          {input},
          'python3 -u /stream/tests/scripts/any_chunks.py',
          format:'feather',
          types:'{scidb_ty}'{names})""".format(
              input=input,
              scidb_ty=scidb_ty,
              names='' if name is None else ", names:'{}'".format(name)),
                    fetch=True,
                    atts_only=True,
                    as_dataframe=False)
    np_name = 'a0' if name is None else name
    assert numpy.array_equal(
        res[res[np_name]['val'].argsort()],
        numpy.array(
            [((255, eval('{}({})'.format(
                py_type_map.get(scidb_ty, scidb_ty),
                str(i).encode() if scidb_ty == 'binary' else i))),)
             for i in range(10)],
            dtype=[(np_name, [('null', 'u1'),
                              ('val', np_type_map.get(scidb_ty, scidb_ty))])]))


def test_arrow_1676(db):
    """
    https://issues.apache.org/jira/browse/ARROW-1676
    https://github.com/Paradigm4/stream/issues/16
    """
    df = db.iquery(
        '''
        stream(
          build(
            <val:string>[i=1:10000,10000,0],
            iif(i<10000, string(i), null)),
          'python3 -uc "
import scidbstrm
def f(x):
  x.to_feather(\\"/tmp/arrow_1676.feather\\")
  return x
scidbstrm.map(f)"',
         format:'feather',
         types:'string'
        )''',
        fetch=True)
    assert df.shape == (10000, 4)
