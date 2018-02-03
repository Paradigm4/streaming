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
          'python -u /stream/tests/scripts/one_chunk.py',
          'format=feather',
          'types={scidb_ty}'{names})""".format(
              input=input,
              scidb_ty=scidb_ty,
              names='' if name is None else ", 'names={}'".format(name)),
                    fetch=True,
                    as_dataframe=False)
    print(res)
    print(numpy.array(
        [(0, 0, i, (255, eval('{}({})'.format(
            py_type_map.get(scidb_ty, scidb_ty), i))))
         for i in range(3)],
        dtype=[('instance_id', '<i8'),
               ('chunk_no', '<i8'),
               ('value_no', '<i8'),
               ('a0' if name is None else name,
                [('null', 'u1'),
                 ('val', np_type_map.get(scidb_ty, scidb_ty))])]))

    assert(numpy.array_equal(res, numpy.array(
        [(0, 0, i, (255, eval('{}({})'.format(
            py_type_map.get(scidb_ty, scidb_ty),
            str(i).encode() if scidb_ty == 'binary' else i))))
         for i in range(3)],
        dtype=[('instance_id', '<i8'),
               ('chunk_no', '<i8'),
               ('value_no', '<i8'),
               ('a0' if name is None else name,
                [('null', 'u1'),
                 ('val', np_type_map.get(scidb_ty, scidb_ty))])])))


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
          'python -u /stream/tests/scripts/any_chunks.py',
          'format=feather',
          'types={scidb_ty}'{names})""".format(
              input=input,
              scidb_ty=scidb_ty,
              names='' if name is None else ", 'names={}'".format(name)),
                    fetch=True,
                    atts_only=True,
                    as_dataframe=False)
    np_name = 'a0' if name is None else name
    assert(numpy.array_equal(res[res[np_name]['val'].argsort()], numpy.array(
        [((255, eval('{}({})'.format(
            py_type_map.get(scidb_ty, scidb_ty),
            str(i).encode() if scidb_ty == 'binary' else i))),)
         for i in range(10)],
        dtype=[(np_name, [('null', 'u1'),
                          ('val', np_type_map.get(scidb_ty, scidb_ty))])])))
