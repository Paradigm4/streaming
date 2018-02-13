import dill
import io
import scidbpy
import scidbstrm
import sys


# Setup:
# pip install sicdb-py sklearn scipy
#
# On SciDB server:
# pip install sklearn scipy


python = 'python{}'.format(sys.version_info.major)
db = scidbpy.connect()


# -- - --
# 1. Load CSV training data
# -- - --
# https://www.kaggle.com/c/digit-recognizer/download/train.csv
db.aio_input(
    "'path=/stream/py_pkg/examples/train.csv'",
    "'num_attributes=1'",
    "'attribute_delimiter=,'",
    "'header=1'"
).store(
    db.arrays.train_csv)

# AFL% limit(train_csv, 3);
# {tuple_no,dst_instance_id,src_instance_id} a0,error
# {0,0,0} '1','long,0,0,0,...'
# {1,0,0} '0','long,0,0,0,...'
# {2,0,0} '1','long,0,0,0,...'


# -- - --
# 2. Convert CSV to binary
# -- - --
def map_to_bin(df):
    # Workaround for NumPy bug #10338
    # https://github.com/numpy/numpy/issues/10338
    import os
    os.environ.setdefault('PATH', '')
    import numpy

    df['a0'] = df['a0'].map(int)
    df['error'] = df['error'].map(
        lambda x: numpy.array(list(map(int, x.split(',')[1:])),
                              dtype=numpy.uint8).tobytes())
    return df


upload_schema = scidbpy.Schema.fromstring('<x:binary not null>[i]')
ar_fun = db.input(upload_data=scidbstrm.pack_func(map_to_bin),
                  upload_schema=upload_schema).store()
que = db.stream(
    db.arrays.train_csv,
    scidbstrm.python_map,
    "'format=feather'",
    "'types=int64,binary'",
    "'names=label,img'",
    '_sg({}, 0)'.format(ar_fun.name)
).store(
    db.arrays.train_bin)

# AFL% limit(train_bin, 3);
# {instance_id,chunk_no,value_no} label,img
# {0,0,0} 1,<binary>
# {0,0,1} 0,<binary>
# {0,0,2} 1,<binary>

# Plot:
# %matplotlib gtk
# matplotlib.pyplot.imshow(
#     numpy.frombuffer(db.limit(db.arrays.train_bin, 1)[0]['img']['val'],
#                      dtype=numpy.uint8).reshape((28, 28)),
#     cmap='gray')


# -- - --
# 3. Convert images to black and white
# -- - --
def map_to_bw(df):
    import os
    os.environ.setdefault('PATH', '')
    import numpy

    def bin_to_bw(img):
        img_ar = numpy.frombuffer(img, dtype=numpy.uint8).copy()
        img_ar[img_ar > 1] = 1
        return img_ar.tobytes()

    df['img'] = df['img'].map(bin_to_bw)
    return df


que = db.iquery("""
store(
  stream(
    train_bin,
    {script},
    'format=feather',
    'types=int64,binary',
    'names=label,img',
    _sg(
      input(
        {{sch}},
        '{{fn}}',
        0,
        '{{fmt}}'),
      0)),
  train_bw)""".format(script=scidbstrm.python_map),
                upload_data=scidbstrm.pack_func(map_to_bw),
                upload_schema=upload_schema)


# AFL% limit(train_bw, 3);
# {instance_id,chunk_no,value_no} label,img
# {0,0,0} 1,<binary>
# {0,0,1} 0,<binary>
# {0,0,2} 1,<binary>

# Plot:
# matplotlib.pyplot.imshow(
#     numpy.frombuffer(db.limit(db.arrays.train_bw, 1)[0]['img']['val'],
#                      dtype=numpy.uint8).reshape((28, 28)),
#     cmap='gray')


# -- - --
# 4. Train multiple models in parallel
# -- - --

class Train:
    model = None
    count = 0

    @staticmethod
    def map(df):
        img = df['img'].map(
            lambda x: numpy.frombuffer(x, dtype=numpy.uint8))
        Train.model.partial_fit(numpy.matrix(img.tolist()),
                                df['label'],
                                range(10))
        Train.count += len(df)
        return None

    @staticmethod
    def finalize():
        if Train.count == 0:
            return None
        buf = io.BytesIO()
        sklearn.externals.joblib.dump(Train.model, buf)
        return pandas.DataFrame({
            'count': [Train.count],
            'model': [buf.getvalue()]})


ar_fun = db.input(upload_data=scidbstrm.pack_func(Train),
                  upload_schema=upload_schema).store()
python_run = """'{python} -uc "
import io
import os
os.environ.setdefault(\\\"PATH\\\", \\\"\\\")
import numpy
import pandas
import scidbstrm
import sklearn.externals
import sklearn.linear_model

Train = scidbstrm.read_func()
Train.model = sklearn.linear_model.SGDClassifier()
scidbstrm.map(Train.map, Train.finalize)
"'""".format(python=python)
que = db.stream(
    db.arrays.train_bin,
    python_run,
    "'format=feather'",
    "'types=int64,binary'",
    "'names=count,model'",
    '_sg({}, 0)'.format(ar_fun.name)
).store(
    db.arrays.model)

# AFL% scan(model);
# {instance_id,chunk_no,value_no} count,model
# {0,0,0} 22949,<binary>
# {1,0,0} 19051,<binary>


# -- - --
# 5. Merge partial models
# -- - --

def merge_models(df):
    import io
    import pandas
    import sklearn.ensemble
    import sklearn.externals

    estimators = [sklearn.externals.joblib.load(io.BytesIO(byt))
                  for byt in df['model']]
    if not estimators:
        return None

    labelencoder = sklearn.preprocessing.LabelEncoder()
    labelencoder.fit(range(10))

    model = sklearn.ensemble.VotingClassifier(())
    model.estimators_ = estimators
    model.le_ = labelencoder

    buf = io.BytesIO()
    sklearn.externals.joblib.dump(model, buf)

    return pandas.DataFrame({'count': df.sum()['count'],
                             'model': [buf.getvalue()]})


ar_fun = db.input(upload_data=scidbstrm.pack_func(merge_models),
                  upload_schema=upload_schema).store()
que = db.redimension(
    db.arrays.model,
    '<count:int64, model:binary> [i]'
).stream(
    scidbstrm.python_map,
    "'format=feather'",
    "'types=int64,binary'",
    "'names=count,model'",
    '_sg({}, 0)'.format(ar_fun.name)
).store(
    db.arrays.model_final)

# AFL% scan(model_final);
# {instance_id,chunk_no,value_no} count,model
# {0,0,0} 42000,<binary>


# -- - --
# 6. Predict labels for train data
# -- - --
def predict(df):
    img = df['img'].map(
        lambda x: numpy.frombuffer(x, dtype=numpy.uint8))
    df['img'] = model.predict(numpy.matrix(img.tolist()))
    return df


ar_fun = db.input(
    upload_data=scidbstrm.pack_func(predict),
    upload_schema=upload_schema
).cross_join(
    db.arrays.model_final
).store()

python_run = """'{python} -uc "
import dill
import io
import os
os.environ.setdefault(\\\"PATH\\\", \\\"\\\")
import numpy
import scidbstrm
import sklearn.externals

df = scidbstrm.read()
predict = dill.loads(df.iloc[0, 0])
model = sklearn.externals.joblib.load(io.BytesIO(df.iloc[0, 2]))
scidbstrm.write()

scidbstrm.map(predict)
"'""".format(python=python)
que = db.stream(
    db.arrays.train_bw,
    python_run,
    "'format=feather'",
    "'types=int64,int64'",
    "'names=Label,Predict'",
    '_sg({}, 0)'.format(ar_fun.name)
).store(
    db.arrays.predict_train)

# AFL% limit(predict_train, 3);
# {instance_id,chunk_no,value_no} Label,Predict
# {0,0,0} 1,1
# {0,0,1} 0,0
# {0,0,2} 1,1

# Plot:
# %matplotlib gtk
# df = db.arrays.predict_train.fetch(as_dataframe=True)
# def rand_jitter(arr):
#     return arr + np.random.randn(len(arr)) * .4
# matplotlib.pyplot.xticks(range(10))
# matplotlib.pyplot.yticks(range(10))
# matplotlib.pyplot.xlabel('True')
# matplotlib.pyplot.ylabel('Predicted')
# matplotlib.pyplot.plot(
#     rand_jitter(df['Label']), rand_jitter(df['Predict']), '.', ms=1)


# -- - --
# 7. Load CSV test data
# -- - --
# https://www.kaggle.com/c/digit-recognizer/download/test.csv
db.input(
    '<img:string>[ImageID=1:*]',
    "'/stream/py_pkg/examples/test.csv'",
    0,
    'csv:lt'
).store(
    db.arrays.test_csv)

# AFL% limit(test_csv, 3);
# {ImageID} img
# {1} '0,0,0,...'
# {2} '0,0,0,...'
# {3} '0,0,0,...'


# -- - --
# 8. Predict labels for test data
# -- - --
class Predict:
    model = None

    @staticmethod
    def csv_to_bw(csv):
        img_ar = numpy.array(list(map(int, csv.split(','))), dtype=numpy.uint8)
        img_ar[img_ar > 1] = 1
        return img_ar

    @staticmethod
    def map(df):
        img = df['img'].map(Predict.csv_to_bw)
        df['img'] = Predict.model.predict(numpy.matrix(img.tolist()))
        return df


ar_fun = db.input(
    upload_data=scidbstrm.pack_func(Predict),
    upload_schema=upload_schema
).cross_join(
    db.arrays.model_final
).store()

python_run = """'{python} -uc "
import dill
import io
import os
os.environ.setdefault(\\\"PATH\\\", \\\"\\\")
import numpy
import scidbstrm
import sklearn.externals

df = scidbstrm.read()
Predict = dill.loads(df.iloc[0, 0])
Predict.model = sklearn.externals.joblib.load(io.BytesIO(df.iloc[0, 2]))
scidbstrm.write()

scidbstrm.map(Predict.map)
"'""".format(python=python)
que = db.apply(
    db.arrays.test_csv,
    'ImageID',
    'ImageID'
).stream(
    python_run,
    "'format=feather'",
    "'types=int64,int64'",
    "'names=Label,ImageID'",
    '_sg({}, 0)'.format(ar_fun.name)
).store(
    db.arrays.predict_test)

# AFL% limit(predict_test, 3);
# {instance_id,chunk_no,value_no} Label,ImageID
# {0,0,0} 2,1
# {0,0,1} 0,2
# {0,0,2} 9,3


# -- - --
# 9. Download results
# -- - --
df = db.arrays.predict_test.fetch(as_dataframe=True)
df['ImageID'] = df['ImageID'].map(int)
df['Label'] = df['Label'].map(int)
df.to_csv('results.csv',
          header=True,
          index=False,
          columns=('ImageID', 'Label'))

# -- - --
# 10. Cleanup
# -- - --
for ar in (db.arrays.train_csv,
           db.arrays.train_bin,
           db.arrays.train_bw,
           db.arrays.model,
           db.arrays.model_final,
           db.arrays.predict_train,
           db.arrays.test_csv,
           db.arrays.predict_test):
    db.remove(ar)
