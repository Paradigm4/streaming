import dill
import io
import scidbpy
import scidbstrm


# Setup:
# pip install sicdb-py sklearn scipy
#
# On SciDB server:
# pip install sklearn scipy


db = scidbpy.connect()


# -- - --
# 1. Load CSV training data
# -- - --
# https://www.kaggle.com/c/digit-recognizer/download/train.csv
db.aio_input(
    "'path=/kaggle/train.csv'",
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
    import numpy

    df['a0'] = df['a0'].map(int)
    df['error'] = df['error'].map(
        lambda x: numpy.array(map(int, x.split(',')[1:]),
                              dtype=numpy.uint8).tobytes())
    return df

ar_fun = db.input(upload_data=scidbstrm.pack_func(map_to_bin)).store()
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
    import numpy

    def bin_to_bw(img):
        img_ar = numpy.frombuffer(img, dtype=numpy.uint8).copy()
        img_ar[img_ar > 1] = 1
        return img_ar.tobytes()

    df['img'] = df['img'].map(bin_to_bw)
    return df

ar_fun = db.input(upload_data=scidbstrm.pack_func(map_to_bw)).store()
que = db.stream(
    db.arrays.train_bin,
    scidbstrm.python_map,
    "'format=feather'",
    "'types=int64,binary'",
    "'names=label,img'",
    '_sg({}, 0)'.format(ar_fun.name)
).store(
    db.arrays.train_bw)

# AFL% limit(train_bw, 3);
# {instance_id,chunk_no,value_no} label,img
# {0,0,0} 1,<binary>
# {0,0,1} 0,<binary>
# {0,0,2} 1,<binary>

# Plot:
# matplotlib.pyplot.imshow(
#     numpy.frombuffer(db.limit(db.arrays.train_bw, 1)[0]['img']['val'],
#                      dtype=numpy.uint8).reshape((28, 28)),
#     cmap='binary')


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


ar_fun = db.input(upload_data=scidbstrm.pack_func(Train)).store()
python_run = """'python -uc "
import io
import numpy
import pandas
import scidbstrm
import sklearn.externals
import sklearn.linear_model

Train = scidbstrm.read_func()
Train.model = sklearn.linear_model.SGDClassifier()
scidbstrm.map(Train.map, Train.finalize)
"'"""
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


ar_fun = db.input(upload_data=scidbstrm.pack_func(merge_models)).store()

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
# 6. Load CSV test data
# -- - --
# https://www.kaggle.com/c/digit-recognizer/download/test.csv
db.input(
    '<img:string>[ImageID=1:*]',
    "'/kaggle/test.csv'",
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
# 7. Predict labels for test data
# -- - --
class Predict:
    model = None

    @staticmethod
    def csv_to_bw(csv):
        img_ar = numpy.array(map(int, csv.split(',')), dtype=numpy.uint8)
        img_ar[img_ar > 1] = 1
        return img_ar

    @staticmethod
    def map(df):
        img = df['img'].map(Predict.csv_to_bw)
        df['img'] = Predict.model.predict(numpy.matrix(img.tolist()))
        return df

ar_fun = db.input(
    upload_data=scidbstrm.pack_func(Predict)
).cross_join(
    db.arrays.model_final
).store()

python_run = """'python -uc "
import dill
import io
import numpy
import scidbstrm
import sklearn.externals

df = scidbstrm.read()
Predict = dill.loads(df.iloc[0, 0])
Predict.model = sklearn.externals.joblib.load(io.BytesIO(df.iloc[0, 2]))
scidbstrm.write()

scidbstrm.map(Predict.map)
"'"""
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
    db.arrays.predict)

# AFL% limit(predict, 3);
# {instance_id,chunk_no,value_no} Label,ImageID
# {0,0,0} 2,1
# {0,0,1} 0,2
# {0,0,2} 9,3


# -- - --
# 8. Download results
# -- - --
df = db.arrays.predict.fetch(as_dataframe=True)
df['ImageID'] = df['ImageID'].map(int)
df['Label'] = df['Label'].map(int)
df.to_csv('results.csv',
          header=True,
          index=False,
          columns=('ImageID', 'Label'))
