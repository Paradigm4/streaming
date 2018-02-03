import scidbstrm

while True:
    df = scidbstrm.read()
    scidbstrm.write(df)
    if df is None:
        break
