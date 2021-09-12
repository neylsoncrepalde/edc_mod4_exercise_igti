#!/usr/local/bin python
import pandas as pd

url = 'https://raw.githubusercontent.com/jbrownlee/Datasets/master/pima-indians-diabetes.data.csv'

df = pd.read_csv(url, header=None)

df["novacoluna"] = df[5].apply(lambda x: x*2)

print(df.columns)

print(df.head())

print(df.shape)
