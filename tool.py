from pymongo import MongoClient
import pandas as pd
import numpy as np
import jieba as jb



def connect_db(host='mongodb://localhost:27017/', name='ptt', column='article'):
    cli = MongoClient(host)
    db = cli[name]
    col = db[column]
    return col

def get_data(col, format=None, projection=None):
    if projection is None:
        projection = {'title': 1}
    if format is None:
        format = dict()
    df = pd.DataFrame([row for row in col.find(format, projection)])
    return df

class SentenceProcessor:
    def __init__(self):
        self.stop_word = self.init_stop_word()
        jb.enable_parallel(4)

    def init_stop_word(self, file='./document/stopword.txt'):
        with open(file, 'r', encoding='utf8') as f:
            stop_word = { word.strip() for word in f.readlines() }
        return stop_word

    def remove_stop_word(self, text):
        return [word for word in text if word not in self.stop_word and word.strip()]

    def cut(self, text):
        return [word for word in jb.cut(text)]

    def cut_and_remove(self, text):
        return self.remove_stop_word(self.cut(text))

