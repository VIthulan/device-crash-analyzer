import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

no_ip_df = pd.read_csv('/home/vithulanv/Experiments/data_analytics/elastic_data_downloader/no_ip_1_1.csv')

array = np.arange(len(no_ip_df.columns))
array.fill(0)
print(array)
k = 0
# print(no_ip_df.iloc[0])

columns = no_ip_df.columns
for index, row in no_ip_df.iterrows():
    if k == 0:
        array = row[:]
        k = k +1
    # i = 0
    # for column in columns:
    #     if array[i] != row[column]:
    #         array[i] = "?"

print(array)