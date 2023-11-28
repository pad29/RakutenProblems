import pandas as pd

exp_day1_filename = "ab_test_dataset/2023-10-15.parquet"
exp_day2_filename = "ab_test_dataset/2023-10-16.parquet"
exp_day3_filename = "ab_test_dataset/2023-10-17.parquet"
exp_day4_filename = "ab_test_dataset/2023-10-18.parquet"
exp_day5_filename = "ab_test_dataset/2023-10-19.parquet"
exp_all_filename = "ab_test_dataset/exp_all.pkl"

def read_data():
    """Load from parquet files into dataframe and compute a few additional columns, then store it on disk for further analysis
    """
    day1 = pd.read_parquet(exp_day1_filename, engine='pyarrow')
    day1['day'] = pd.to_datetime('2023-10-15', format='%Y-%m-%d')
    day1['start_time'] = day1.apply(lambda row: pd.to_datetime(row.start, unit='s'), axis = 1)
    day1['end_time'] = day1.apply(lambda row: pd.to_datetime(row.end, unit='s'), axis = 1)
    day1['visit_time'] = day1.apply(lambda row: (row.end - row.start)/3600, axis = 1)
    print(f"2023-10-15:\n{day1.describe()}")

    day2 = pd.read_parquet(exp_day2_filename, engine='pyarrow')
    day2['day'] = pd.to_datetime('2023-10-16', format='%Y-%m-%d')
    day2['start_time'] = day2.apply(lambda row: pd.to_datetime(row.start, unit='s'), axis = 1)
    day2['end_time'] = day2.apply(lambda row: pd.to_datetime(row.end, unit='s'), axis = 1)
    day2['visit_time'] = day2.apply(lambda row: (row.end - row.start)/3600, axis = 1)
    print(f"2023-10-16:\n{day2.describe()}")

    day3 = pd.read_parquet(exp_day3_filename, engine='pyarrow')
    day3['day'] = pd.to_datetime('2023-10-17', format='%Y-%m-%d')
    day3['start_time'] = day3.apply(lambda row: pd.to_datetime(row.start, unit='s'), axis = 1)
    day3['end_time'] = day3.apply(lambda row: pd.to_datetime(row.end, unit='s'), axis = 1)
    day3['visit_time'] = day3.apply(lambda row: (row.end - row.start)/3600, axis = 1)
    print(f"2023-10-17:\n{day3.describe()}")

    day4 = pd.read_parquet(exp_day4_filename, engine='pyarrow')
    day4['day'] = pd.to_datetime('2023-10-18', format='%Y-%m-%d')
    day4['start_time'] = day4.apply(lambda row: pd.to_datetime(row.start, unit='s'), axis = 1)
    day4['end_time'] = day4.apply(lambda row: pd.to_datetime(row.end, unit='s'), axis = 1)
    day4['visit_time'] = day4.apply(lambda row: (row.end - row.start)/3600, axis = 1)
    print(f"2023-10-18:\n{day4.describe()}")

    day5 = pd.read_parquet(exp_day5_filename, engine='pyarrow')
    day5['day'] = pd.to_datetime('2023-10-19', format='%Y-%m-%d')
    day5['start_time'] = day5.apply(lambda row: pd.to_datetime(row.start, unit='s'), axis = 1)
    day5['end_time'] = day5.apply(lambda row: pd.to_datetime(row.end, unit='s'), axis = 1)
    day5['visit_time'] = day5.apply(lambda row: (row.end - row.start)/3600, axis = 1)
    print(f"2023-10-19:\n{day5.describe()}")

    exp_all = pd.concat(
        [day1[day1['variant']=='test'],
         day1[day1['variant']=='control'],
         day2[day2['variant']=='test'],
         day2[day2['variant']=='control'],
         day3[day3['variant']=='test'],
         day3[day3['variant']=='control'], 
         day4[day4['variant']=='test'],
         day4[day4['variant']=='control'], 
         day5[day5['variant']=='test'],
         day5[day5['variant']=='control'], 
        ])

    exp_all.to_pickle(exp_all_filename)


def main():

    read_data()
  


if __name__ == "__main__":
    main()