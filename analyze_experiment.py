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
    day1_day = pd.to_datetime('2023-10-15', format='%Y-%m-%d')
    day1['day'] = pd.Series([day1_day for x in range(len(day1))])
    day1['startTime'] = pd.Series([pd.to_datetime(day1.iloc[x, 4], unit='s') for x in range(len(day1))])
    day1['endTime'] = pd.Series([pd.to_datetime(day1.iloc[x, 5], unit='s') for x in range(len(day1))])
    day1['visit_time'] = pd.Series([(day1.iloc[x, 5] - day1.iloc[x, 4])*3600 for x in range(len(day1))])
    print(f"{day1_day}:\n{day1.describe()}")

    day2 = pd.read_parquet(exp_day2_filename, engine='pyarrow')
    day2_day = pd.to_datetime('2023-10-16', format='%Y-%m-%d')
    day2['day'] = pd.Series([day2_day for x in range(len(day2))])
    day2['startTime'] = pd.Series([pd.to_datetime(day2.iloc[x, 4], unit='s') for x in range(len(day2))])
    day2['endTime'] = pd.Series([pd.to_datetime(day2.iloc[x, 5], unit='s') for x in range(len(day2))])
    day2['visit_time'] = pd.Series([(day2.iloc[x, 5] - day2.iloc[x, 4])*3600 for x in range(len(day2))])
    print(f"{day2_day}:\n{day2.describe()}")

    day3 = pd.read_parquet(exp_day3_filename, engine='pyarrow')
    day3_day = pd.to_datetime('2023-10-17', format='%Y-%m-%d')
    day3['day'] = pd.Series([day3_day for x in range(len(day3))])
    day3['startTime'] = pd.Series([pd.to_datetime(day3.iloc[x, 4], unit='s') for x in range(len(day3))])
    day3['endTime'] = pd.Series([pd.to_datetime(day3.iloc[x, 5], unit='s') for x in range(len(day3))])
    day3['visit_time'] = pd.Series([(day3.iloc[x, 5] - day3.iloc[x, 4])*3600 for x in range(len(day3))])
    print(f"{day3_day}:\n{day3.describe()}")

    day4 = pd.read_parquet(exp_day4_filename, engine='pyarrow')
    day4_day = pd.to_datetime('2023-10-18', format='%Y-%m-%d')
    day4['day'] = pd.Series([day4_day for x in range(len(day4))])
    day4['startTime'] = pd.Series([pd.to_datetime(day4.iloc[x, 4], unit='s') for x in range(len(day4))])
    day4['endTime'] = pd.Series([pd.to_datetime(day4.iloc[x, 5], unit='s') for x in range(len(day4))])
    day4['visit_time'] = pd.Series([(day4.iloc[x, 5] - day4.iloc[x, 4])*3600 for x in range(len(day4))])
    print(f"{day4_day}:\n{day4.describe()}")

    day5 = pd.read_parquet(exp_day5_filename, engine='pyarrow')
    day5_day = pd.to_datetime('2023-10-19', format='%Y-%m-%d')
    day5['day'] = pd.Series([day5_day for x in range(len(day5))])
    day5['startTime'] = pd.Series([pd.to_datetime(day5.iloc[x, 4], unit='s') for x in range(len(day5))])
    day5['endTime'] = pd.Series([pd.to_datetime(day5.iloc[x, 5], unit='s') for x in range(len(day5))])
    day5['visit_time'] = pd.Series([(day5.iloc[x, 5] - day5.iloc[x, 4])*3600 for x in range(len(day5))])
    print(f"{day5_day}:\n{day5.describe()}")

    exp_all = pd.concat([day1, day2, day3, day4, day5])

    exp_all.to_pickle(exp_all_filename)


def main():

    read_data()
  


if __name__ == "__main__":
    main()