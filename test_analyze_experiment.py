import pandas as pd

def read_dataframe_and_print_stats(exp_all_filename="ab_test_dataset/exp_all.pkl"):

    exp_all = pd.read_pickle(exp_all_filename)

    print(exp_all.describe())


def main():
    read_dataframe_and_print_stats()    


if __name__ == "__main__":
    main()

