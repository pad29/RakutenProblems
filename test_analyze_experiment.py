import pandas as pd

def read_dataframe_and_print_stats(exp_all_filename="ab_test_dataset/exp_all.pkl"):

    exp_all = pd.read_pickle(exp_all_filename)
    print(list(exp_all))
    print(exp_all.describe())
    print(exp_all.variant.unique())
    
    default=exp_all[exp_all['variant'] == 'default']
    print(default.describe())

    test=exp_all[exp_all['variant'] == 'test']
    print(test.describe())

    control=exp_all[exp_all['variant'] == 'control']
    print(control.describe())




def main():
    read_dataframe_and_print_stats()    


if __name__ == "__main__":
    main()

