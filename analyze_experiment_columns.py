import pandas as pd
from scipy.stats import chisquare

exp_all_filename = "ab_test_dataset/exp_all.pkl"
pvs_stats_filename = "ab_test_dataset/pvs_stats.csv"
id_stats_filename = "ab_test_dataset/id_stats.csv"

def read_dataframe(filename=exp_all_filename):
    """Read the experiment dataframe
    """
    return pd.read_pickle(filename)


def analyze_pvs(pvs, stats_filename=pvs_stats_filename):
    """Perform various analyses for pvs column
    """
    stats = pvs.describe()
    stats.to_csv(stats_filename)

def analyze_id(id, stats_filename=id_stats_filename):
    """Perform various analyses for pvs column
    """
    #print(f"All:\n{id.describe()}")

    test=id[id['variant'] == 'test']
    print(f"Test total values:{len(test)}")
    print(f"Test unique values:{test.id.nunique()}")

    control=id[id['variant'] == 'control']
    print(f"Control total values:{len(control)}")
    print(f"Control unique values:{control.id.nunique()}")

    #perform SRM test
    observed = [test.id.nunique(), control.id.nunique()]
    expected = [(test.id.nunique() + control.id.nunique())/2,(test.id.nunique() + control.id.nunique())/2]
    chi = chisquare(observed, f_exp = expected)
    print(chi)
    if chi[1] < 0.01:
        print("Sample ratio mismatch (SRM) may be present")
    else:
        print("Sample ratio mismatch (SRM) may not be present")
 



def main():
    exp_all = read_dataframe()    
    #analyze_pvs(exp_all[['pvs','variant']])
    analyze_id(exp_all[['id','variant']])



if __name__ == "__main__":
    main()
