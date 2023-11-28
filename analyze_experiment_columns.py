import pandas as pd
from scipy.stats import chisquare
from contextlib import redirect_stdout


exp_all_filename = "ab_test_dataset/exp_all.pkl"
cleaning_stats_filename = "ab_test_dataset/cleaning_stats.txt"
id_stats_filename = "ab_test_dataset/id_stats.txt"
id_stats_cleaned_filename = "ab_test_dataset/id_stats_cleaned.txt"
pvs_stats_filename = "ab_test_dataset/pvs_stats.txt"
dist_search_queries_stats_filename = "ab_test_dataset/dist_search_queries_stats_filename.txt"
conversion_stats_filename = "ab_test_dataset/conversion_stats.txt"
visit_time_stats_filename = "ab_test_dataset/visit_time_stats.txt"


def read_dataframe(filename=exp_all_filename):
    """Read the experiment dataframe
    """
    return pd.read_pickle(filename)


def clean_data(exp_all, filename=cleaning_stats_filename):
    """Remove all the illegal rows
    """
    with open(filename, 'w') as f:
        with redirect_stdout(f):
            print(f"Original Data:")
            print(f"Test:\n{exp_all[exp_all.variant == 'test'].describe()}")
            print(f"Control:\n{exp_all[exp_all.variant == 'control'].describe()}")

            #remove rows with dates not in range
            start = pd.to_datetime('2023-10-15', format='%Y-%m-%d')
            end = pd.to_datetime('2023-10-20', format='%Y-%m-%d')
            exp_date_cleaned = exp_all[(exp_all.start_time >= start) & (exp_all.end_time < end)]
            print(f"After removing dates not in range:")
            print(f"Test:\n{exp_date_cleaned[exp_date_cleaned.variant == 'test'].describe()}")
            print(f"Control:\n{exp_date_cleaned[exp_date_cleaned.variant == 'control'].describe()}")

            #remove rows with negative session duration
            exp_session_duration_cleaned = exp_date_cleaned[exp_date_cleaned.visit_time > 0]
            print(f"After removing negative session durations:")
            print(f"Test:\n{exp_session_duration_cleaned[exp_session_duration_cleaned.variant == 'test'].describe()}")
            print(f"Control:\n{exp_session_duration_cleaned[exp_session_duration_cleaned.variant == 'control'].describe()}")
    
            return exp_session_duration_cleaned


def analyze_pvs(pvs, filename=pvs_stats_filename):
    """Perform various analyses for pvs column
    """
    with open(filename, 'w') as f:
        with redirect_stdout(f):
            print(f"Test:\n{pvs[pvs.variant == 'test'].describe()}")
            print(f"Control:\n{pvs[pvs.variant == 'control'].describe()}")


def analyze_conversion(conversion, filename=conversion_stats_filename):
    """Perform various analyses for pvs column
    """
    with open(filename, 'w') as f:
        with redirect_stdout(f):
            print(f"Test:\n{conversion[conversion.variant == 'test'].describe()}")
            print(f"Control:\n{conversion[conversion.variant == 'control'].describe()}")


def analyze_visit_time(visit_time, filename=visit_time_stats_filename):
    """Perform various analyses for pvs column
    """
    with open(filename, 'w') as f:
        with redirect_stdout(f):
            print(f"Test:\n{visit_time[visit_time.variant == 'test'].describe()}")
            print(f"Control:\n{visit_time[visit_time.variant == 'control'].describe()}")


def analyze_dist_search_queries(dist_search_queries, filename=dist_search_queries_stats_filename):
    """Perform various analyses for dist_search_queries column
    """
    with open(filename, 'w') as f:
        with redirect_stdout(f):
            print(f"Test:\n{dist_search_queries[dist_search_queries.variant == 'test'].describe()}")
            print(f"Control:\n{dist_search_queries[dist_search_queries.variant == 'control'].describe()}")


def analyze_id(id, filename=id_stats_filename):
    """Perform various analyses for id column
    """
    with open(filename, 'w') as f:
        with redirect_stdout(f):
    
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
                print("WARNING: Sample ratio mismatch (SRM) may be present")
            else:
                print("Sample ratio mismatch (SRM) may not be present")
 



def main():
    exp_all = read_dataframe()    
    analyze_id(exp_all[['id','variant']])
    
    exp_cleaned = clean_data(exp_all)
    analyze_id(exp_cleaned[['id','variant']], id_stats_cleaned_filename)
    analyze_pvs(exp_cleaned[['pvs','variant']])
    analyze_dist_search_queries(exp_cleaned[['dist_search_queries','variant']])
    analyze_conversion(exp_cleaned[['conversion','variant']])
    analyze_visit_time(exp_cleaned[['visit_time','variant']])


    print(list(exp_cleaned))

if __name__ == "__main__":
    main()
