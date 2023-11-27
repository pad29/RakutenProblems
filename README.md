# RakutenProblems

* UserHashing.py : solution to problem 1
* wikiprocess.py : loads wikipedia titles
* wikiindex.py : a helper module used by wikiprocess.py
* categoryprocess.py : loads wikipedia categories
* create_embedding_index.py : creates an embedding index for wikipedia titles
* analyze_experiment.py : loads data from parquet files into a dataframe and adds columns to perpare for future analysis
* test_*.py : tests for the various files above, which also show how to query the dbs and embedding index. Test require pytest to be installed. Some test files can be run directly to print out raw data for examination.