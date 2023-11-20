import sqlite3

def CountArticlesInTitlesTable(database_name='wikipedia_database_jp.db'):
    """Get count of articles in the WikipediaTitles table
    """    
    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()

    cursor.execute('SELECT COUNT(article_id) AS ArticleCount FROM WikipediaTitles')
    articleCount = cursor.fetchone()[0]
    
    connection.close()

    return articleCount


def PrintTitlesTable(database_name='wikipedia_database_jp.db'):
    """Print the contents of the WikipediaTitles table
    """
    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM WikipediaTitles LIMIT 10')
    print("TITLES:\n")
    for row in cursor: 
        print(row)
    
    connection.close()

    return 1


def test1():
    """Check that there are articles in the WikipediaTitles table
    """
    assert CountArticlesInTitlesTable() > 0, "Should be greater than 0. Latest was 1961562"




def main():
    PrintTitlesTable()


if __name__ == "__main__":
    main()

