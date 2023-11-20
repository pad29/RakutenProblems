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


def CountArticlesInCategoriesTable(database_name='wikipedia_database_jp.db'):
    """Get count of articles in the WikipediaCategories table
    """
    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()

    cursor.execute('SELECT COUNT(article_id) AS ArticleCount FROM WikipediaCategories')
    articleCount = cursor.fetchone()[0]
    
    connection.close()

    return articleCount


def PrintCategoriesTable(database_name='wikipedia_database_jp.db'):
    """Print the contents of the WikipediaCategories table
    """
    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM WikipediaCategories LIMIT 10')
    print("CATEGORIES:\n")
    for row in cursor: 
        print(row)
    
    connection.close()

    return 1

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
    assert CountArticlesInTitlesTable() == 1, "Should be greater than 0"

def test2():
    """Check that there are articles in the WikipediaCategories table
    """
    assert CountArticlesInCategoriesTable() == 1, "Should be greater than 0"




def main():
    PrintTitlesTable()
    PrintCategoriesTable()


if __name__ == "__main__":
    main()

