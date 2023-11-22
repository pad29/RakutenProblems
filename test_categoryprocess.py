import sqlite3


def CountArticlesInCategoriesTable(database_name='wikipedia_database_fat.db'):
    """Get count of articles in the WikipediaCategories table
    """
    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()

    cursor.execute('SELECT COUNT(article_id) AS ArticleCount FROM WikipediaCategories')
    articleCount = cursor.fetchone()[0]
    
    connection.close()

    return articleCount


def PrintCategoriesTable(database_name='wikipedia_database_fat.db'):
    """Print the contents of the WikipediaCategories table
    """
    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM WikipediaCategories ORDER BY article_id DESC LIMIT 10')
    print("CATEGORIES:")
    for row in cursor: 
        print(row)
    
    connection.close()

    return 1


def test1():
    """Check that there are articles in the WikipediaCategories table
    """
    assert CountArticlesInCategoriesTable() > 0, "Should be greater than 0. Latest was 13380222"

def test2():
    """Check that there are articles in the WikipediaCategories table
    """
    assert CountArticlesInCategoriesTable('wikipedia_database_jp.db') > 0, "Should be greater than 0. Latest was 13380222"


def main():
    PrintCategoriesTable()
    PrintCategoriesTable('wikipedia_database_jp.db')


if __name__ == "__main__":
    main()

