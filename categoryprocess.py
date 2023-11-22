
import sqlite3

#categories_source = "wikipedia_dump/fatwiki-20231101-categorylinks.sql"
#database_name = "wikipedia_database_fat.db"
categories_source = "wikipedia_dump/jawiki-20231101-categorylinks.sql"
database_name = "wikipedia_database_jp.db"


def read_category_file(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        while True:
            line = file.readline()
            if not line:
                break
            yield line.rstrip('\n')

def parse_query(query:str):
    insertValues = []
    tuples = query[36:].split("),(")
    for tuple in tuples:
        values = tuple.split(",")
        insertValues.append((int(values[0]),str(values[1])))
    #for val in insertValues:
    #    print(val)
    return insertValues

def main():

    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()
    
    cursor.execute('''
        DROP TABLE WikipediaCategories;
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS WikipediaCategories (
            article_id INTEGER,
            category_name TEXT
        )
    ''')

    for line in read_category_file(categories_source):
        if line.startswith("INSERT"):
            insertValues = parse_query(line)
            for val in insertValues:
                cursor.execute('INSERT INTO WikipediaCategories VALUES (?, ?)', val)
    
    connection.commit()
    connection.close()    


if __name__ == "__main__":
    main()
