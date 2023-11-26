from langchain.embeddings.sentence_transformer import SentenceTransformerEmbeddings
from langchain.vectorstores import FAISS
import sqlite3


#database_name = "wikipedia_database_fat.db"
#embedding_index_name = "wikipedia_embeddings_title_fat.db"
#embedding_function_name = "all-MiniLM-L6-v2"
embedding_index_name = "wikipedia_embeddings_title_jp.db"
database_name = "wikipedia_database_jp.db"
embedding_function_name = "pkshatech/GLuCoSE-base-ja"


def main():

    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()
    
    cursor.execute('''
        SELECT * FROM WikipediaTitles WHERE article_id <= 10000
    ''')

    embedding_function = SentenceTransformerEmbeddings(model_name=embedding_function_name)
    db = FAISS.from_texts("test", embedding_function)
    #connection_vss = SQLiteVSS.create_connection(db_file=embedding_index_name)
    #db = SQLiteVSS(table="WikipediaTitleEmbeddings", embedding=embedding_function, connection=connection_vss)
    
    for row in cursor:
        db.add_texts([row[1]])

    db.save_local(embedding_index_name)

    #connection_vss.commit()
    #connection_vss.close()
    connection.commit()
    connection.close()    


if __name__ == "__main__":
    main()
