from langchain.embeddings.sentence_transformer import SentenceTransformerEmbeddings
from langchain.vectorstores import FAISS


def ExecuteEmbeddingQuery(embedding_index_name = "wikipedia_embeddings_title_fat.db", embedding_function_name = "all-MiniLM-L6-v2", query="test"):
    """Execute a test query against an embedding DB
    """
    embedding_function = SentenceTransformerEmbeddings(model_name=embedding_function_name)
    #connection_vss = SQLiteVSS.create_connection(db_file=embedding_index_name)
    #db = SQLiteVSS(table="WikipediaTitleEmbeddings", embedding=embedding_function, connection=connection_vss)
    
    #data = db.similarity_search(query)

    db = FAISS.load_local(embedding_index_name, embedding_function)
    docs_and_scores = db.similarity_search_with_score(query)
    i=0
    for res in docs_and_scores:
        print(f'Result {i+1} for query {query} is:\n {docs_and_scores[i]}')
        i += 1

    #print(f'Results for query {query} are:\n {data[0].page_content}')

    #connection_vss.close()


def main():
    ExecuteEmbeddingQuery()
    ExecuteEmbeddingQuery(query='Benin')
    ExecuteEmbeddingQuery(embedding_index_name = "wikipedia_embeddings_title_jp.db", embedding_function_name = "pkshatech/GLuCoSE-base-ja", query="あんはさんと")

if __name__ == "__main__":
    main()

