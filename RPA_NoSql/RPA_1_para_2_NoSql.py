import psycopg2
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# Parâmetros de conexão com o banco SQL
conn_params_extracao = {
    "dbname": os.getenv("dbname1"),
    "user": os.getenv("user1"),
    "host": os.getenv("host1"),
    "password": os.getenv("password1"),
    "port": os.getenv("port1")
}

# Conexão com o banco SQL
sql_conn = psycopg2.connect(**conn_params_extracao)
sql_cursor = sql_conn.cursor()

# Conexão com o MongoDB
mongo_client = MongoClient(os.getenv('mongodb_url'))
mongo_db = mongo_client[os.getenv('mongodb_db')]
forum_collection = mongo_db[os.getenv("mongo_forum")]
news_collection = mongo_db[os.getenv("mongo_news")]

def transfer_forum():
    # Selecionando todos os registros da tabela `forum_post`
    sql_cursor.execute("SELECT id_post, user_id, content, post_date, code_repr FROM forum_post")
    posts = sql_cursor.fetchall()

    for post in posts:
        id_post, user_id, content, post_date, code_repr = post

        # Verifica se o `post_id` já existe no MongoDB para evitar duplicação
        if not forum_collection.find_one({"post_id": id_post}):
            if id_post == code_repr:
                # Criando um novo post com `post_id`
                forum_document = {
                    "post_id": id_post,  # Usando `post_id` no MongoDB
                    "user_id": user_id,
                    "content": content,
                    "post_date": post_date.isoformat(),  # Convertendo data para string
                    "comments": []  # Lista vazia para comentários
                }
                # Inserindo o post no MongoDB
                forum_collection.insert_one(forum_document)
            
            else:
                # Encontrando o post correto no MongoDB pelo `code_repr` e adicionando um comentário
                comment = {
                    "post_id": id_post,
                    "user_id": user_id,
                    "content": content,
                    "post_date": post_date.isoformat()
                }
                forum_collection.update_one(
                    {"post_id": code_repr},
                    {"$push": {"comments": comment}}
                )

def transfer_news():
    # Selecionando todos os registros da tabela `informative_articles`
    sql_cursor.execute("SELECT id_article, headline, news_url, source FROM informative_articles")
    articles = sql_cursor.fetchall()

    for article in articles:
        id_article, headline, news_url, source = article

        # Verifica se o `id_article` já existe no MongoDB para evitar duplicação
        if not news_collection.find_one({"post_id": id_article}):
            # Montando o documento para o MongoDB com `post_id`
            news_document = {
                "post_id": id_article,  # Usando `post_id` no MongoDB
                "url": news_url,
                "title": headline,
                "description": source,
                "date": datetime.now().isoformat()  # Data atual como exemplo
            }
            # Inserindo o artigo no MongoDB
            news_collection.insert_one(news_document)

# Executando as funções de transferência
transfer_forum()
transfer_news()

# Fechando as conexões
sql_cursor.close()
sql_conn.close()
mongo_client.close()
