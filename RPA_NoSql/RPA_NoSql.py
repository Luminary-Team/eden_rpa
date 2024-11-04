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
try:
    sql_conn = psycopg2.connect(**conn_params_extracao)
    sql_cursor = sql_conn.cursor()
    # print("Conexão com SQL estabelecida.")
except Exception as e:
    print("Erro ao conectar ao banco SQL:", e)

# Conexão com o MongoDB
try:
    mongo_client = MongoClient(os.getenv('mongo_url'))
    mongo_db = mongo_client[str(os.getenv('mongo_db'))]
    forum_collection = mongo_db[str(os.getenv("mongo_forum"))]
    news_collection = mongo_db[str(os.getenv("mongo_news"))]
    # print("Conexão com MongoDB estabelecida.")
except Exception as e:
    print("Erro ao conectar ao MongoDB:", e)

def transfer_forum():
    try:
        sql_cursor.execute("SELECT id_post, user_id, content, post_date, code_repr FROM forum_post")
        posts = sql_cursor.fetchall()
        # print("Total de posts encontrados:", len(posts))

        for post in posts:
            id_post, user_id, content, post_date, code_repr = post
            if not forum_collection.find_one({"post_id": id_post}):
                if id_post == code_repr:
                    forum_document = {
                        "post_id": id_post,
                        "user_id": user_id,
                        "content": content,
                        "post_date": post_date.isoformat(),
                        "comments": []
                    }
                    forum_collection.insert_one(forum_document)
                    print("Post inserido:", forum_document)
                else:
                    comment = {
                        "post_id": id_post,
                        "user_id": user_id,
                        "content": content,
                        "post_date": post_date.isoformat()
                    }
                    result = forum_collection.update_one(
                        {"post_id": code_repr},
                        {"$push": {"comments": comment}}
                    )
                    # print("Comentário atualizado:", comment, "Resultado:", result.modified_count)
    except Exception as e:
        print("Erro ao transferir posts:", e)

def transfer_news():
    try:
        sql_cursor.execute("SELECT id_article, headline, news_url, source FROM informative_articles")
        articles = sql_cursor.fetchall()
        # print("Total de artigos encontrados:", len(articles))

        for article in articles:
            id_article, headline, news_url, source = article
            if not news_collection.find_one({"post_id": id_article}):
                news_document = {
                    "post_id": id_article,
                    "url": news_url,
                    "title": headline,
                    "description": source,
                    "date": datetime.now().isoformat()
                }
                news_collection.insert_one(news_document)
                # print("Artigo inserido:", news_document)
    except Exception as e:
        print("Erro ao transferir artigos:", e)

# Executando as funções de transferência
transfer_forum()
transfer_news()

# Fechando as conexões
sql_cursor.close()
sql_conn.close()
mongo_client.close()
