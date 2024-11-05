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
except Exception as e:
    print("Erro ao conectar ao banco SQL:", e)

# Conexão com o MongoDB
try:
    mongo_client = MongoClient(os.getenv('mongo_url'))
    mongo_db = mongo_client[str(os.getenv('mongo_db'))]
    forum_collection = mongo_db[str(os.getenv("mongo_forum"))]
    news_collection = mongo_db[str(os.getenv("mongo_news"))]
except Exception as e:
    print("Erro ao conectar ao MongoDB:", e)

# Função para transferir posts do fórum
def transfer_forum():
    try:
        sql_cursor.execute("SELECT id_post, user_id, content, post_date, code_repr FROM forum_post")
        posts = sql_cursor.fetchall()

        for post in posts:
            id_post, user_id, content, post_date, code_repr = post
            if not forum_collection.find_one({"post_id": id_post}):
                if id_post == code_repr:
                    forum_document = {
                        "post_id": id_post,
                        "user_id": user_id,
                        "content": content,
                        "post_date": post_date.isoformat(),
                        "comments": [],
                        "engager": []
                    }
                    forum_collection.insert_one(forum_document)
                    print("Post inserido:", forum_document)
                else:
                    existing_comment = forum_collection.find_one(
                        {"post_id": code_repr, "comments": {"$elemMatch": {"post_id": id_post, "user_id": user_id}}}
                    )
                    if not existing_comment:
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
                        print("Comentário atualizado:", comment, "Resultado:", result.modified_count)
    except Exception as e:
        print("Erro ao transferir posts:", e)

# Função para transferir artigos de notícias
def transfer_news():
    try:
        sql_cursor.execute("SELECT id_article, headline, news_url, source FROM informative_articles")
        articles = sql_cursor.fetchall()

        for article in articles:
            id_article, headline, news_url, source = article
            if not news_collection.find_one({"article_id": id_article}):
                news_document = {
                    "article_id": id_article,
                    "url": news_url,
                    "title": headline,
                    "description": source,
                    "date": datetime.now().isoformat(),
                    "engager": []
                }
                news_collection.insert_one(news_document)
                print("Artigo inserido:", news_document)
    except Exception as e:
        print("Erro ao transferir artigos:", e)

# Função para transferir curtidas dos usuários
def transfer_engagements():
    try:
        sql_cursor.execute("SELECT user_id, forum_post_id, article_id FROM pinned")
        pinned_entries = sql_cursor.fetchall()

        for entry in pinned_entries:
            user_id, forum_post_id, article_id = entry

            if forum_post_id:
                forum_collection.update_one(
                    {"post_id": forum_post_id},
                    {"$addToSet": {"engager": user_id}}
                )
                print(f"Engagement adicionado ao post do fórum {forum_post_id}: usuário {user_id}")

            if article_id:
                news_collection.update_one(
                    {"article_id": article_id},
                    {"$addToSet": {"engager": user_id}}
                )
                print(f"Engagement adicionado ao artigo {article_id}: usuário {user_id}")

    except Exception as e:
        print("Erro ao transferir curtidas:", e)

# Executando as funções de transferência
transfer_forum()
transfer_news()
transfer_engagements()

# Fechando as conexões
sql_cursor.close()
sql_conn.close()
mongo_client.close()
