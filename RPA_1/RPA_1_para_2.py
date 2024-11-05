import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

# Função para conectar ao banco de dados
def connect_to_db(conn_params):
    conn = psycopg2.connect(**conn_params)
    return conn

# Função para transferir dados entre as tabelas mapeadas
def transfer_data(table_mapping, conn_params_extracao, conn_params_insercao):
    # Conectando ao banco de origem e destino
    conn_extracao = connect_to_db(conn_params_extracao)
    conn_insercao = connect_to_db(conn_params_insercao)

    try:
        for table_origem, table_destino in table_mapping.items():
            with conn_extracao.cursor() as cursor_extracao, conn_insercao.cursor() as cursor_insercao:
                # Extraindo os dados da tabela de origem
                cursor_extracao.execute(f"SELECT * FROM {table_origem}")
                rows = cursor_extracao.fetchall()
                
                # Tratamento especial para cada tabela de acordo com o mapeamento de colunas
                if table_origem == "administrator" and table_destino == "admin":
                    for row in rows:
                        cursor_insercao.execute("SELECT 1 FROM admin WHERE pk_id = %s", (row[0],))
                        if cursor_insercao.fetchone() is None:
                            cursor_insercao.execute("""
                                INSERT INTO admin (pk_id, name, email, password)
                                VALUES (%s, %s, %s, %s)
                                """, (row[0], row[1], row[2], row[3]))

                elif table_origem == "categories":
                    for row in rows:
                        cursor_insercao.execute("SELECT 1 FROM categories WHERE pk_id = %s", (row[0],))
                        if cursor_insercao.fetchone() is None:
                            cursor_insercao.execute("""
                                INSERT INTO categories (pk_id, category, description)
                                VALUES (%s, %s, %s)
                                """, (row[0], row[1], row[2][:45]))  # Trunca para caber na coluna de destino

                elif table_origem == "boost_plan":
                    for row in rows:
                        cursor_insercao.execute("SELECT 1 FROM boost_plan WHERE pk_id = %s", (row[0],))
                        if cursor_insercao.fetchone() is None:
                            cursor_insercao.execute("""
                                INSERT INTO boost_plan (pk_id, name, description, price, duration_days)
                                VALUES (%s, %s, %s, %s, %s)
                                """, (row[0], row[1], row[2][:100], row[3], row[4]))  # Trunca descrição

                elif table_origem == "activate_plan":
                    for row in rows:
                        cursor_insercao.execute("SELECT 1 FROM activate_plan WHERE pk_id = %s", (row[0],))
                        if cursor_insercao.fetchone() is None:
                            cursor_insercao.execute("""
                                INSERT INTO activate_plan (pk_id, fk_product_id, fk_user_id, fk_plan_id, activation_date, deactivation_date, activation_status)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """, (row[0], row[1], row[2], row[3], row[4], row[5], row[6]))

        # Commit nas transações
        conn_insercao.commit()

    except Exception as e:
        print(f"Erro durante a transferência de dados: {e}")
        conn_insercao.rollback()
    finally:
        conn_extracao.close()
        conn_insercao.close()

# Parâmetros de conexão (origem e destino)
conn_params_extracao = {
    "dbname": os.getenv("dbname1"),
    "user": os.getenv("user1"),
    "host": os.getenv("host1"),
    "password": os.getenv("password1"),
    "port": os.getenv("port1")
}

conn_params_insercao = {
    "dbname": os.getenv("dbname2"),
    "user": os.getenv("user2"),
    "host": os.getenv("host2"),
    "password": os.getenv("password2"),
    "port": os.getenv("port2")
}

# Mapeamento de tabelas
table_mapping = {
    "administrator": "admin",
    "categories": "categories",
    "boost_plan": "boost_plan",
    "activate_plan": "activate_plan"
}

# Executar a transferência de dados
transfer_data(table_mapping, conn_params_extracao, conn_params_insercao)
