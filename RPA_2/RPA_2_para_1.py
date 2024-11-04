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
        print("Iniciando a transferência de dados...")
        for table_origem, table_destino in table_mapping.items():
            with conn_extracao.cursor() as cursor_extracao, conn_insercao.cursor() as cursor_insercao:
                # Extraindo os dados da tabela de origem
                cursor_extracao.execute(f"SELECT * FROM {table_origem}")
                rows = cursor_extracao.fetchall()
                print(f"Transferindo dados de '{table_origem}' para '{table_destino}'...")

                if table_origem == "boost_plan" and table_destino == "boost_plan":
                    for row in rows:
                        cursor_insercao.execute(f"SELECT COUNT(1) FROM {table_destino} WHERE id_plan = %s", (row[0],))
                        exists = cursor_insercao.fetchone()[0]
                        
                        if not exists:
                            cursor_insercao.execute("""
                                INSERT INTO boost_plan (id_plan, name, description, price, duration_days)
                                VALUES (%s, %s, %s, %s, %s)
                                """, (row[0], row[1], row[2], row[3], row[4]))
                            print(f"Registro {row[0]} transferido com sucesso.")

        conn_insercao.commit()
        print("Transferência concluída com sucesso.")

    except Exception as e:
        print(f"Erro durante a transferência de dados: {e}")
        conn_insercao.rollback()
    finally:
        conn_extracao.close()
        conn_insercao.close()
        print("Conexões encerradas.")

# Parâmetros de conexão (origem e destino invertidos)
conn_params_extracao = {
    "dbname": os.getenv("dbname2"),
    "user": os.getenv("user2"),
    "host": os.getenv("host2"),
    "password": os.getenv("password2"),
    "port": os.getenv("port2")
}

conn_params_insercao = {
    "dbname": os.getenv("dbname1"),
    "user": os.getenv("user1"),
    "host": os.getenv("host1"),
    "password": os.getenv("password1"),
    "port": os.getenv("port1")
}

# Mapeamento de tabelas
table_mapping = {
    "boost_plan": "boost_plan"
}

# Executar a transferência de dados
transfer_data(table_mapping, conn_params_extracao, conn_params_insercao)

