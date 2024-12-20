import psycopg2
import time
import os
from dotenv import load_dotenv

load_dotenv()

def connect_to_database(dbname, user, password, host, port):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        cur = conn.cursor()
        print(f"Conectado ao banco {dbname}")
        return conn, cur
    except Exception as e:
        print(f"Erro ao conectar ao banco {dbname}: {e}")
        return None, None

def fetch_categoria(cur):
    cur.execute("SELECT ID, nome, descricao FROM Categoria_Produto")
    return cur.fetchall()

def insert_categoria(cur, nome):
    cur.execute("INSERT INTO Categoria (nome) VALUES (%s) RETURNING categoria_id", (nome,))
    return cur.fetchone()[0]

def update_categoria(conn_1, conn_2):
    cur_1 = conn_1.cursor()
    cur_2 = conn_2.cursor()

    categorias = fetch_categoria(cur_1)

    for categoria in categorias:
        id_categoria, nome, descricao = categoria
        
        cur_2.execute("SELECT categoria_id FROM Categoria WHERE nome = %s", (nome,))
        if cur_2.fetchone() is None:
            insert_categoria(cur_2, nome)

    conn_2.commit()
    print("Atualização da tabela Categoria concluída.")

def fetch_plano(cur):
    """Coleta dados da tabela Plano e retorna uma lista de planos."""
    cur.execute("SELECT ID, nome_plano, descricao, valor FROM Plano")
    return cur.fetchall()

def insert_plano(cur, nome, descricao, valor):
    cur.execute(
        "INSERT INTO Plano (nome, descricao, valor) VALUES (%s, %s, %s) RETURNING plano_id",
        (nome, descricao, valor)
    )
    return cur.fetchone()[0]

def update_plano(conn_1, conn_2):
    cur_1 = conn_1.cursor()
    cur_2 = conn_2.cursor()

    planos = fetch_plano(cur_1)

    for plano in planos:
        id_plano, nome, descricao, valor = plano
        
        cur_2.execute("SELECT plano_id FROM Plano WHERE nome = %s", (nome,))
        if cur_2.fetchone() is None:
            insert_plano(cur_2, nome, descricao, valor)

    conn_2.commit()
    print("Atualização da tabela Plano concluída.")


def fetch_tag_servico(cur):
    cur.execute("""SELECT ID, nome FROM tag_servico""")
    return cur.fetchall()

def insert_tag_servico(cur, nome):
    cur.execute("""INSERT INTO TagServico (nome) VALUES (%s)""", (nome,))  

def update_tag_servico(conn_1, conn_2):
    cur_1 = conn_1.cursor()
    cur_2 = conn_2.cursor()

    tag_servicos = fetch_tag_servico(cur_1)
    
    for tag_servico_id, nome in tag_servicos:
        cur_2.execute("""SELECT * FROM TagServico WHERE tag_servico_id = %s""", (tag_servico_id,)) 
        if cur_2.fetchone() is None:
            insert_tag_servico(cur_2, nome)

    conn_2.commit()
    print("Atualização da tabela TagServico concluída.")


def fetch_pagamento_plano(cur):
    cur.execute("""SELECT usuario_id, plano_id FROM Pagamento_Plano""")
    return cur.fetchall()

def insert_plano_ativacao(cur, usuario_id, plano_id):
    cur.execute("""SELECT * FROM Plano_Ativacao WHERE ID_usuario_ativacao = %s AND ID_plano = %s""", (usuario_id, plano_id))
    if cur.fetchone() is None:
        cur.execute("""INSERT INTO Plano_Ativacao (ID_usuario_ativacao, ID_plano) VALUES (%s, %s)""", (usuario_id, plano_id))  

def insert_plano_usuario(cur, usuario_id, plano_id, data_assinatura, data_final):
    cur.execute("""SELECT * FROM Plano_Usuario WHERE usuario_id = %s AND plano_id = %s""", (usuario_id, plano_id))
    if cur.fetchone() is None:
        cur.execute("""INSERT INTO Plano_Usuario (usuario_id, plano_id, data_assinatura, data_final) VALUES (%s, %s, %s, %s)""", (usuario_id, plano_id, data_assinatura, data_final))

def update_plano_usuario(conn_1, conn_2):
    cur_1 = conn_1.cursor()
    cur_2 = conn_2.cursor()

    pagamentos = fetch_pagamento_plano(cur_2)
    
    for usuario_id, plano_id in pagamentos:
        insert_plano_ativacao(cur_1, usuario_id, plano_id)

    conn_1.commit()
    print("Dados inseridos na tabela Plano_Ativacao.")

    cur_1.execute("""SELECT ID_usuario_ativacao, ID_plano, data_assinatura, data_final, ativacao FROM Plano_Ativacao""")
    planos_ativacao = cur_1.fetchall()

    for usuario_id_ativacao, plano_id, data_assinatura, data_final, ativacao in planos_ativacao:
        if ativacao == 'A':
            insert_plano_usuario(cur_2, usuario_id_ativacao, plano_id, data_assinatura, data_final)

    conn_2.commit()
    print("Atualização da tabela Plano_Usuario concluída.")


def main():
    start_time = time.time()   
    
    conn_1, cur_1 = connect_to_database(
        dbname=os.getenv('DB1_NAME'), 
        user=os.getenv('DB_USER'), 
        password=os.getenv('DB_PASSWORD'), 
        host=os.getenv('DB_HOST'), 
        port=os.getenv('DB_PORT')
    )

    if conn_1 is None:
        return

    conn_2, cur_2 = connect_to_database(
        dbname=os.getenv('DB2_NORMALIZADO'), 
        user=os.getenv('DB_USER'), 
        password=os.getenv('DB_PASSWORD'), 
        host=os.getenv('DB_HOST'), 
        port=os.getenv('DB_PORT')
    )

    if conn_2 is None:
        return
    
    update_categoria(conn_1, conn_2)
    time.sleep(5) 
    update_tag_servico(conn_1, conn_2)
    time.sleep(5) 
    update_plano(conn_1, conn_2)
    time.sleep(5) 
    update_plano_usuario(conn_1, conn_2)
    time.sleep(5)
    
    desconectar_banco(cur_1, cur_2, conn_1, conn_2)
    
    end_time = time.time() 
    elapsed_time = end_time - start_time  
    print(f"O RPA levou {elapsed_time:.2f} segundos para atualizar as tabelas.")    
    
def desconectar_banco(cur_1, cur_2, conn_1, conn_2):
    try:
        cur_1.close()
        cur_2.close()
        conn_1.close()
        conn_2.close()
        print("Desconectado dos bancos de dados.")
    except Exception as e:
        print(f"Erro ao desconectar do banco de dados: {e}")

if __name__ == "__main__":
    main()