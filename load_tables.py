import psycopg2
import json

# Dados de conexão com o banco PostgreSQL
dbname='postgres'
user='postgres'
password='12345'
host='localhost'
port='5432'

path = 'data/'

# função para criar tabela
def create_table_psql(dbname: str = dbname,
                      user: str = user,
                      password: str = password,
                      host: str = host,
                      port: str = port,
                      query: str = None):
    
    # conexão com o banco de dados PostgreSQL
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
    except Exception as e:
        raise print(f'Not able to make connection with PostgreSQL. Error {e}')

    # Criação da tabela no banco de dados
    with conn.cursor() as cur:
        cur.execute(query)

    conn.commit()
    conn.close()


def load_to_postgres(dbname: str = dbname,
                      user: str = user,
                      password: str = password,
                      host: str = host,
                      port: str = port,
                      path: str = path):

    # Conectar ao PostgreSQL
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
    except Exception as e:
        raise print(f'Not able to make connection with PostgreSQL. Error {e}')
    
    # Leitura dos dados do arquivo json de funcionarios e inserção no banco de dados
    try:
        table = 'funcionarios'
        with open(f'{path}/{table}.json', "r", encoding="utf-8") as json_file:
            data = json.load(json_file)

        with conn.cursor() as cur:
                for row in data:
                    cur.execute('''
                        INSERT INTO funcionarios (
                            id_func,
                            nome,
                            especialidade,
                            telefone,
                            status
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    ''', (
                        row['id_func'],
                        row['nome'],
                        row['especialidade'],
                        row['telefone'],
                        row['status']
                    ))

        # Comitar as mudanças
        conn.commit()
    except Exception as e:
        raise print(f'Not able to insert data in table {table}. Error {e}')
    
    # Leitura dos dados do arquivo json de clientes e inserção no banco de dados
    try:
        table = 'clientes'
        with open(f'{path}/{table}.json', "r", encoding="utf-8") as json_file:
            data = json.load(json_file)

        with conn.cursor() as cur:
                for row in data:
                    cur.execute('''
                        INSERT INTO clientes (
                            id_cliente,
                            nome,
                            endereco,
                            telefone,
                            email
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    ''', (
                        row['id_cliente'],
                        row['nome'],
                        row['endereco'],
                        row['telefone'],
                        row['email']
                    ))
        # Comitar as mudanças
        conn.commit()
    except Exception as e:
        raise print(f'Not able to insert data in table {table}. Error {e}')  
    

    # Leitura dos dados do arquivo json de pacientes e inserção no banco de dados
    try:
        table = 'pacientes'
        with open(f'{path}/{table}.json', "r", encoding="utf-8") as json_file:
            data = json.load(json_file)

        with conn.cursor() as cur:
                for row in data:
                    cur.execute('''INSERT INTO pacientes (
                    id_paciente,
                    nome,
                    especie,
                    raca,
                    peso,
                    id_cliente
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            ''', (
                row['id_paciente'],
                row['nome'],
                row['especie'],
                row.get('raca'),
                row['peso'],
                row['id_cliente']
            ))
                    
        # Comitar as mudanças
        conn.commit()
    except Exception as e:
        raise print(f'Not able to insert data in table {table}. Error {e}')
    
    # Leitura dos dados do arquivo json de atendimento e inserção no banco de dados
    try:
        table = 'atendimento'
        with open(f'{path}/{table}.json', "r", encoding="utf-8") as json_file:
            data = json.load(json_file)

        with conn.cursor() as cur:
                for row in data:
                    cur.execute('''
                        INSERT INTO atendimento (
                            id_atendimento,
                            id_func,
                            id_paciente,
                            valor,
                            data,
                            tipo_atendimento
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                ''', (
                    row['id_atendimento'],
                    row['id_func'],
                    row['id_paciente'],
                    row.get('valor'),
                    row.get('data'),   
                    row['tipo_atendimento']
                ))
                    
        # Comitar as mudanças
        conn.commit()
    except Exception as e:
        raise print(f'Not able to insert data in table {table}. Error {e}')
       
    conn.close()