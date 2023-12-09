import psycopg2
import json

# Dados de conexão com o banco PostgreSQL
dbname='postgres'
user='postgres'
password='12345'
host='localhost'
port='5432'

path = 'data/'


def create_tables_psql(dbname: str = dbname,
                      user: str = user,
                      password: str = password,
                      host: str = host,
                      port: str = port):
    

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


    # Criação da tabela FUNCIONARIO no banco de dados (se não existir)
    with conn.cursor() as cur:
        cur.execute('''
        CREATE TABLE IF NOT EXISTS funcionarios (
            id_func INT PRIMARY KEY,
            nome VARCHAR(100) NOT NULL,
            especialidade VARCHAR(100) NOT NULL,
            telefone VARCHAR(20) UNIQUE NOT NULL,
            status VARCHAR(20) CHECK (status IN ('Ativo', 'Inativo'))         
        );
        ''')

    # Criação da tabela CLIENTE no banco de dados (se não existir)
    with conn.cursor() as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS clientes (
            id_cliente INT PRIMARY KEY,
            nome VARCHAR(100) NOT NULL,
            endereco VARCHAR(200) NOT NULL,
            telefone VARCHAR(20) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE
        );''')

    # Criação da tabela PACIENTE no banco de dados (se não existir)
    with conn.cursor() as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS pacientes (
            id_paciente INT PRIMARY KEY,
            nome VARCHAR(100) NOT NULL,
            especie VARCHAR(50) NOT NULL,
            raca VARCHAR(50),
            peso DECIMAL(5,2) CHECK (peso > 0),
            id_cliente INT NOT NULL,
            FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente)
        );''')

    # Criação da tabela ATENDIMENTO no banco de dados (se não existir)
    with conn.cursor() as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS atendimento (
            id_atendimento INT PRIMARY KEY,
            id_func INT NOT NULL,
            id_paciente INT NOT NULL,
            valor DECIMAL(7,2),
            data DATE,
            tipo_atendimento VARCHAR(100) NOT NULL,
            FOREIGN KEY (id_func) REFERENCES funcionarios(id_func),
            FOREIGN KEY (id_paciente) REFERENCES pacientes(id_paciente)
        );''')


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
                row.get('raca'),  # Pode ser nulo, então usamos row.get()
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
                    row.get('valor'),  # Pode ser nulo, então usamos row.get()
                    row.get('data'),   # Pode ser nulo, então usamos row.get()
                    row['tipo_atendimento']
                ))
                    
        # Comitar as mudanças
        conn.commit()
    except Exception as e:
        raise print(f'Not able to insert data in table {table}. Error {e}')
       
    conn.close()