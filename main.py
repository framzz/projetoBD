from load_tables import load_to_postgres, create_table_psql

if __name__ == "__main__":
    query_funcionario = '''
            CREATE TABLE IF NOT EXISTS funcionarios (
                id_func INT PRIMARY KEY,
                nome VARCHAR(100) NOT NULL,
                especialidade VARCHAR(100) NOT NULL,
                telefone VARCHAR(20) UNIQUE NOT NULL,
                status VARCHAR(20) CHECK (status IN ('Ativo', 'Inativo'))         
        );'''

    query_cliente = '''
            CREATE TABLE IF NOT EXISTS clientes (
                id_cliente INT PRIMARY KEY,
                nome VARCHAR(100) NOT NULL,
                endereco VARCHAR(200) NOT NULL,
                telefone VARCHAR(20) UNIQUE NOT NULL,
                email VARCHAR(100) UNIQUE
            );'''

    query_paciente = '''
            CREATE TABLE IF NOT EXISTS pacientes (
                id_paciente INT PRIMARY KEY,
                nome VARCHAR(100) NOT NULL,
                especie VARCHAR(50) NOT NULL,
                raca VARCHAR(50),
                peso DECIMAL(5,2) CHECK (peso > 0),
                id_cliente INT NOT NULL,
                FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente)
            );'''

    query_atendimento = '''
            CREATE TABLE IF NOT EXISTS atendimento (
                id_atendimento INT PRIMARY KEY,
                id_func INT NOT NULL,
                id_paciente INT NOT NULL,
                valor DECIMAL(7,2),
                data DATE,
                tipo_atendimento VARCHAR(100) NOT NULL,
                FOREIGN KEY (id_func) REFERENCES funcionarios(id_func),
                FOREIGN KEY (id_paciente) REFERENCES pacientes(id_paciente)
            );'''
    try:
        create_table_psql(query = query_funcionario)
        create_table_psql(query = query_cliente)
        create_table_psql(query = query_paciente)
        create_table_psql(query = query_atendimento)
        load_to_postgres()
        print("Data loading completed.")
    except Exception as e:
        print(f'Failed to do the process. Error {e}')