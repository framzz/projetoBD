# Projeto Pet System
## Módulo Banco de Dados
Nesse módulo, criaremos uma conexão com um banco de dados PostgreSQL, com dados de uma clínica veterinária.

## Diretório

```bash
├── data
│   └── atendimento.json
│   └── clientes.json
│   └── funcionarios.json
│   └── pacientes.json
├── load_tables.py
├── main.py
├── README.md
├── requirements.txt
```


## Requirements
Os códigos foram desenvolvidos utilizando as seguintes configurações:

- Python 3.11
- pip 23.3.1
- PostgreSQL 16.0

## Executando projeto em um Virtual Enviroment (Windows):

- Crie um ambiente virtual no seu diretório: 
```
python -m venv env
 ```
- Ative o ambiente virtual: 
```
env\scripts\activate
 ```
- Instale as dependencias do projeto:
```
pip install -r requirements.txt
 ```
- Rode o arquivo `main.py`:
```
python main.py
```