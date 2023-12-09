from load_tables import load_to_postgres, create_tables_psql

if __name__ == "__main__":
    try:
        create_tables_psql()
        load_to_postgres()
        print("Data loading completed.")
    except Exception as e:
        print(f'Failed to do the process. Error {e}')