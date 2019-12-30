import requests
import json
import psycopg2

class DatabaseConnection:
    def __enter__(self):
        # Aqui Bruno sempre que vc declarar um objeto da sua classe usando With isso vai ser executado, garantindo que na
        # entrada vai ter uma conexão válida e um cursor pronto e válido.
        with psycopg2.connect(host='xxxx', dbname= 'snowplow', port= '5439', user= 'bruno_almeida', password= 'xxx') as conn:
            conn.autocommit = True
            self.connection = conn
            with self.connection.cursor() as curs:
                self.cursor = curs
     
    def __exit__(self):
        self.cursor.close()
        self.connection.close()

    def execute_query(self):
        execute_query_command = ("SELECT id, app_codigo FROM pebmedapps.tb_usuario LIMIT 10;")
        self.cursor.execute(execute_query_command)
        rows = self.cursor.fetchall()
        print (f"id    | app_codigo ")
        print (f"------ ----------- ")
        for row in rows:
            print (f"{row[0]}        {row[1]} ")

    def insert_record(self):
        new_record = ("10", "6")
        insert_command = ("INSERT INTO tablexxx (col1, col2) VALUES ('" + new_record[0] + "','" + new_record[1] + "') ")
        print(insert_command)
        self.cursor.execute(insert_command) 


if __name__ == '__main__':
    # Quando entra no trecho identado no with é sinal de que seu enter executou com sucesso.
    # ao sair do escopo aninhado do with, então o __exit__é chamado fechando cursor e conexão.
    # verifica se seus execute e insert estão certinhos Bruno mas é basicamente isso para 
    # garantir em Python que nada fica aberto.
    with DatabaseConnection() as conn:
        conn.execute_query()
