import requests
import json
import MySQLdb

#            print (f"{row[0]}")


class DatabaseConnection:
    
    def __init__(self):
        self.connection_bdc = MySQLdb.connect(
            host='db_pebmedapps.producao.pebmed.local'
            , user='producao'
            , passwd = 'QAdHmKWvDiPpxezhMP'
            , db='bdc_data')

        self.connection_pebmedapps = MySQLdb.connect(
            host='db_pebmedapps.producao.pebmed.local'
            , user='producao'
            , passwd = 'QAdHmKWvDiPpxezhMP'
            , db='pebmedapps')
                
        self.cursor_bdc = self.connection_bdc.cursor()
        self.cursor_pebmedapps = self.connection_pebmedapps.cursor()


    def execute_query(self):
        fetch_id_cpf = ("SELECT id, cpf FROM pebmedapps.tb_usuario WHERE DATE(data_cadastro) = CURRENT_DATE - 'INTERVAL 1 DAY' AND cpf IS NOT NULL LIMIT 1")
        self.cursor_pebmedapps.execute(fetch_id_cpf)
        
        cpfs = self.cursor_pebmedapps.fetchall()
        print(cpfs)
        datasets = ['basic_data','occupation_data']    
        api_token = 'd2b92070-dcb3-48e7-942a-bd5fa9a94452'
        api_url_base = 'https://bigboost.bigdatacorp.com.br/peoplev2?'
    
        for id, cpf in cpfs:
            
                for ds in datasets:
                    print (cpf)
                    print (ds)
                    payload = {'Datasets':ds, 'q=doc:':'{'+cpf+'}', 'AccessToken':api_token}
                    response = requests.get(api_url_base, params = payload).url.replace('%3D','=').replace('%3A=%7B','{').replace('%7D','}')
                    response_final = requests.get(response)
                    print(response)
                    data = response_final.text
                    parsed = json.loads(data)
                    #print(json.dumps(parsed, indent=4))
                    collections_to_insert = []
                    #list_order = ["TaxIdNumber", "TaxIdNumber", "BirthCountry", "TaxIdOrigin", "TaxIdStatus", "SocialSecurityNumber", "Name", "CommonName", "StandardizedName", "NameUniquenessScore", "FirstNameUniquenessScore", "FirstAndLastNameUniquenessScore", "MotherName", "FatherName", "BirthDate", "Age", "Gender", "ZodiacSign", "ChineseSign", "HasObitIndication", "CreationDate", "LastUpdateDate"]

                    for record in parsed ['Result']:
                        data_to_insert = []
                        for k, v in record.items():
                            data_to_insert.append(v)

                        collections_to_insert.append(tuple(data_to_insert))
                        print (data_to_insert)


                    self.cursor_bdc.commit() 
                  


 
 
        #https://bigboost.bigdatacorp.com.br/peoplev2?Datasets=basic_data&q=doc{34288575850}&AccessToken=d2b92070-dcb3-48e7-942a-bd5fa9a94452


    def close_connections (self):
        self.cursor_pebmedapps.close()
        self.cursor_bdc.close()
        self.connection_pebmedapps.close()
        self.connection_bdc.close()
        
#    def insert_record(self):
#        new_record = ("10", "6")
#        insert_command = ("INSERT INTO tablexxx (col1, col2) VALUES ('" + new_record[0] + "','" + new_record[1] + "') ")
#        print(insert_command)
#        self.cursor.execute(insert_command) 


if __name__ == '__main__':
    db_connection = DatabaseConnection()
    db_connection.execute_query()
    db_connection.close_connections() 