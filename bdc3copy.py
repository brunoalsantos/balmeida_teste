import requests
import json
import pymysql

#            print (f"{row[0]}")


class DatabaseConnection:
    
    def __init__(self):
        self.connection_bdc = pymysql.connect(
            host='db_pebmedapps.producao.pebmed.local'
            , user='producao'
            , passwd = 'QAdHmKWvDiPpxezhMP'
            , db='bdc_data')

        self.connection_pebmedapps = pymysql.connect(
            host='db_pebmedapps.producao.pebmed.local'
            , user='producao'
            , passwd = 'QAdHmKWvDiPpxezhMP'
            , db='pebmedapps')
                
        self.cursor_bdc = self.connection_bdc.cursor()
        self.cursor_pebmedapps = self.connection_pebmedapps.cursor()


    def execute_query(self):
        fetch_id_cpf = ("SELECT id, cpf FROM pebmedapps.tb_usuario WHERE DATE(data_cadastro) = CURRENT_DATE - 'INTERVAL 1 DAY' AND cpf IS NOT NULL  AND cpf <> '0000000000000' LIMIT 1")
        self.cursor_pebmedapps.execute(fetch_id_cpf)
        
        cpfs = self.cursor_pebmedapps.fetchall()

        datasets = ['basic_data','related_people', 'business_relationships', 'financial_data', 'online_presence', 'passages', 'interests_and_behaviors', 'demographic_data', 'flags_and_features', 'university_student_data', 'addresses', 'emails', 'occupation_data', 'online_ads', 'collections', 'class_organization']   

        api_token = 'd2b92070-dcb3-48e7-942a-bd5fa9a94452'
        api_url_base = 'https://bigboost.bigdatacorp.com.br/peoplev2?'
    
        for id, cpf in cpfs:
            
                for ds in datasets:
                    print (cpf)
                    print (ds)
                    payload = {'Datasets':ds, 'q=doc:':'{'+cpf+'}', 'AccessToken':api_token}
                    response = requests.get(api_url_base, params = payload).url.replace('%3D','=').replace('%3A=%7B','{').replace('%7D','}')
                    response_final = requests.get(response)
                    user_data = response_final.json()
                    print(response)
                    # -------

                    if ds == 'basic_data':
                        user_data2 = user_data['Result'][0]['BasicData']                       
                        insert_command = ("INSERT INTO bdc_data.people_dados_basicos ( id_pebmed, documento, `documento.1`, pais_de_origem, orgao_emissor, status_do_documento, pis, nome, nome_comum, nome_padronizado, unicidade_do_nome, unicidade_do_primeiro_nome, unicidade_do_primeiro_e_ultimo_nome, nome_da_mae, nome_do_pai, data_de_nascimento, idade, genero, signo_do_zodiaco, signo_do_calendario_chines, indicacao_de_obito, data_da_primeira_captura, data_da_ultima_captura ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " )
                        
                        # basic_data parameters
                        t = (id, str(user_data2["TaxIdNumber"]), str(user_data2["TaxIdNumber"]), str(user_data2["BirthCountry"]), str(user_data2['TaxIdOrigin'])  , str(user_data2['TaxIdStatus']), str(user_data2['AlternativeIdNumbers']['SocialSecurityNumber']), str(user_data2['Name']), str(user_data2['Aliases']['CommonName']), str(user_data2['Aliases']['StandardizedName']), str(user_data2['NameUniquenessScore']), str(user_data2['FirstNameUniquenessScore']), str(user_data2['FirstAndLastNameUniquenessScore']), str(user_data2['MotherName']), str(user_data2['FatherName']), str(user_data2['BirthDate']), str(user_data2['Age']), str(user_data2['Gender']), str(user_data2['ZodiacSign']), str(user_data2['ChineseSign']), str(user_data2['HasObitIndication']), str(user_data2['CreationDate']), str(user_data2['LastUpdateDate']) )
                        print(insert_command)
                        self.cursor_bdc.execute(insert_command, t)
                        self.connection_bdc.commit()
                    # -------

                    elif ds == 'related_people':
                        user_data2 = user_data['Result'][0]['RelatedPeople']                       
                        insert_command = ("INSERT INTO bdc_data.people_realacionamentos_pessoais_resumo (id_pebmed, documento, quantidade_de_relacionamentos, quantidade_de_parentes, quantidade_de_vizinhos, quantidade_de_conjuges, quantidade_de_colegas_de_trabalho, quantidade_de_pessoas_no_household, quantidade_de_socios) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ) " )

                        #related_people parameters
                        t = (id, cpf, str(user_data2['TotalRelationships']), str(user_data2['TotalRelatives']), str(user_data2['TotalNeighbors']),str(user_data2['TotalSpouses']),str(user_data2['TotalCoworkers']),str(user_data2['TotalHousehold']),str(user_data2['TotalPartners']) )
                        print(insert_command)
                        self.cursor_bdc.execute(insert_command, t)
                        self.connection_bdc.commit()    
                    # --------

                    elif ds == 'business_relationships':
                        user_data2 = user_data['Result'][0]['BusinessRelationships']  
                        max_index = ("SELECT MAX(indice) FROM bdc_data.people_relacionamentos_empresariais_detalhes") 
                        print(max_index)
                        self.cursor_bdc.execute(max_index)
                        print(type(x))
                        print(x)

                        insert_command = ("INSERT INTO bdc_data.people_relacionamentos_empresariais_detalhes (indice, id_pebmed, documento, documento_da_empresa_relacionada, tipo_do_documento_da_empresa_relacionada, pais_da_emrpesa_relacionada, nome_da_empresa_relacionada, tipo_do_relacionamento, nivel_do_relacionamento, data_de_inicio_de_relacionamento, data_de_termino_do_relacionamento) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) " )

                        #related_people parameters
                        t = (max_index+1, id, cpf, str(user_data2['BusinessRelationships']['RelatedEntityTaxIdNumber']), str(user_data2['BusinessRelationships']['RelatedEntityTaxIdType']), str(user_data2['BusinessRelationships']['RelatedEntityTaxIdCountry']), str(user_data2['BusinessRelationships']['RelatedEntityName']) ,str(user_data2['BusinessRelationships']['RelationshipType']), str(user_data2['BusinessRelationships']['RelationshipLevel']), str(user_data2['BusinessRelationships']['RelationshipStartDate']) ,str(user_data2['BusinessRelationships']['RelationshipEndDate']) )
                        print(insert_command)
                        self.cursor_bdc.execute(insert_command, t)
                        self.connection_bdc.commit()
                    

                    else:
                        print ("Nenhuma das opcoes")
                        break                  


                  


 
 
        #https://bigboost.bigdatacorp.com.br/peoplev2?Datasets=basic_data&q=doc{34288575850}&AccessToken=d2b92070-dcb3-48e7-942a-bd5fa9a94452


    def close_connections (self):
        self.connection_pebmedapps.close()
        self.connection_bdc.close()


if __name__ == '__main__':
    db_connection = DatabaseConnection()
    db_connection.execute_query()
    db_connection.close_connections() 