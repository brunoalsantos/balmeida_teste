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
        #fetch_id_cpf = ("SELECT id, cpf FROM pebmedapps.tb_usuario WHERE DATE(data_cadastro) = CURRENT_DATE - 'INTERVAL 1 DAY' AND cpf IS NOT NULL  AND cpf <> '0000000000000' LIMIT 1")
        fetch_id_cpf = ("SELECT id, cpf FROM pebmedapps.tb_usuario WHERE cpf = '00067446132' LIMIT 1")
        
        self.cursor_pebmedapps.execute(fetch_id_cpf)
        
        cpfs = self.cursor_pebmedapps.fetchall()

        #datasets = ['basic_data','related_people', 'business_relationships', 'financial_data', 'online_presence', 'passages', 'interests_and_behaviors', 'demographic_data', 'flags_and_features', 'university_student_data', 'addresses', 'emails', 'occupation_data', 'online_ads', 'collections', 'class_organization'] 
        datasets = ['basic_data', 'addresses' ]   


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
                        print(user_data2)
               
                        insert_command = ("INSERT INTO bdc_data.people_dados_basicos ( id_pebmed, documento, `documento.1`, pais_de_origem, orgao_emissor, status_do_documento, pis, nome, nome_comum, nome_padronizado, unicidade_do_nome, unicidade_do_primeiro_nome, unicidade_do_primeiro_e_ultimo_nome, nome_da_mae, nome_do_pai, data_de_nascimento, idade, genero, signo_do_zodiaco, signo_do_calendario_chines, indicacao_de_obito, data_da_primeira_captura, data_da_ultima_captura ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " )
                        
                        # basic_data parameters
                        t = (
                            id
                            , str(user_data2['TaxIdNumber'])
                            , str(user_data2['TaxIdNumber'])
                            , str(user_data2['BirthCountry'])
                            , str(user_data2['TaxIdOrigin'])
                            , str(user_data2['TaxIdStatus'])
                            , str(user_data2['AlternativeIdNumbers']['SocialSecurityNumber'])
                            , str(user_data2['Name'])
                            , str(user_data2['Aliases']['CommonName'])
                            , str(user_data2['Aliases']['StandardizedName'])
                            , str(user_data2['NameUniquenessScore'])
                            , str(user_data2['FirstNameUniquenessScore'])
                            , str(user_data2['FirstAndLastNameUniquenessScore'])
                            , str(user_data2['MotherName'])
                            , str(user_data2['FatherName'])
                            , str(user_data2['BirthDate'])
                            , str(user_data2['Age'])
                            , str(user_data2['Gender'])
                            , str(user_data2['ZodiacSign'])
                            , str(user_data2['ChineseSign'])
                            , str(user_data2['HasObitIndication'])
                            , str(user_data2['CreationDate'])
                            , str(user_data2['LastUpdateDate']) )

                        print(insert_command)
                        self.cursor_bdc.execute(insert_command, t)
                        self.connection_bdc.commit()
                        print()

                    # -------

                    elif ds == 'related_people':
                        user_data2 = user_data['Result'][0]['RelatedPeople']   
                        print(user_data2)
                    
                        insert_command = ("INSERT INTO bdc_data.people_realacionamentos_pessoais_resumo (id_pebmed, documento, quantidade_de_relacionamentos, quantidade_de_parentes, quantidade_de_vizinhos, quantidade_de_conjuges, quantidade_de_colegas_de_trabalho, quantidade_de_pessoas_no_household, quantidade_de_socios) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ) " )

                        #related_people parameters
                        t = (
                            id
                            , cpf
                            , str(user_data2['TotalRelationships'])
                            , str(user_data2['TotalRelatives'])
                            , str(user_data2['TotalNeighbors'])
                            , str(user_data2['TotalSpouses'])
                            , str(user_data2['TotalCoworkers'])
                            , str(user_data2['TotalHousehold'])
                            , str(user_data2['TotalPartners']) )
                        
                        print(insert_command)
                        self.cursor_bdc.execute(insert_command, t)
                        self.connection_bdc.commit()   
                        print()
 
                    # --------

                    elif ds == 'business_relationships':
                        user_data2 = user_data['Result'][0]['BusinessRelationships']['BusinessRelationships'] 
                        print(user_data2)
                        max_index = ("SELECT MAX(indice) FROM bdc_data.people_relacionamentos_empresariais_detalhes") 
                        self.cursor_bdc.execute(max_index)
                        max_index = self.cursor_bdc.fetchone()[0]
                        print(max_index)

                        for row in user_data2:
                            max_index = max_index + 1
                            print(max_index)

                            insert_command = ("INSERT INTO bdc_data.people_relacionamentos_empresariais_detalhes (indice, id_pebmed, documento, documento_da_empresa_relacionada, tipo_do_documento_da_empresa_relacionada, pais_da_emrpesa_relacionada, nome_da_empresa_relacionada, tipo_do_relacionamento, nivel_do_relacionamento, data_de_inicio_de_relacionamento, data_de_termino_do_relacionamento) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) " )

                            #business_relationships parameters
                            t = (
                                max_index
                                , id
                                , cpf
                                , str(row['RelatedEntityTaxIdNumber'])
                                , str(row['RelatedEntityTaxIdType'])
                                , str(row['RelatedEntityTaxIdCountry'])
                                , str(row['RelatedEntityName'])
                                , str(row['RelationshipType'])
                                , str(row['RelationshipLevel'])
                                , str(row['RelationshipStartDate'])
                                , str(row['RelationshipEndDate']) )

                            print(insert_command)
                            self.cursor_bdc.execute(insert_command, t)
                            self.connection_bdc.commit()
                            print()
                    # --------

                    elif ds == 'financial_data':
                        user_data2 = user_data['Result'][0]['FinantialData']   
                        print(user_data2)
                    
                        insert_command = ("INSERT INTO bdc_data.people_dados_financeiros (id_pebmed, documento, renda_estimada_ibge, renda_estimada_mte, renda_estimada_bigdata_copr, renda_estimada_bigdata_corp_Empresas, patrimonio_estimado) VALUES (%s, %s, %s, %s, %s, %s, %s) " )

                        # FinantialData parameters
                        t = (
                            id
                            , cpf
                            , str(user_data2['IncomeEstimates']['IBGE'])
                            , str(user_data2['IncomeEstimates']['MTE'])
                            , str(user_data2['IncomeEstimates']['BIGDATA'])
                            , str(user_data2['IncomeEstimates']['COMPANY OWNERSHIP'])
                            , str(user_data2['TotalAssets']) )

                        print(insert_command)
                        self.cursor_bdc.execute(insert_command, t)
                        self.connection_bdc.commit()   
                        print()
 
                    # --------                   
 
                    elif ds == 'online_presence':
                        user_data2 = user_data['Result'][0]['OnlinePresence']   
                        print(user_data2)
                    
                        insert_command = ("INSERT INTO bdc_data.people_presenca_online (id_pebmed, documento, e_shopper, e_seller, nivel_de_utilizacao_da_internet, passagens_na_web) VALUES (%s, %s, %s, %s, %s, %s) " )

                        # OnlinePresence parameters
                        t = (
                            id
                            , cpf
                            , str(user_data2['Eshopper'])
                            , str(user_data2['Eseller'])
                            , str(user_data2['InternetUsageLevel'])
                            , str(user_data2['WebPassages']) )

                        print(insert_command)
                        self.cursor_bdc.execute(insert_command, t)
                        self.connection_bdc.commit()   
                        print()
                    # --------    
                    
                    elif ds == 'passages':
                        user_data2 = user_data['Result'][0]['Passages']   
                        print(user_data2)
                    
                        insert_command = ("INSERT INTO bdc_data.people_passagens (id_pebmed, documento, passagens, passagens_suspeitas, passagens_de_consulta, passagens_de_captura, passagens_de_validacao, media_de_passagens_por_mes, data_da_primeira_passagem, data_da_ultima_passagem, quantidade_de_enderecos, quantidade_de_telefones, `Quantidade de Emails`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " )

                        # Passages parameters
                        t = (id, cpf, str(user_data2['TotalPassages']), str(user_data2['BadPassages']), str(user_data2['QueryPassages']), str(user_data2['CrawlingPassages'] ), str(user_data2['ValidationPassages'] ), str(user_data2['MonthAveragePassages'] ), str(user_data2['FirstPassageDate']), str(user_data2['LastPassageDate']), str(user_data2['NumberOfAddresses'] ), str(user_data2['NumberOfPhones'] ), str(user_data2['NumberOfEmails'] ) )
                        print(insert_command)
                        self.cursor_bdc.execute(insert_command, t)
                        self.connection_bdc.commit()   
                        print() 
                    # --------    


                    elif ds == 'interests_and_behaviors':
                        user_data2 = user_data['Result'][0]['InterestsAndBehaviors']                       
                        insert_command = ("INSERT INTO bdc_data.people_interesses_comportamentais ( id_pebmed, documento, revendedor_porta_porta, nivel_de_utlizacao_cartao_de_credito, programas_de_milhagem, investidor_online, carros_e_motos, bebes_e_criancas, saude_e_beleza, comida_e_bebida, musculacao_e_suplementos, livros, computacao, entregas, eletro_eletronicos, entretenimento, moda_e_acessorios, videogames, casa_e_decoracao, itens_domesticos, telefonia_e_celulares, insturmentos_musicais, servicos, esporte_e_lazer, streaming, brinquedos, viagem_e_turismo ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " )
                        
                        # interests_and_behaviors parameters
                        t = (
                            id
                            , cpf
                            , str(user_data2['Behaviors']['ProductReseller'])
                            , str(user_data2['Behaviors']['CreditCardScore'])
                            , str(user_data2['Behaviors']['MilesProgram'])
                            , str(user_data2['Behaviors']['OnlineInvestor'])
                            , str(user_data2['CategoriesOfInterest']['Automotive'])
                            , str(user_data2['CategoriesOfInterest']['Baby'])
                            , str(user_data2['CategoriesOfInterest']['BeautyAndHealth'])
                            , str(user_data2['CategoriesOfInterest']['BeverageAndFood'])
                            , str(user_data2['CategoriesOfInterest']['Bodybuilding'])
                            , str(user_data2['CategoriesOfInterest']['Books'])
                            , str(user_data2['CategoriesOfInterest']['Computing'])
                            , str(user_data2['CategoriesOfInterest']['Delivery'])
                            , str(user_data2['CategoriesOfInterest']['Eletronics'])
                            , str(user_data2['CategoriesOfInterest']['Entertainment'])
                            , str(user_data2['CategoriesOfInterest']['FashionAndAcessories'])
                            , str(user_data2['CategoriesOfInterest']['Games'])
                            , str(user_data2['CategoriesOfInterest']['HomeAndDecoration'])
                            , str(user_data2['CategoriesOfInterest']['Housewares'])
                            , str(user_data2['CategoriesOfInterest']['MobileTelco'])
                            , str(user_data2['CategoriesOfInterest']['MusicalInstruments'])
                            , str(user_data2['CategoriesOfInterest']['Service'])
                            , str(user_data2['CategoriesOfInterest']['SportAndLeisure'])
                            , str(user_data2['CategoriesOfInterest']['Streaming'])
                            , str(user_data2['CategoriesOfInterest']['Toys'])
                            , str(user_data2['CategoriesOfInterest']['TravelAndTourism']) )

                        print(insert_command)
                        self.cursor_bdc.execute(insert_command, t)
                        self.connection_bdc.commit()
                        print()
                    # --------    

                    elif ds == 'demographic_data':
                        user_data2 = user_data['Result'][0]['DemographicData']
                        print(user_data2)

                        for row in user_data2:
                            insert_command = ("INSERT INTO bdc_data.people_dados_demograficos (id_pebmed, Documento, origem, nivel_de_agregacao, pais, renda_estimada, nivel_de_escolaridade_estimado, classe_social) VALUES (%s, %s, %s, %s, %s, %s, %s, %s ) " )

                            # demographic_data parameters
                            t = (
                                id
                                , cpf
                                , str(row['DataOrigin'])
                                , str(row['DataAgregationLevel'])
                                , str(row['DataCountry'])
                                , str(row['EstimatedIncomeRange'])
                                , str(row['EstimatedInstructionLevel'])
                                , str(row['SocialClass']) )

                            print(insert_command)
                            self.cursor_bdc.execute(insert_command, t)
                            self.connection_bdc.commit()
                            print()
                    # --------   

                    elif ds == 'flags_and_features':
                        user_data2 = user_data['Result'][0]['FlagsAndFeatures']
                        print(user_data2)
                        max_index = ("SELECT MAX(indice) FROM bdc_data.people_indicadore_caracteristicas") 
                        self.cursor_bdc.execute(max_index)
                        max_index = self.cursor_bdc.fetchone()[0]
                        print(max_index)

                        for row in user_data2:
                            max_index = max_index + 1
                            print(max_index)

                            insert_command = ("INSERT INTO bdc_data.people_indicadore_caracteristicas (indice, id_pebmed, documento, nome_do_modelo, descricao_do_modelo, rating, score) VALUES (%s, %s, %s, %s, %s, %s, %s ) " )

                            # flags_and_features parameters
                            t = (
                                max_index
                                , id
                                , cpf
                                , str(row['ModelName'])
                                , str(row['ModelDescription'])
                                , str(row['ModelRating'])
                                , str(row['ModelScore']) )

                            print(insert_command)
                            self.cursor_bdc.execute(insert_command, t)
                            self.connection_bdc.commit()
                            print()
                    # --------   

                    elif ds == 'university_student_data':
                        user_data2 = user_data['Result'][0]['Scholarship']['ScholarshipHistory']
                        print(user_data2)
                        
                        for row in user_data2:                       
                            insert_command = ("INSERT INTO bdc_data.people_escolaridade_detalhes ( id_pebmed, documento, nivel, instituicao, curso, turno, tipo_de_curso, ano_de_inicio, ano_de_termino ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s ) " )
                        
                            # university_student_data parameters
                            t = (
                                id
                                , cpf
                                , str(row['Level'])
                                , str(row['Institution'])
                                , str(row['SpecializationArea'])
                                , str(row['CourseShift'])
                                , str(row['CourseType'])
                                , str(row['StartYear'])
                                , str(row['EndYear']) )

                            print(insert_command)
                            self.cursor_bdc.execute(insert_command, t)
                            self.connection_bdc.commit()
                            print()
                    # -------- 

                    elif ds == 'addresses':
                        user_data2 = user_data['Result'][0]['Addresses']
                        print(user_data2)
                        
                        for row in user_data2:                       
                            insert_command = ("INSERT INTO bdc_data.people_enderecos ( id_pebmed, documento, tipologia, titularidade, nucleo, numero, complemento, bairro, cep, cidade, estado, pais, tipo, codigo_do_household, codigo_do_predio_building_code, passagens_do_household, passagens_suspeitas_do_household, passagens_de_captura_do_household, passagens_de_validacao_do_household, passagens_de_consulta_do_household, passagens_medias_por_mes_do_household, quantidade_de_entidades_do_household, passagens_do_building_code, passagens_suspeitas_do_building_code, passagens_de_captura_do_building_code, passagens_de_validacao_do_building_code, passagens_de_consulta_do_building_code, passagens_medias_por_mes_do_building_code, quantidade_de_entidades_do_building_code, quantidade_de_households_do_building_code, prioridade, `Flag Principal`, `Flag Recente`, `Flag Ativo`, flag_ratificado, latitude, longitude, data_da_primeira_passagem, data_da_ultima_passagem ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) " )
                        
                            # university_student_data parameters 39
                            t = (
                                id
                                , cpf
                                , str(row['Typology']) tipologia
                                , str(row['Title']) titularidade
                                , str(row['AddressMain']) nucleo
                                , str(row['Number']) numero
                                , str(row['Complement']) complemento
                                , str(row['Neighborhood']) bairro
                                , str(row['ZipCode']) cep
                                , str(row['City']) cidade
                                , str(row['State']) estado
                                , str(row['Country']) pais
                                , str(row['Type']) tipo
                                , str(row['HouseholdCode']) codigo_do_household
                                , str(row['BuildingCode']) codigo_do_predio_building_code
                                , str(row['HouseholdTotalPassages'])  passagens_do_household
                                , str(row['HouseholdBadPassages']) passagens_suspeitas_do_household
                                , str(row['HouseholdCrawlingPassages']) passagens_de_captura_do_household
                                , str(row['HouseholdValidationPassages'])  passagens_de_validacao_do_household
                                , str(row['HouseholdQueryPassages']) passagens_de_consulta_do_household
                                , str(row['HouseholdMonthAveragePassages']) passagens_medias_por_mes_do_household
                                , str(row['HouseholdNumberOfEntities']) quantidade_de_entidades_do_household
                                , str(row['BuildingTotalPassages'])  passagens_do_building_code
                                , str(row['BuildingBadPassages'])  passagens_suspeitas_do_building_code
                                , str(row['BuildingCrawlingPassages'])  passagens_de_captura_do_building_code
                                , str(row['BuildingValidationPassages']) passagens_de_validacao_do_building_code
                                , str(row['BuildingQueryPassages']) passagens_de_consulta_do_building_code
                                , str(row['BuildingMonthAveragePassages']) passagens_medias_por_mes_do_building_code
                                , str(row['BuildingNumberOfHouseholds']) quantidade_de_entidades_do_building_code
                                , str(row['Priority'])
                                , str(row['IsMain'])
                                , str(row['IsRecent']) 
                                , str(row['IsActive'])
                                , str(row['IsRatified'])
                                , str(row['Latitude'])
                                , str(row['Longitude'])
                                , str(row['FirstPassageDate'])
                                , str(row['LastPassageDate'])  )

                            print(insert_command)
                            self.cursor_bdc.execute(insert_command, t)
                            self.connection_bdc.commit()
                            print()
                    # -------- 
  
                    else:
                        print ("Nenhuma das opcoes")
                        break                  

        #https://bigboost.bigdatacorp.com.br/peoplev2?Datasets=basic_data&q=doc{34288575850}&AccessToken=d2b92070-dcb3-48e7-942a-bd5fa9a94452


    def close_connections (self):
        print("Fechando conexoes ... ")
        self.connection_pebmedapps.close()
        self.connection_bdc.close()
        print("Done!")


if __name__ == '__main__':
    db_connection = DatabaseConnection()
    db_connection.execute_query()
    db_connection.close_connections()