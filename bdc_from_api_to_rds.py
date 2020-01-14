import requests
import json
import pymysql
import dateutil.parser
from datetime import datetime



class DatabaseConnection:
    
    def __init__(self):
        try:
            self.connection_bdc = pymysql.connect(
                host='xxxx'
                , user='xxx'
                , passwd = 'xxx'  
                , db='xxx')

            self.connection_pebmedapps = pymysql.connect(
                host='xxxx'
                , user='xxxx'
                , passwd = 'xxx'
                , db='xxxx')
                    
            self.cursor_bdc = self.connection_bdc.cursor()
            self.cursor_pebmedapps = self.connection_pebmedapps.cursor()

        except:
            print("Error getting database connection")
            exit()


    def execute_query(self):
        fetch_id_cpf = ("SELECT id, cpf FROM pebmedapps.tb_usuario a LEFT OUTER JOIN bdc_data.people_dados_basicos b ON (a.id = b.id_pebmed) WHERE DATE(data_cadastro) >= '2020-01-01' AND cpf IS NOT NULL  AND cpf NOT IN  ('0000000000000','0') AND b.id_pebmed IS NULL ORDER BY 1 LIMIT 10")
        self.cursor_pebmedapps.execute(fetch_id_cpf)
        
        cpfs = self.cursor_pebmedapps.fetchall()

        datasets = ['basic_data','related_people', 'business_relationships', 'financial_data', 'online_presence', 'passages', 'interests_and_behaviors', 'demographic_data', 'flags_and_features', 'university_student_data', 'addresses', 'emails', 'occupation_data', 'collections', 'class_organization' ] # 'ondemand_restituicao'

        api_token = 'd2b92070-dcb3-48e7-942a-bd5fa9a94452'
        api_url_base = 'https://bigboost.bigdatacorp.com.br/peoplev2?'
    
        for id, cpf in cpfs:
            
                for ds in datasets:
                    print("============================================")
                    print("CPF: {0} Dataset: {1}".format(cpf, ds) )
                    print("============================================")
                    print()


                    payload = {'Datasets':ds, 'q=doc:':'{'+cpf+'}', 'AccessToken':api_token}
                    response = requests.get(api_url_base, params = payload).url.replace('%3D','=').replace('%3A=%7B','{').replace('%7D','}')
                    response_final = requests.get(response)
                    user_data = response_final.json()
                    print(response)

                    # -----------------------------------------------
                    # Dataset: basic_data
                    # -----------------------------------------------
                    if ds == 'basic_data':
                        try:
                            user_data2 = []
                            user_data2 = user_data['Result'][0]['BasicData']        
                            print(user_data2)
                
                            insert_command = ("INSERT INTO bdc_data.people_dados_basicos ( id_pebmed, documento, `documento.1`, pais_de_origem, orgao_emissor, status_do_documento, pis, nome, nome_comum, nome_padronizado, unicidade_do_nome, unicidade_do_primeiro_nome, unicidade_do_primeiro_e_ultimo_nome, nome_da_mae, nome_do_pai, data_de_nascimento, idade, genero, signo_do_zodiaco, signo_do_calendario_chines, indicacao_de_obito, data_da_primeira_captura, data_da_ultima_captura ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " )
                            
                            # basic_data parameters 
                            t = (
                                id
                                , user_data2.get('TaxIdNumber') # documento
                                , user_data2.get('TaxIdNumber') # documento.1
                                , user_data2.get('BirthCountry') # pais_de_origem
                                , user_data2.get('TaxIdOrigin') # orgao_emissor
                                , user_data2.get('TaxIdStatus') # status_do_documento
                                , user_data2.get('AlternativeIdNumbers').get('SocialSecurityNumber') if user_data2.get('AlternativeIdNumbers') is not None else None # pis
                                , user_data2.get('Name') # nome
                                , user_data2.get('Aliases').get('CommonName') if user_data2.get('Aliases') is not None else None # nome_comum
                                , user_data2.get('Aliases').get('StandardizedName')  if user_data2.get('Aliases') is not None else None # nome_padronizado
                                , user_data2.get('NameUniquenessScore') # unicidade_do_nome
                                , user_data2.get('FirstNameUniquenessScore') # unicidade_do_primeiro_nome
                                , user_data2.get('FirstAndLastNameUniquenessScore') # unicidade_do_primeiro_e_ultimo_nome
                                , user_data2.get('MotherName') # nome_da_mae
                                , user_data2.get('FatherName') # nome_do_pai
                                , user_data2.get('BirthDate')  # data_de_nascimento
                                , user_data2.get('Age') # idade
                                , user_data2.get('Gender') # genero
                                , user_data2.get('ZodiacSign') # signo_do_zodiaco
                                , user_data2.get('ChineseSign') # signo_do_calendario_chines
                                , user_data2.get('HasObitIndication') # indicacao_de_obito
                                , datetime.strftime(dateutil.parser.parse(user_data2.get('CreationDate')),'%d/%m/%y') if user_data2.get('CreationDate') is not None else None
                                , datetime.strftime(dateutil.parser.parse(user_data2.get('LastUpdateDate')),'%d/%m/%y') if user_data2.get('LastUpdateDate') is not None else None ) # data_da_ultima_captura

                            print(insert_command)
                            self.cursor_bdc.execute(insert_command, t)
                            self.connection_bdc.commit()
                            print()
                        except:
                            print("Error in [BasicData] json data")
                            print()
                            break
                           
                        

                    # -----------------------------------------------
                    # Dataset: related_people
                    # -----------------------------------------------
                    elif ds == 'related_people':
                        try:
                            user_data2 = []
                            user_data2 = user_data['Result'][0]['RelatedPeople']['PersonalRelationships']  
                        except:
                            print(user_data2)
                            print("Key [PersonalRelationships] not found in json") 
                            print()


                        try: 
                            for row in user_data2:                 
                                insert_command = ("INSERT INTO bdc_data.people_relacionamentos_pessoais_detalhes (id_pebmed, documento, documento_da_pessoa_relacionada, tipo_do_documento_da_pessoa_relacionada, pais_da_pessoa_relacionada, nome_da_pessoa_relacionada, tipo_do_relacionamento, `Nivel do Relacionamento`, data_de_inicio_do_relacionamento, data_de_termino_do_relacionamento) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )  " )

                                #related_people parameters
                                t = (
                                    id
                                    , cpf
                                    , row.get('RelatedEntityTaxIdNumber')
                                    , row.get('RelatedEntityTaxIdType')
                                    , row.get('RelatedEntityTaxIdCountry')
                                    , row.get('RelatedEntityName')
                                    , row.get('RelationshipType')
                                    , row.get('RelationshipLevel')
                                    , datetime.strftime(dateutil.parser.parse(row.get('RelationshipStartDate')),'%d/%m/%y') if row.get('RelationshipStartDate') is not None else None 
                                    , datetime.strftime(dateutil.parser.parse(row.get('RelationshipEndDate')),'%d/%m/%y')  if row.get('RelationshipEndDate')   is not None else None  )
                                
                                print(insert_command)
                                self.cursor_bdc.execute(insert_command, t)
                                self.connection_bdc.commit()   
                                print()
                        except:
                                print("Error in [RelatedPeople] json data") 

 
                    # -----------------------------------------------
                    # Dataset: business_relationships
                    # -----------------------------------------------
                    elif ds == 'business_relationships':
                        try:
                            user_data2 = []
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
                                    , row.get('RelatedEntityTaxIdNumber')
                                    , row.get('RelatedEntityTaxIdType')
                                    , row.get('RelatedEntityTaxIdCountry')
                                    , row.get('RelatedEntityName')
                                    , row.get('RelationshipType')
                                    , row.get('RelationshipLevel')
                                    , datetime.strftime(dateutil.parser.parse(row.get('RelationshipStartDate')),'%d/%m/%y') if row.get('RelationshipStartDate') is not None else None
                                    , datetime.strftime(dateutil.parser.parse(row.get('RelationshipEndDate')),'%d/%m/%y')   if row.get('RelationshipEndDate')   is not None else None )

                                print(insert_command)
                                self.cursor_bdc.execute(insert_command, t)
                                self.connection_bdc.commit()
                                print()
                        except:
                            print("Error in [BusinessRelationships] json data")
                            print()




                    # -----------------------------------------------
                    # Dataset: emails
                    # -----------------------------------------------
                    elif ds == 'emails':
                        try:
                            user_data2 = []
                            user_data2 = user_data['Result'][0]['Emails'] 
                            print(user_data2)

                            for row in user_data2:
                                insert_command = ("INSERT INTO bdc_data.people_emails (id_pebmed, documento, email, dominio, usuario, tipo, passagens, `Passagens Suspeitas`, passagens_de_captura, passagens_de_validacao, passagens_de_consultao, media_de_passagens_por_mes, numero_de_entidades_associadas, prioridade, flag_principal, flag_recente, flag_ativo, status_da_validacao, data_da_ultima_validacao, data_da_primeira_passagem, data_da_ultima_passagem) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) " )

                                # emails parameters
                                t = (
                                    id
                                    , cpf
                                    , row.get('EmailAddress')
                                    , row.get('Domain')
                                    , row.get('UserName')
                                    , row.get('Type')
                                    , row.get('EmailTotalPassages')
                                    , row.get('EmailBadPassages')
                                    , row.get('EmailCrawlingPassages')
                                    , row.get('EmailValidationPassages') 
                                    , row.get('EmailQueryPassages')
                                    , row.get('EmailMonthAveragePassages')
                                    , row.get('EmailNumberOfEntities')
                                    , row.get('Priority')
                                    , row.get('IsMain')
                                    , row.get('IsRecent')
                                    , row.get('IsActive')
                                    , row.get('ValidationStatus')
                                    , datetime.strftime(dateutil.parser.parse(row.get('LastValidationDate')),'%d/%m/%y') if row.get('LastValidationDate') is not None else None
                                    , datetime.strftime(dateutil.parser.parse(row.get('FirstPassageDate')),'%d/%m/%y')   if row.get('FirstPassageDate')   is not None else None
                                    , datetime.strftime(dateutil.parser.parse(row.get('LastPassageDate')),'%d/%m/%y')    if row.get('LastPassageDate')    is not None else None )

                                print(insert_command)
                                self.cursor_bdc.execute(insert_command, t)
                                self.connection_bdc.commit()
                                print()
                        except:
                            print("Error in [Emails] json data")
                            print()


                    # -----------------------------------------------
                    # Dataset: financial_data
                    # -----------------------------------------------
                    elif ds == 'financial_data':
                        try:
                            user_data2 = []
                            user_data2 = user_data['Result'][0]['FinantialData']   
                            print(user_data2)
                        
                            insert_command = ("INSERT INTO bdc_data.people_dados_financeiros (id_pebmed, documento, renda_estimada_ibge, renda_estimada_mte, renda_estimada_bigdata_copr, renda_estimada_bigdata_corp_Empresas, patrimonio_estimado) VALUES (%s, %s, %s, %s, %s, %s, %s) " )

                            # FinantialData parameters
                            t = (
                                id
                                , cpf
                                , user_data2.get('IncomeEstimates').get('IBGE') if user_data2.get('IncomeEstimates') is not None else None 
                                , user_data2.get('IncomeEstimates').get('MTE') if user_data2.get('IncomeEstimates') is not None else None
                                , user_data2.get('IncomeEstimates').get('BIGDATA') if user_data2.get('IncomeEstimates') is not None else None
                                , user_data2.get('IncomeEstimates').get('COMPANY OWNERSHIP') if user_data2.get('IncomeEstimates') is not None else None
                                , user_data2.get('TotalAssets') )

                            print(insert_command)
                            self.cursor_bdc.execute(insert_command, t)
                            self.connection_bdc.commit()   
                            print()
                        except:
                            print("Error in [FinantialData] json data")
                            print()


 
                    # -----------------------------------------------
                    # Dataset: online_presence
                    # -----------------------------------------------
                    elif ds == 'online_presence':
                        try:
                            user_data2 = []
                            user_data2 = user_data['Result'][0]['OnlinePresence']   
                            print(user_data2)
                        
                            insert_command = ("INSERT INTO bdc_data.people_presenca_online (id_pebmed, documento, e_shopper, e_seller, nivel_de_utilizacao_da_internet, passagens_na_web) VALUES (%s, %s, %s, %s, %s, %s) " )

                            # OnlinePresence parameters
                            t = (
                                id
                                , cpf
                                , user_data2.get('Eshopper')
                                , user_data2.get('Eseller')
                                , user_data2.get('InternetUsageLevel')
                                , user_data2.get('WebPassages') )

                            print(insert_command)
                            self.cursor_bdc.execute(insert_command, t)
                            self.connection_bdc.commit()   
                            print()
                        except:
                            print("Error in [OnlinePresence] json data")
                            print()


                    # -----------------------------------------------
                    # Dataset: passages
                    # -----------------------------------------------                
                    elif ds == 'passages':
                        try:
                            user_data2 = []
                            user_data2 = user_data['Result'][0]['Passages']   
                            print(user_data2)
                        
                            insert_command = ("INSERT INTO bdc_data.people_passagens (id_pebmed, documento, passagens, passagens_suspeitas, passagens_de_consulta, passagens_de_captura, passagens_de_validacao, media_de_passagens_por_mes, data_da_primeira_passagem, data_da_ultima_passagem, quantidade_de_enderecos, quantidade_de_telefones, `Quantidade de Emails`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " )

                            # Passages parameters
                            t = (
                                id
                                , cpf
                                , user_data2.get('TotalPassages')
                                , user_data2.get('BadPassages')
                                , user_data2.get('QueryPassages')
                                , user_data2.get('CrawlingPassages')
                                , user_data2.get('ValidationPassages')
                                , user_data2.get('MonthAveragePassages')
                                , datetime.strftime(dateutil.parser.parse(user_data2.get('FirstPassageDate')),'%d/%m/%y') if user_data2.get('FirstPassageDate') is not None else None
                                , datetime.strftime(dateutil.parser.parse(user_data2.get('LastPassageDate')),'%d/%m/%y')  if user_data2.get('LastPassageDate')  is not None else None
                                , user_data2.get('NumberOfAddresses')
                                , user_data2.get('NumberOfPhones')
                                , user_data2.get('NumberOfEmails') )

                            print(insert_command)
                            self.cursor_bdc.execute(insert_command, t)
                            self.connection_bdc.commit()   
                            print() 
                        except:
                            print("Error in [Passages] json data")


                    # -----------------------------------------------
                    # Dataset: interests_and_behaviors
                    # -----------------------------------------------
                    elif ds == 'interests_and_behaviors':
                        try:
                            user_data2 = []
                            user_data2 = user_data['Result'][0]['InterestsAndBehaviors']  
                            print(user_data2)                     
                            insert_command = ("INSERT INTO bdc_data.people_interesses_comportamentais ( id_pebmed, documento, revendedor_porta_porta, nivel_de_utlizacao_cartao_de_credito, programas_de_milhagem, investidor_online, carros_e_motos, bebes_e_criancas, saude_e_beleza, comida_e_bebida, musculacao_e_suplementos, livros, computacao, entregas, eletro_eletronicos, entretenimento, moda_e_acessorios, videogames, casa_e_decoracao, itens_domesticos, telefonia_e_celulares, insturmentos_musicais, servicos, esporte_e_lazer, streaming, brinquedos, viagem_e_turismo ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " )
                            
                            # interests_and_behaviors parameters
                            t = (
                                id
                                , cpf
                                , user_data2.get('Behaviors').get('ProductReseller') if user_data2.get('Behaviors') is not None else None
                                , user_data2.get('Behaviors').get('CreditCardScore') if user_data2.get('Behaviors') is not None else None
                                , user_data2.get('Behaviors').get('MilesProgram') if user_data2.get('Behaviors') is not None else None
                                , user_data2.get('Behaviors').get('OnlineInvestor') if user_data2.get('Behaviors') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('Automotive') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('Baby') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('BeautyAndHealth') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('BeverageAndFood') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('Bodybuilding') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('Books') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('Computing') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('Delivery') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('Eletronics') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('Entertainment') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('FashionAndAcessories') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('Games') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('HomeAndDecoration') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('Housewares') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('MobileTelco') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('MusicalInstruments') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('Service') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('SportAndLeisure') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('Streaming') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('Toys') if user_data2.get('CategoriesOfInterest') is not None else None
                                , user_data2.get('CategoriesOfInterest').get('TravelAndTourism') if user_data2.get('CategoriesOfInterest') is not None else None )

                            print(insert_command)
                            self.cursor_bdc.execute(insert_command, t)
                            self.connection_bdc.commit()
                            print()
                        except:
                            print("Error in [InterestsAndBehaviors] json data")
                            print()


                    # -----------------------------------------------
                    # Dataset: demographic_data
                    # -----------------------------------------------
                    elif ds == 'demographic_data':
                        try:
                            user_data2 = []
                            user_data2 = user_data['Result'][0]['DemographicData']
                            print(user_data2)

                            for row in user_data2:
                                insert_command = ("INSERT INTO bdc_data.people_dados_demograficos (id_pebmed, Documento, origem, nivel_de_agregacao, pais, renda_estimada, nivel_de_escolaridade_estimado, classe_social) VALUES (%s, %s, %s, %s, %s, %s, %s, %s ) " )

                                # demographic_data parameters
                                t = (
                                    id
                                    , cpf
                                    , row.get('DataOrigin')
                                    , row.get('DataAgregationLevel')
                                    , row.get('DataCountry')
                                    , row.get('EstimatedIncomeRange')
                                    , row.get('EstimatedInstructionLevel')
                                    , row.get('SocialClass') )

                                print(insert_command)
                                self.cursor_bdc.execute(insert_command, t)
                                self.connection_bdc.commit()
                                print()
                        except:
                            print("Error in [DemographicData] json data")
                            print()


                    # -----------------------------------------------
                    # Dataset: flags_and_features
                    # -----------------------------------------------
                    elif ds == 'flags_and_features':
                        try:
                            user_data2 = []
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
                                    , row.get('ModelName')
                                    , row.get('ModelDescription')
                                    , row.get('ModelRating')
                                    , row.get('ModelScore') )

                                print(insert_command)
                                self.cursor_bdc.execute(insert_command, t)
                                self.connection_bdc.commit()
                                print()
                        except:
                            print("Error in [FlagsAndFeatures] json data")
                            print()


                    # -----------------------------------------------
                    # Dataset: university_student_data
                    # -----------------------------------------------
                    elif ds == 'university_student_data':
                        try:
                            user_data2 = []
                            user_data2 = user_data['Result'][0]['Scholarship']['ScholarshipHistory']
                            print(user_data2)
                            
                            for row in user_data2:                       
                                insert_command = ("INSERT INTO bdc_data.people_escolaridade_detalhes ( id_pebmed, documento, nivel, instituicao, curso, turno, tipo_de_curso, ano_de_inicio, ano_de_termino ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s ) " )
                            
                                # university_student_data parameters
                                t = (
                                    id
                                    , cpf
                                    , row.get('Level')
                                    , row.get('Institution')
                                    , row.get('SpecializationArea')
                                    , row.get('CourseShift')
                                    , row.get('CourseType')
                                    , row.get('StartYear')
                                    , row.get('EndYear') )

                                print(insert_command)
                                self.cursor_bdc.execute(insert_command, t)
                                self.connection_bdc.commit()
                                print()
                        except:
                            print("Error in [ScholarshipHistory] json data")
                            print()


                    # -----------------------------------------------
                    # Dataset: addresses
                    # -----------------------------------------------
                    elif ds == 'addresses':
                        try:
                            user_data2 = []
                            user_data2 = user_data['Result'][0]['Addresses']
                            print(user_data2)
                            
                            for row in user_data2:                       
                                insert_command = ("INSERT INTO bdc_data.people_enderecos ( id_pebmed, documento, tipologia, titularidade, nucleo, numero, complemento, bairro, cep, cidade, estado, pais, tipo, codigo_do_household, codigo_do_predio_building_code, passagens_do_household, passagens_suspeitas_do_household, passagens_de_captura_do_household, passagens_de_validacao_do_household, passagens_de_consulta_do_household, passagens_medias_por_mes_do_household, quantidade_de_entidades_do_household, passagens_do_building_code, passagens_suspeitas_do_building_code, passagens_de_captura_do_building_code, passagens_de_validacao_do_building_code, passagens_de_consulta_do_building_code, passagens_medias_por_mes_do_building_code, quantidade_de_households_do_building_code, prioridade, `Flag Principal`, `Flag Recente`, `Flag Ativo`, flag_ratificado, latitude, longitude, data_da_primeira_passagem, data_da_ultima_passagem ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) " )
                            
                                # addresses parameters 
                                t = (
                                    id
                                    , cpf
                                    , row.get('Typology') # tipologia
                                    , row.get('Title') # titularidade
                                    , row.get('AddressMain') # nucleo
                                    , row.get('Number') # numero
                                    , row.get('Complement') # complemento
                                    , row.get('Neighborhood') # bairro
                                    , row.get('ZipCode') # cep
                                    , row.get('City') # cidade
                                    , row.get('State') # estado
                                    , row.get('Country') # pais
                                    , row.get('Type') # tipo
                                    , row.get('HouseholdCode') # codigo_do_household
                                    , row.get('BuildingCode') # codigo_do_predio_building_code
                                    , row.get('HouseholdTotalPassages')  # passagens_do_household
                                    , row.get('HouseholdBadPassages') # passagens_suspeitas_do_household
                                    , row.get('HouseholdCrawlingPassages') # passagens_de_captura_do_household
                                    , row.get('HouseholdValidationPassages')  # passagens_de_validacao_do_household
                                    , row.get('HouseholdQueryPassages') # passagens_de_consulta_do_household
                                    , row.get('HouseholdMonthAveragePassages') # passagens_medias_por_mes_do_household
                                    , row.get('HouseholdNumberOfEntities') # quantidade_de_entidades_do_household
                                    , row.get('BuildingTotalPassages')  # passagens_do_building_code
                                    , row.get('BuildingBadPassages')  # passagens_suspeitas_do_building_code
                                    , row.get('BuildingCrawlingPassages')  # passagens_de_captura_do_building_code
                                    , row.get('BuildingValidationPassages') # passagens_de_validacao_do_building_code
                                    , row.get('BuildingQueryPassages') # passagens_de_consulta_do_building_code
                                    , row.get('BuildingMonthAveragePassages') # passagens_medias_por_mes_do_building_code
                                    , row.get('BuildingNumberOfHouseholds') # quantidade_de_households_do_building_code
                                    , row.get('Priority') # prioridade
                                    , row.get('IsMain') # Flag Principal
                                    , row.get('IsRecent')  # Flag Recente
                                    , row.get('IsActive') # Flag Ativo
                                    , row.get('IsRatified') # flag_ratificado
                                    , row.get('Latitude') # latitude
                                    , row.get('Longitude') # longitude
                                    , datetime.strftime(dateutil.parser.parse(row.get('FirstPassageDate')),'%d/%m/%y') if row.get('FirstPassageDate') is not None else None
                                    , datetime.strftime(dateutil.parser.parse(row.get('LastPassageDate')),'%d/%m/%y')  if row.get('LastPassageDate')  is not None else None
                                    )

                                print(insert_command)
                                self.cursor_bdc.execute(insert_command, t)
                                self.connection_bdc.commit()
                                print()
                        except:
                            print("Error in [Addresses] json data")
                            print()


                    # -----------------------------------------------
                    # Dataset: occupation_data
                    # -----------------------------------------------
                    elif ds == 'occupation_data':
                        try:
                            user_data2 = []
                            user_data2 = user_data['Result'][0]['ProfessionData']['Professions']   
                            print(user_data2)
                        
                            for row in user_data2: 
                                insert_command = ("INSERT INTO bdc_data.people_dados_profissionais_historico ( id_pebmed, documento, setor, pais, numero_de_identificacao_do_empregador, nome_do_empregador, area, nivel, status, remuneracao_estimada, data_de_inicio_do_trabalho, data_de_termino_do_trabalho ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " )

                                # occupation_data parameters
                                t = (
                                    id
                                    , cpf
                                    , row.get('Sector')
                                    , row.get('Country')
                                    , row.get('CompanyIdNumber')
                                    , row.get('CompanyName')
                                    , row.get('Area')
                                    , row.get('Level')
                                    , row.get('Status')
                                    , row.get('IncomeRange')
                                    , datetime.strftime(dateutil.parser.parse(row.get('StartDate')),'%d/%m/%y') if row.get('StartDate') is not None else None 
                                    , datetime.strftime(dateutil.parser.parse(row.get('EndDate')),'%d/%m/%y') if row.get('EndDate')   is not None else None 
                                    )

                                print(insert_command)
                                self.cursor_bdc.execute(insert_command, t)
                                self.connection_bdc.commit()   
                                print() 
                        except:
                            print("Error in ProfessionData json data")
                            print()


                    # -----------------------------------------------
                    # Dataset: collections
                    # -----------------------------------------------
                    elif ds == 'collections':
                        try:
                            user_data2 = []
                            user_data2 = user_data['Result'][0]['Collections']   
                            print(user_data2)
                        
                            insert_command = ("INSERT INTO bdc_data.people_cobrancas (id_pebmed, documento, ocorrencias_de_cobranca, origens_de_cobranca, data_da_primeira_cobranca, data_da_ultima_cobranca) VALUES (%s, %s, %s, %s, %s, %s ) " )

                            #collections parameters
                            t = (
                                id
                                , cpf
                                , user_data2.get('CollectionOccurrences')
                                , user_data2.get('CollectionOrigins')
                                , datetime.strftime(dateutil.parser.parse(user_data2.get('FirstCollectionDate')),'%d/%m/%y') if user_data2.get('FirstCollectionDate') is not None else None 
                                , datetime.strftime(dateutil.parser.parse(user_data2.get('LastCollectionDate')),'%d/%m/%y')  if user_data2.get('LastCollectionDate')  is not None else None 
                                )
                            
                            print(insert_command)
                            self.cursor_bdc.execute(insert_command, t)
                            self.connection_bdc.commit()   
                            print()
                        except:
                            print("Error in [Collections] json data")
                            print()


                    # -----------------------------------------------
                    # Dataset: class_organization
                    # -----------------------------------------------
                    elif ds == 'class_organization':
                        try:
                            user_data2 = []
                            user_data2 = user_data['Result'][0]['Memberships']['Memberships']  
                            print(user_data2)
                            for row in user_data2: 
                                insert_command = ("INSERT INTO bdc_data.people_conselhos_associacoes ( id_pebmed, documento, nome_da_organizacao, pais_da_organizacao, tipo_da_organizacao, capitulo, numero_de_registro, categoria, status, data_de_inicio_da_associacao, data_de_termino_da_associacao ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " )

                                # class_organization parameters
                                t = (
                                    id
                                    , cpf
                                    , row.get('OrganizationName')
                                    , row.get('OrganizationCountry')
                                    , row.get('OrganizationType')
                                    , row.get('OrganizationChapter')
                                    , row.get('RegistrationId')
                                    , row.get('Category')
                                    , row.get('Status')
                                    , datetime.strftime(dateutil.parser.parse(row.get('MembershipStartDate')),'%d/%m/%y') if row.get('MembershipStartDate') is not None else None
                                    , datetime.strftime(dateutil.parser.parse(row.get('MembershipEndDate')),'%d/%m/%y')   if row.get('MembershipEndDate')   is not None else None
                                    )

                                print(insert_command)
                                self.cursor_bdc.execute(insert_command, t)
                                self.connection_bdc.commit()   
                                print() 
                        except:
                            print("Error in [Memberships] json data")
                            print()


                    # -----------------------------------------------
                    # End 
                    # -----------------------------------------------  
                    else:
                        print ("Dataset not exists")
                        break                  


    def close_connections (self):
        print("Closing database connections ... ")
        self.connection_pebmedapps.close()
        self.connection_bdc.close()
        print("Done!")


if __name__ == '__main__':

    initial_timestamp = datetime.now()
    print("Start:", initial_timestamp)
    print()

    db_connection = DatabaseConnection()
    db_connection.execute_query()
    db_connection.close_connections()
    print()

    print("Start:", initial_timestamp)

    end_timestamp = datetime.now()
    print("End:", end_timestamp)
    
    total_time = end_timestamp - initial_timestamp
    print("Total time execution:", total_time)