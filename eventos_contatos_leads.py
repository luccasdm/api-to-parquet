import requests
import json
import pandas as pd
import os
import uuid
import time

# Altera caminho para pastas de "arquivos"
pasta_atual = os.getcwd()

if pasta_atual != f"{pasta_atual}\\arquivos":
    try:
        pasta_desejada = f"{pasta_atual}\\arquivos"
        os.chdir(pasta_desejada)
    except OSError:
        print(f"")
else:
    print(f"Já está no diretório desejado")

access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJodHRwczovL2FwaS5yZC5zZXJ2aWNlcyIsInN1YiI6Ilh4WGZuY294TEt2UjRSeVhUckZIaDRxeThTUzI1cktBeHU0T2JYVjBwVE1AY2xpZW50cyIsImF1ZCI6Imh0dHBzOi8vYXBwLnJkc3RhdGlvbi5jb20uYnIvYXBpL3YyLyIsImFwcF9uYW1lIjoiQVBJIEF6dXJlIiwiZXhwIjoxNjk4MzY4NDQ1LCJpYXQiOjE2OTgyODIwNDUsInNjb3BlIjoiIn0.gYLl4slMP5ENNgorzC-V-IFZtaD6qvg_NTHsdE_EEhZAfuPbj4Z_Kkch3gqpPQLHmfGUbjjDsAx54PE9PQq9QyITAVrtHO69XtZ9B4v0ehLY3hMF726ou38DwmvnrGMKR1xa2ZDdNYDQFUl-AdUuO04iQhgNU7pnBEIEaoIi_tsuFQjEwqP0bPCUZQig7XxHl2YLTiQsgyZXuQ9gXgSLigbAzQOijOb8nYLtPS8CoyglBfhzUgjqwtAeyyo9JRMyU9N6YJb6Qy7fSupDpHA5x0_Qm_5Mfyz6GC4fgu0iEa2FF7eKSBFQ4o4wOzbsqGZ6hasyEcuvSbfjhbt5eZVqog"

headers = {"accept": "application/json",
           "authorization": access_token
        }

uuid_field = "uuid"
data_all = []
uuid_erro = []
loop = 1
requests_feitos = 1
string_to_check = '"message":"API rate limit exceeded"'

# Função para os requests
def requests_api(uuid):
    print(f"START: Consultando UUID: {uuid}")
    # Constroi a URL completa 
    url_completa = f"https://api.rd.services/platform/contacts/{uuid}/events?event_type=CONVERSION"
    # Faça a solicitação HTTP
    response = requests.get(url_completa, headers=headers)
    response.raise_for_status() 
    response_text_to_check = response.text
    print(f"JSON: {response_text_to_check}")
    # Verifica se o valor do JSON retornor uma string de API LIMITED EXCEEDED
    while string_to_check in response_text_to_check:
        print("PAUSE: Limite de requests atingido, aguardando 10 segundos")
        time.sleep(10)
        # Continua com consulta para o UUID pausado
        response = requests.get(url_completa, headers=headers)
        loop = loop + 1
        print(f"Consultando UUID {value} pela {loop} vez")
        print(f"JSON: {response.text}")
        response_text_to_check = response.text
        loop = 1
    # Se o valor do JSON não for a string, executa a criação do df e salva os dados
    if string_to_check not in response_text_to_check:
        print(f"Iniciando criação do dataframe e salvando dados na lista")
        data = json.loads(response.text)
        df = pd.DataFrame(data)
        data_all.append(df)
        # Exibe status
        print(f"DONE: Consulta bem-sucedida, próximo.")
        print(f"")
    else:
        print(f"Erro na solicitação: {response.status_code}") 

# Função para salvar os erros em um arquivo
def salva_erro_txt(uuid):
    uuid_erro.append(uuid)
    # Salvando arquivo com os uuids que deram except
    with open("uuids_erros.txt", "a") as arquivo:
        for item in uuid_erro:
            arquivo.write(item + "\n")
    
# Lista com uuids para Eventos de Leads
df = pd.read_parquet('uuids-for-events-leads.parquet')

# Itere sobre os dados e consulte os UUIDs
for value in df[uuid_field]:
    try:
        requests_api(value)       

    except requests.exceptions.HTTPError as e:
        print(f"Erro na solicitação HTTP: {e}")
        salva_erro_txt(value)

    except requests.exceptions.RequestException as e:
        print(f"Erro na solicitação HTTP: {e}")
        salva_erro_txt(value)

    requests_feitos = requests_feitos + 1 
    print(f"Requests realizados: {requests_feitos} de 103.578")

# Concatena todos 
final_all_data = pd.concat(data_all, ignore_index=True)
df_final = pd.DataFrame(final_all_data)

#Separa campo event-identifier
df_identifier = df_final['event_identifier']

# Separa campo aninhado em novo dataframe
df_payload = pd.json_normalize(df_final['payload'])

# Merge dos dataframes
df_payload_final = pd.merge(df_identifier, df_payload, left_index=True, right_index=True)

# Selecionando apenas as colunas corretas
df_payload_final = df_payload_final[['event_identifier', 'email', 'event_timestamp', 'conversion_identifier', 'traffic_source', 'name']]

# Salva o DataFrame em formato Parquet
df_payload_final.to_parquet("eventos-contatos-leads-api.parquet", index=False)

