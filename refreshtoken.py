import requests

# Variáveis de configuração
client_id = '186c63b0-4a88-4976-8950-4367d650d1bd'
client_secret = '10cf4cd181604c82bf08a2d12a2ed17a'
# Armazena o refresh token
refresh_token = 'qCvXNuHJYYmmv5Muz-C_ngG1CtTxrk7SC6XjaY1MRIk'
token_url = 'https://api.rd.services/auth/token'

# Variável para armazenar o refresh token
refresh_token = None

def verificar_validade_token(access_token, url):
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        print('Token válido.')
    elif response.status_code == 401:
        print('Token expirado. Renovando...')
        renovar_token()
    else:
        print('Erro na verificação do token.')
       
def renovar_token():
    global refresh_token
    
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret
    }
    
    response = requests.post(token_url, data=data)
    
    if response.status_code == 200:
        token_data = response.json()
        access_token = token_data['access_token']
        refresh_token = token_data['refresh_token']  # Atualiza o refresh token, se um novo for fornecido
        print('Token renovado com sucesso.')
        print('Novo access token:', access_token)
    else:
        print('Erro na renovação do token.')