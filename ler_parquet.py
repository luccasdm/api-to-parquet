import pandas as pd
import os
import uuid

# Altera caminho para pastas de "arquivos"
pasta_atual = os.getcwd()

if pasta_atual != f"{pasta_atual}\\arquivos":
    try:
        pasta_desejada = f"{pasta_atual}\\arquivos"
        os.chdir(pasta_desejada)
    except OSError:
        print(f"Não foi possível ou Já está no diretório desejado")
else:
    print(f"Já está no diretório desejado")

df  = pd.read_parquet('rds_contatos_clientes.parquet')

df.count()

# num_particoes = 10

# # Calcule o tamanho de cada partição
# tamanho_particao = len(df) // num_particoes

# # Crie uma lista de DataFrames particionados
# particoes = [df[i*tamanho_particao:(i+1)*tamanho_particao] for i in range(num_particoes)]


# df = particoes[0]
# df.to_parquet("uuids-for-events-leads-1.parquet", index=False)

# df = particoes[1]
# df.to_parquet("uuids-for-events-leads-2.parquet", index=False)

# df = particoes[2]
# df.to_parquet("uuids-for-events-leads-3.parquet", index=False)

# df = particoes[3]
# df.to_parquet("uuids-for-events-leads-4.parquet", index=False)

# df = particoes[4]
# df.to_parquet("uuids-for-events-leads-5.parquet", index=False)

# df = particoes[5]
# df.to_parquet("uuids-for-events-leads-6.parquet", index=False)

# df = particoes[6]
# df.to_parquet("uuids-for-events-leads-7.parquet", index=False)

# df = particoes[7]
# df.to_parquet("uuids-for-events-leads-8.parquet", index=False)

# df = particoes[8]
# df.to_parquet("uuids-for-events-leads-9.parquet", index=False)

# df = particoes[9]
# df.to_parquet("uuids-for-events-leads-10.parquet", index=False)