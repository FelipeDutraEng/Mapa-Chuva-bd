import os
import pandas as pd
import time

# Definir os caminhos das pastas
caminho_raw = r"C:\Users\Felip\OneDrive\Documentos\Engenharia de Dados\Projeto Dados Meteorologicos\dados\data_lake\raw"
caminho_processed = r"C:\Users\Felip\OneDrive\Documentos\Engenharia de Dados\Projeto Dados Meteorologicos\dados\data_lake\processed"

# Garantir que a pasta processed existe
os.makedirs(caminho_processed, exist_ok=True)

# Listar os anos disponíveis
anos = sorted(os.listdir(caminho_raw))

# Função para padronizar os nomes das colunas
def padronizar_colunas(df):
    df.columns = [col.strip().replace(":", "").replace("?", "").upper() for col in df.columns]
    return df

# Loop por cada ano encontrado na pasta raw
for ano in anos:
    caminho_ano_raw = os.path.join(caminho_raw, ano)
    caminho_ano_processed = os.path.join(caminho_processed, ano)

    # Verifica se é realmente uma pasta de ano
    if not os.path.isdir(caminho_ano_raw):
        continue

    # Criar pasta do ano em processed caso não exista
    os.makedirs(caminho_ano_processed, exist_ok=True)

    # Lista os arquivos CSV dentro da pasta do ano
    arquivos_csv = [f for f in os.listdir(caminho_ano_raw) if f.endswith('.CSV')]

    # Criar lista para armazenar os DataFrames
    lista_dfs = []

    print(f"\n🚀 Processando ano {ano}... ({len(arquivos_csv)} arquivos encontrados)")
    start_time = time.time()  # Inicia o cronômetro

    for arquivo in arquivos_csv:
        caminho_csv = os.path.join(caminho_ano_raw, arquivo)

        try:
            # Testa os dois delimitadores possíveis (; e ,)
            try:
                df = pd.read_csv(caminho_csv, delimiter=';', encoding='latin1', on_bad_lines='skip')
            except pd.errors.ParserError:
                df = pd.read_csv(caminho_csv, delimiter=',', encoding='latin1', on_bad_lines='skip')

            # Padronizar nomes das colunas para evitar erros de codificação
            df = padronizar_colunas(df)

            # Substituir valores -9999 por NaN (nulos)
            df.replace(-9999, pd.NA, inplace=True)

            # Adiciona o DataFrame na lista
            lista_dfs.append(df)

        except Exception as e:
            print(f"❌ Erro ao processar {arquivo}: {e}")
            continue

    # Se houver arquivos processados, salvar no formato Parquet
    if lista_dfs:
        df_final = pd.concat(lista_dfs, ignore_index=True)
        caminho_parquet = os.path.join(caminho_ano_processed, f"{ano}.parquet")
        df_final.to_parquet(caminho_parquet, index=False)

        elapsed_time = round(time.time() - start_time, 2)  # Tempo de processamento

        # Exibir resumo compacto
        print(f"✅ {ano} processado: {df_final.shape[0]} registros | {df_final.shape[1]} colunas | {len(arquivos_csv)} arquivos convertidos ({elapsed_time}s)")

print("\n🎯 Conversão concluída!")
