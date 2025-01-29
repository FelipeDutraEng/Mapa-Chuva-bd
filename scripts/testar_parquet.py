import os
import pandas as pd

# Caminho da pasta onde os arquivos Parquet foram salvos
caminho_processed = r"C:\Users\Felip\OneDrive\Documentos\Engenharia de Dados\Projeto Dados Meteorologicos\dados\data_lake\processed"

# Função para listar os arquivos Parquet convertidos
def listar_arquivos_parquet():
    anos = os.listdir(caminho_processed)
    
    print("\n📂 **Arquivos Parquet encontrados:**\n")
    
    for ano in anos:
        caminho_ano = os.path.join(caminho_processed, ano)
        if os.path.isdir(caminho_ano):
            arquivos_parquet = [f for f in os.listdir(caminho_ano) if f.endswith('.parquet')]
            print(f"✔ {ano}: {arquivos_parquet}")

# Função para testar a leitura de um arquivo Parquet
def testar_leitura_parquet(ano_teste):
    caminho_parquet = os.path.join(caminho_processed, ano_teste, f"{ano_teste}.parquet")

    try:
        df = pd.read_parquet(caminho_parquet)
        print(f"\n✅ Arquivo {ano_teste}.parquet carregado com sucesso!")
        print(df.head())  # Mostrar as primeiras 5 linhas
    except Exception as e:
        print(f"\n❌ Erro ao abrir {ano_teste}.parquet: {e}")

# Função para verificar se os valores -9999 foram convertidos corretamente para NaN
def verificar_valores_nulos(ano_teste):
    caminho_parquet = os.path.join(caminho_processed, ano_teste, f"{ano_teste}.parquet")
    
    df = pd.read_parquet(caminho_parquet)
    
    print(f"\n🔍 **Contagem de valores nulos no arquivo {ano_teste}.parquet:**")
    print(df.isna().sum())

# Função para contar os registros no arquivo Parquet
def contar_registros(ano_teste):
    caminho_parquet = os.path.join(caminho_processed, ano_teste, f"{ano_teste}.parquet")
    
    df = pd.read_parquet(caminho_parquet)
    
    print(f"\n📊 **O arquivo {ano_teste}.parquet possui {df.shape[0]} registros e {df.shape[1]} colunas.**")

# ------------------------------- EXECUÇÃO DOS TESTES ------------------------------- #

if __name__ == "__main__":
    # Listar todos os arquivos Parquet gerados
    listar_arquivos_parquet()

    # Escolher um ano para testar
    ano_teste = "2000"  # Altere para outro ano, se necessário

    # Testar leitura do arquivo Parquet
    testar_leitura_parquet(ano_teste)

    # Verificar se os valores -9999 foram convertidos corretamente para NaN
    verificar_valores_nulos(ano_teste)

    # Contar os registros no arquivo Parquet
    contar_registros(ano_teste)
