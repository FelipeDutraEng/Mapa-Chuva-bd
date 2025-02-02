import os
import pandas as pd

# Caminho da pasta onde os arquivos Parquet foram salvos
caminho_processed = r"C:\Users\Felip\OneDrive\Documentos\Engenharia de Dados\Projeto Dados Meteorologicos\dados\data_lake\processed"

# ------------------------------- FUNÇÕES ------------------------------- #

def listar_arquivos_parquet():
    """ Lista os arquivos Parquet disponíveis na pasta processed. """
    anos = sorted(os.listdir(caminho_processed))

    print("\n📂 **Arquivos Parquet encontrados:**\n")
    for ano in anos:
        caminho_ano = os.path.join(caminho_processed, ano)
        if os.path.isdir(caminho_ano):
            arquivos_parquet = [f for f in os.listdir(caminho_ano) if f.endswith('.parquet')]
            if arquivos_parquet:
                print(f"✔ {ano}: {arquivos_parquet}")
            else:
                print(f"⚠ {ano}: Nenhum arquivo Parquet encontrado.")

def testar_leitura_parquet(ano):
    """ Testa a leitura do arquivo Parquet de um determinado ano. """
    caminho_parquet = os.path.join(caminho_processed, ano, f"{ano}.parquet")

    if not os.path.exists(caminho_parquet):
        print(f"\n❌ Arquivo {ano}.parquet **NÃO ENCONTRADO**.")
        return

    try:
        df = pd.read_parquet(caminho_parquet)
        print(f"\n✅ Arquivo {ano}.parquet carregado com sucesso!")
        print(df.head())  # Exibir as primeiras 5 linhas
    except Exception as e:
        print(f"\n❌ Erro ao abrir {ano}.parquet: {e}")

def verificar_valores_nulos(ano):
    """ Verifica se os valores -9999 foram convertidos corretamente para NaN. """
    caminho_parquet = os.path.join(caminho_processed, ano, f"{ano}.parquet")

    if not os.path.exists(caminho_parquet):
        return

    df = pd.read_parquet(caminho_parquet)
    
    valores_nulos = df.isna().sum()
    valores_nulos = valores_nulos[valores_nulos > 0]  # Mostrar apenas colunas com valores nulos

    if valores_nulos.empty:
        print(f"\n✅ **Nenhum valor nulo encontrado** no arquivo {ano}.parquet.")
    else:
        print(f"\n🔍 **Contagem de valores nulos no arquivo {ano}.parquet:**")
        print(valores_nulos)

def contar_registros(ano):
    """ Conta o número de registros e colunas do arquivo Parquet. """
    caminho_parquet = os.path.join(caminho_processed, ano, f"{ano}.parquet")

    if not os.path.exists(caminho_parquet):
        return

    df = pd.read_parquet(caminho_parquet)
    print(f"\n📊 **O arquivo {ano}.parquet possui {df.shape[0]} registros e {df.shape[1]} colunas.**")

# ------------------------------- EXECUÇÃO DOS TESTES ------------------------------- #

if __name__ == "__main__":
    # 1️⃣ Listar todos os arquivos Parquet gerados
    listar_arquivos_parquet()

    # 2️⃣ Testar leitura, valores nulos e contagem para cada ano encontrado
    anos = sorted(os.listdir(caminho_processed))

    for ano in anos:
        print("\n" + "="*50)  # Separador para cada ano
        print(f"📌 Testando o arquivo de {ano}...")

        testar_leitura_parquet(ano)
        verificar_valores_nulos(ano)
        contar_registros(ano)

    print("\n🎯 **Testes finalizados!**")
