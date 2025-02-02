import os
import pandas as pd

# Caminho para os arquivos brutos (raw)
caminho_raw = r"C:\Users\Felip\OneDrive\Documentos\Engenharia de Dados\Projeto Dados Meteorologicos\dados\data_lake\raw"

# Listar os anos disponíveis
anos = sorted(os.listdir(caminho_raw))

# Criar um dicionário para armazenar as colunas de cada arquivo
colunas_por_ano = {}

print("\n🔍 **Verificando arquivos RAW...**\n")

# Loop por cada ano na pasta raw
for ano in anos:
    caminho_ano = os.path.join(caminho_raw, ano)

    # Verifica se é realmente uma pasta de ano
    if not os.path.isdir(caminho_ano):
        continue

    # Lista os arquivos CSV dentro da pasta do ano
    arquivos_csv = [f for f in os.listdir(caminho_ano) if f.endswith('.CSV')]

    if not arquivos_csv:
        print(f"⚠ Nenhum arquivo encontrado para {ano}.")
        continue

    # Selecionar um arquivo para teste
    arquivo_teste = arquivos_csv[0]
    caminho_csv = os.path.join(caminho_ano, arquivo_teste)

    print(f"📂 Testando arquivo: {arquivo_teste} ({ano})")

    try:
        # Testa os dois delimitadores possíveis (; e ,)
        try:
            df = pd.read_csv(caminho_csv, delimiter=';', encoding='latin1', on_bad_lines='skip')
        except pd.errors.ParserError:
            df = pd.read_csv(caminho_csv, delimiter=',', encoding='latin1', on_bad_lines='skip')

        # Armazena as colunas para verificação posterior
        colunas_por_ano[ano] = df.columns.tolist()

        # Exibir as primeiras 5 linhas do arquivo
        print(f"🔹 **Amostra de dados ({ano}):**\n")
        print(df.head(), "\n")

        # Contar os valores -9999 (devem ser tratados como NaN depois)
        contagem_nulos = (df == -9999).sum().sum()
        print(f"🛑 **Valores -9999 encontrados:** {contagem_nulos}\n")

    except Exception as e:
        print(f"❌ Erro ao processar {arquivo_teste}: {e}\n")
        continue

# Verificar se os arquivos seguem o mesmo padrão de colunas
print("\n📊 **Comparação de colunas entre anos**:\n")
colunas_base = None

for ano, colunas in colunas_por_ano.items():
    if colunas_base is None:
        colunas_base = colunas
    else:
        if colunas != colunas_base:
            print(f"⚠ **Diferença detectada nas colunas de {ano}!**")
            print(f"🔹 **Esperado:** {colunas_base}")
            print(f"🔸 **Encontrado:** {colunas}\n")

print("\n✅ **Verificação concluída!**\n")
