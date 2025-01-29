import os
import shutil

# Caminho da pasta onde estão os arquivos organizados por ano
pasta_origem = r"C:\Users\Felip\OneDrive\Documentos\Engenharia de Dados\Histórico Anual Meteorologico"

# Caminho da pasta destino (Data Lake - Raw)
pasta_destino = r"C:\Users\Felip\OneDrive\Documentos\Engenharia de Dados\Projeto Dados Meteorologicos\dados\data_lake\raw"

# Criar a pasta destino se não existir
os.makedirs(pasta_destino, exist_ok=True)

# Percorrer todas as pastas dentro da pasta de origem (anos)
for ano in os.listdir(pasta_origem):
    caminho_ano = os.path.join(pasta_origem, ano)

    # Verificar se é uma pasta (e não um arquivo)
    if os.path.isdir(caminho_ano):
        arquivos = [f for f in os.listdir(caminho_ano) if f.lower().endswith(".csv")]

        if not arquivos:
            print(f"❌ Nenhum arquivo CSV encontrado na pasta {ano}.")
        else:
            # Criar a pasta de destino para esse ano
            pasta_destino_ano = os.path.join(pasta_destino, ano)
            os.makedirs(pasta_destino_ano, exist_ok=True)

            for arquivo in arquivos:
                caminho_origem_arquivo = os.path.join(caminho_ano, arquivo)
                caminho_destino_arquivo = os.path.join(pasta_destino_ano, arquivo)

                # Mover o arquivo
                shutil.move(caminho_origem_arquivo, caminho_destino_arquivo)
                print(f"✅ Arquivo movido: {arquivo} → {pasta_destino_ano}/")

print("\n🚀 Todos os arquivos foram organizados com sucesso no Data Lake!")
