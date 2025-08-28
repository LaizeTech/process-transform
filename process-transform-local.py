import os
import time
import pandas as pd
from datetime import datetime
import re

# ===============================
# VARIÁVEIS DE CONFIGURAÇÃO
# ===============================
DIRETORIO_ENTRADA = r"C:\Users\Ayrton Casa\Documents\SPTech\2025\PI\Projeto\bucket-raw"  # pasta monitorada
DIRETORIO_SAIDA   = r"C:\Users\Ayrton Casa\Documents\SPTech\2025\PI\Projeto\bucket-trusted"    # pasta onde o CSV sai
INTERVALO_VERIFICACAO = 2  # segundos entre as varreduras
ESPERA_ESTABILIZACAO = 2   # segundos para checar se o arquivo terminou de copiar

# ===============================
# FUNÇÕES DE NOMENCLAURA/TRANSFORMAÇÃO (inalteradas)
# ===============================
def generate_unique_filename(original_name, plataforma_id, suffix="_processado.csv"):
    base_name = os.path.splitext(os.path.basename(original_name))[0]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_name}_{plataforma_id}_{timestamp}{suffix}"

def processXLSX(file_path, output_path):
    df = pd.read_excel(file_path)
    df_filtrado = df[df["Status do pedido"] == "Concluído"]

    colunas_mapeadas = {
        "ID do pedido": "numeroPedido",
        "Data de criação do pedido": "dtVenda",
        "Valor Total": "precoVenda",
        "Desconto do vendedor": "totalDesconto",
        "Nome do Produto": "nomeProduto",
        "Quantidade do Produto": "quantidade",
        "Nome da variação": "caracteristicaProduto"
    }

    colunas_existentes = {orig: novo for orig, novo in colunas_mapeadas.items() if orig in df_filtrado.columns}

    if not colunas_existentes:
        raise ValueError("Nenhuma coluna esperada encontrada no Excel.")

    df_selecionado = df_filtrado[list(colunas_existentes.keys())].rename(columns=colunas_existentes)
    
    # Adicionar 'sem caracteristica' se a coluna caracteristicaProduto existir e tiver valores vazios
    if 'caracteristicaProduto' in df_selecionado.columns:
        df_selecionado['caracteristicaProduto'] = df_selecionado['caracteristicaProduto'].fillna('sem caracteristica')
    
    df_selecionado.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Arquivo XLSX processado e salvo em: {output_path}")

def processCSV(file_path, output_path):
    # Função para extrair característica (entre parênteses no final do nome)
    def extrair_caracteristica_e_nome(nome_produto):
        if pd.isna(nome_produto):
            return None, nome_produto
        s = nome_produto.strip()
        end = s.rfind(')')
        if end == -1:
            return None, s
        # Encontrar o '(' correspondente ao último ')'
        count = 0
        start = -1
        for i in range(end, -1, -1):
            if s[i] == ')':
                count += 1
            elif s[i] == '(':
                count -= 1
                if count == 0:
                    start = i
                    break
        if start == -1:
            return None, s
        # Extrair característica e nome limpo
        caracteristica = s[start+1:end]
        nome_limpo = (s[:start] + s[end+1:]).strip()
        return caracteristica, nome_limpo

    df = pd.read_csv(file_path, sep=';', encoding='latin1')

    # Dados da venda
    df_vendas = df[df['Data'].notna()].copy()
    df_vendas = df_vendas[['Número do Pedido', 'Data', 'Total', 'Desconto']]
    df_vendas.rename(columns={
        'Número do Pedido': 'numeroPedido',
        'Data': 'dtVenda',
        'Total': 'precoVenda',
        'Desconto': 'totalDesconto'
    }, inplace=True)
    df_vendas['dtVenda'] = pd.to_datetime(df_vendas['dtVenda'], format='%d/%m/%Y', errors='coerce').dt.date

    # Dados dos produtos
    df_produtos = df[df['Nome do Produto'].notna()].copy()

    df_produtos[['caracteristicaProduto', 'nomeProduto']] = df_produtos['Nome do Produto'].apply(
        lambda x: pd.Series(extrair_caracteristica_e_nome(x))
    )

    df_produtos = df_produtos[['Número do Pedido', 'nomeProduto', 'Quantidade Comprada', 'caracteristicaProduto']].rename(
        columns={
            'Número do Pedido': 'numeroPedido',
            'Quantidade Comprada': 'quantidade'
        }
    )

    # Junção final
    df_final = pd.merge(df_produtos, df_vendas, on='numeroPedido', how='left')
    
    # Adicionar 'sem caracteristica' para valores vazios na coluna caracteristicaProduto
    df_final['caracteristicaProduto'] = df_final['caracteristicaProduto'].fillna('sem caracteristica')
    
    df_final.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
    print(f"Arquivo CSV processado e salvo em: {output_path}")

# ===============================
# AUXILIARES DE MONITORAMENTO
# ===============================
def arquivo_pronto(caminho):
    """Evita processar arquivo ainda sendo copiado: checa se o tamanho estabilizou."""
    try:
        tamanho1 = os.path.getsize(caminho)
        time.sleep(ESPERA_ESTABILIZACAO)
        tamanho2 = os.path.getsize(caminho)
        return tamanho1 == tamanho2
    except FileNotFoundError:
        return False

def processar_novo_arquivo(caminho_arquivo):
    ext = os.path.splitext(caminho_arquivo)[1].lower()
    if ext == ".xlsx":
        plataforma_id = 1
    elif ext == ".csv":
        plataforma_id = 2
    else:
        print(f"Ignorando (extensão não suportada): {caminho_arquivo}")
        return

    output_filename = generate_unique_filename(caminho_arquivo, plataforma_id)
    os.makedirs(DIRETORIO_SAIDA, exist_ok=True)
    output_path = os.path.join(DIRETORIO_SAIDA, output_filename)

    print(f"🔄 Processando: {caminho_arquivo}")
    if ext == ".xlsx":
        processXLSX(caminho_arquivo, output_path)
    else:
        processCSV(caminho_arquivo, output_path)

# ===============================
# LOOP DE MONITORAMENTO
# ===============================
if __name__ == "__main__":
    print(f"👀 Monitorando: {DIRETORIO_ENTRADA}")
    os.makedirs(DIRETORIO_ENTRADA, exist_ok=True)
    os.makedirs(DIRETORIO_SAIDA, exist_ok=True)

    vistos = {}  # caminho -> mtime já processado

    while True:
        try:
            for nome in os.listdir(DIRETORIO_ENTRADA):
                caminho = os.path.join(DIRETORIO_ENTRADA, nome)
                if not os.path.isfile(caminho):
                    continue

                ext = os.path.splitext(nome)[1].lower()
                if ext not in (".xlsx", ".csv"):
                    continue

                # Pula se ainda não estabilizou (arquivo em cópia)
                if not arquivo_pronto(caminho):
                    continue

                mtime = os.path.getmtime(caminho)
                # Processa se é novo ou foi modificado depois do último processamento
                if caminho not in vistos or mtime > vistos[caminho]:
                    processar_novo_arquivo(caminho)
                    vistos[caminho] = mtime

            time.sleep(INTERVALO_VERIFICACAO)

        except KeyboardInterrupt:
            print("\nEncerrado pelo usuário.")
            break

        except Exception as e:
            print(f"Erro no monitoramento: {e}")
            time.sleep(INTERVALO_VERIFICACAO)