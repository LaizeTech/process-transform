import boto3
import pandas as pd
import os
from datetime import datetime
import re

# Configurações
BUCKET_ENTRADA = "meu-bucket-entrada"
BUCKET_SAIDA = "meu-bucket-saida"

s3 = boto3.client("s3")

def generate_unique_filename(original_name, plataforma_id, suffix="_processado.csv"):
    base_name = os.path.splitext(os.path.basename(original_name))[0]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_name}_{plataforma_id}_{timestamp}{suffix}"

def lambda_handler(event, context):
    #Pega o último arquivo do bucket
    objects = s3.list_objects_v2(Bucket=BUCKET_ENTRADA)
    if "Contents" not in objects:
        print("Nenhum arquivo encontrado no bucket de entrada.")
        return

    latest_file = max(objects["Contents"], key=lambda x: x["LastModified"])
    file_key = latest_file["Key"]
    file_name = os.path.basename(file_key)
    local_input_path = f"/tmp/{file_name}"

    print(f"Último arquivo encontrado: {file_key}")

    #Baixa o arquivo para /tmp
    s3.download_file(BUCKET_ENTRADA, file_key, local_input_path)

    #Decide o que fazer com base na extensão, define plataforma e processa
    ext = file_name.lower().split('.')[-1]
    if ext == "xlsx":
        plataforma_id = 1
        output_filename = generate_unique_filename(file_name, plataforma_id)
        local_output_path = f"/tmp/{output_filename}"
        processXLSX(local_input_path, local_output_path)

    elif ext == "csv":
        plataforma_id = 2
        output_filename = generate_unique_filename(file_name, plataforma_id)
        local_output_path = f"/tmp/{output_filename}"
        processCSV(local_input_path, local_output_path)

    else:
        print(f"Extensão não suportada: {ext}")
        return

    
    #Envia o resultado para o bucket de saída
    s3.upload_file(local_output_path, BUCKET_SAIDA, output_filename)
    print(f"✅ Arquivo processado enviado para {BUCKET_SAIDA}/{output_filename}")

def processXLSX(file_path, output_path):
    df = pd.read_excel(file_path)
    df_filtrado = df[df["Status do pedido"] == "Concluído"]

    colunas_mapeadas = {
        "ID do pedido": "numeroPedido",
        "Data de criação do pedido": "dtVenda",
        "Valor Total": "precoVenda",
        "Desconto do Vendedor": "totalDesconto",
        "Nome do produto": "nomeProduto",
        "Quantidade do produto": "quantidade",
        "Nome da variação": "caracteristicaProduto"
    }

    colunas_existentes = {orig: novo for orig, novo in colunas_mapeadas.items() if orig in df_filtrado.columns}
    df_selecionado = df_filtrado[list(colunas_existentes.keys())].rename(columns=colunas_mapeadas)
    df_selecionado.to_csv(output_path, index=False, encoding="utf-8-sig")

def processCSV(file_path, output_path):

    # Função para extrair característica (entre parênteses no final do nome)
    def extrair_caracteristica(nome_produto):
        if pd.isna(nome_produto):
            return None
        match = re.search(r"\(([^()]*)\)\s*$", nome_produto.strip())
        return match.group(1) if match else None

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

    # Criar coluna caracteristicaProduto e limpar nomeProduto
    df_produtos['caracteristicaProduto'] = df_produtos['Nome do Produto'].apply(extrair_caracteristica)
    df_produtos['Nome do Produto'] = df_produtos['Nome do Produto'].str.replace(r"\s*\([^()]*\)\s*$", "", regex=True)

    df_produtos = df_produtos[['Número do Pedido', 'Nome do Produto', 'Quantidade Comprada', 'caracteristicaProduto']]
    df_produtos.rename(columns={
        'Número do Pedido': 'numeroPedido',
        'Nome do Produto': 'nomeProduto',
        'Quantidade Comprada': 'quantidade'
    }, inplace=True)

    # Junção final
    df_final = pd.merge(df_produtos, df_vendas, on='numeroPedido', how='left')
    df_final.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')