# Seleção com atributos usando spark.sql
# * algumas colunas
# * 2019 como ano-base
# * empresas de código 23 e 1505 (Reunidas)
# * movimentação de passageiros superior a 10.000
# * modalidade (âmbito) do serviço rodoviário
# * ordenação decrescente do total de passageiros
spark.sql("""
SELECT ano AS Ano, empresa AS Empresa, CONCAT(ori_municipio_nome, '-', des_municipio_nome) AS Linha, ori_des_localidade_nome AS `Origem-destino`, pax_total AS `Total de passageiros`
FROM tb
WHERE ano = 2019
AND (empresa LIKE '23%' OR empresa LIKE '1505%')
AND pax_total > 10000
AND servico_ambito = 'RODOVIARIO'
ORDER BY pax_total DESC
""").show(10, False)

# Seleção com atributos usando select do DataFrame
# * algumas colunas
tb.select(
    "ano",
    "empresa",
    "prefixo",
    concat_ws("-", "ori_municipio_nome", "des_municipio_nome").alias("linha_ori_des_sie"),
    concat_ws("-", "ori_localidade_nome", "des_localidade_nome").alias("secao_ori_des_sie"),
    "pax_total"
).show(10, False)

# Seleção com atributos usando select do DataFrame
# * algumas colunas
# * concatenação de dados
tb.select(
    "ano",
    "empresa",
    struct("prefixo", concat_ws("-", "ori_municipio_nome", "des_municipio_nome").alias("linha_ori_des_sie")).alias("linha"),
    concat_ws("-", "ori_localidade_nome", "des_localidade_nome").alias("secao_ori_des_sie"),
    "pax_total"
).show(10, False)

# Schema ANTT legado
schema = StructType([
    StructField("ano", LongType(), True),
    StructField("codigo", StringType(), True),
    StructField("des_localidade_id", StringType(), True),
    StructField("des_localidade_nome", StringType(), True),
    StructField("des_localidade_uf", StringType(), True),
    StructField("des_municipio_id", StringType(), True),
    StructField("des_municipio_nome", StringType(), True),
    StructField("empresa", StringType(), True),
    StructField("empresa_cnpj", StringType(), True),
    StructField("empresa_situacao", StringType(), True),
    StructField("empresa_tipo", StringType(), True),
    StructField("ida_idoso_desconto", StringType(), True),
    StructField("ida_idoso_gratis", StringType(), True),
    StructField("ida_jovem_desconto", StringType(), True),
    StructField("ida_jovem_gratis", StringType(), True),
    StructField("ida_pagantes", StringType(), True),
    StructField("ida_passelivre", StringType(), True),
    StructField("km", StringType(), True),
    StructField("km_total", StringType(), True),
    StructField("linha", StringType(), True),
    StructField("linha_id", StringType(), True),
    StructField("lugares_idas", StringType(), True),
    StructField("lugares_voltas", StringType(), True),
    StructField("mes", StringType(), True),
    StructField("ori_des_localidade_nome", StringType(), True),
    StructField("ori_localidade_id", StringType(), True),
    StructField("ori_localidade_nome", StringType(), True),
    StructField("ori_localidade_uf", StringType(), True),
    StructField("ori_municipio_id", StringType(), True),
    StructField("ori_municipio_nome", StringType(), True),
    StructField("pax_gratis_descontos", StringType(), True),
    StructField("pax_idoso_desconto", StringType(), True),
    StructField("pax_idoso_gratis", StringType(), True),
    StructField("pax_jovem_desconto", StringType(), True),
    StructField("pax_jovem_gratis", StringType(), True),
    StructField("pax_pagantes", StringType(), True),
    StructField("pax_passelivre", StringType(), True),
    StructField("pax_total", StringType(), True),
    StructField("prefixo", StringType(), True),
    StructField("secao_id", StringType(), True),
    StructField("sequencial", StringType(), True),
    StructField("servico_ambito", StringType(), True),
    StructField("servico_tipo", StringType(), True),
    StructField("sisdap_fim", StringType(), True),
    StructField("sisdap_inicio", StringType(), True),
    StructField("viagem_idas", StringType(), True),
    StructField("viagem_voltas", StringType(), True),
    StructField("volta_idoso_desconto", StringType(), True),
    StructField("volta_idoso_gratis", StringType(), True),
    StructField("volta_jovem_desconto", StringType(), True),
    StructField("volta_jovem_gratis", StringType(), True),
    StructField("volta_pagantes", StringType(), True),
    StructField("volta_passelivre", StringType(), True)])

# Teste de leitura
leitura_joined = spark.read.option('delimiter', ';').option('encoding', 'utf-8').option('header', 'true').parquet("./intermunicipal_sc/ref_join/parquet/singlefile/")

def teste_qualidade(df, n_colunas, n_linhas):
    if (len(df.columns) == n_colunas) == True and (int(df.count()) == n_linhas) == True:
        print("OK")
    else:
        print("FAILED")

teste_qualidade(leitura_joined, 17, 615285)