# Spark

## instalar as dependências
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
!tar xf spark-2.4.4-bin-hadoop2.7.tgz
!pip install -q findspark

## configurar as variáveis de ambiente
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.4.4-bin-hadoop2.7"

## tornar o pyspark "importável"
import findspark
findspark.init('spark-2.4.4-bin-hadoop2.7')

## iniciar uma sessão local e importar dados do Airbnb
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').getOrCreate()

## montando Drive
from google.colab import drive
drive.mount('/content/drive')

# Lendo parquets ANTT
antt = spark.read.option("header", "True").parquet("drive/My Drive/data/ANTT_2005_a_2021/ref/parquet/")
antt.createOrReplaceTempView("antt")
antt.printSchema()

# Definindo função
def select_antt():
    # ---------------------------------------------------------------------------------------------------------
    print("*" * 100)
    print("Selecione o tipo de particionamento:\n1 | por ano / empresa / cidade\n2 | por ano / por cidade\n3 | por ano / por empresa\n4 | por cidade full (todos os anos e empresas)")
    print("*" * 100)
    select_start = (input("Digite a opção de sua seleção: "))
    # ---------------------------------------------------------------------------------------------------------
    if int(select_start) == 1: # 1 | por ano / empresa / cidade
        print(f"Sua seleção foi:\nAno(s): {select_anos}\nEmpresa(s): {select_empresas}\nCidades(s): {select_cidades}")
        print("*" * 100)
        print("Realizando a consulta.")
        for cidade in select_cidades:
            for ano in select_anos:
                for empresa in select_empresas:
                    origem = cidade
                    destino = origem
                    ano = ano
                    empresa = empresa

                    df_temp = spark.sql(f"""

                    SELECT '{origem}' AS cidade_base, ano, mes, empresa, prefixo, ori_des_localidade_nome, pax_total
                    FROM antt
                    WHERE pax_total > 0
                    AND empresa LIKE "%{empresa}%"
                    AND ano = {ano}
                    AND (ori_des_localidade_nome LIKE '%{origem}%' OR ori_des_localidade_nome LIKE '%{destino}%')
                    ORDER BY ano ASC, empresa ASC, prefixo ASC, pax_total DESC

                    """)
                    print("Gravando a consulta em formato xlsx.")
                    if origem == destino:
                        df_temp.toPandas().to_excel(f"./drive/My Drive/data/export/interestadual_filtro/{origem}_{ano}_{empresa}.xlsx", index = False)
                    else:
                        df_temp.toPandas().to_excel(f"./drive/My Drive/data/export/interestadual_filtro/{origem}_{destino}_{ano}_{empresa}.xlsx", index = False)
    # ---------------------------------------------------------------------------------------------------------
    elif int(select_start) == 2: # 2 | por cidade / por ano
        print(f"Sua seleção foi:\nAno(s): {select_anos}\nCidades(s): {select_cidades}")
        print("*" * 100)
        print("Realizando a consulta.")
        for cidade in select_cidades:
            for ano in select_anos:
                origem = cidade
                destino = origem
                ano = ano

                df_temp = spark.sql(f"""

                SELECT '{origem}' AS cidade_base, ano, mes, empresa, prefixo, ori_des_localidade_nome, pax_total
                FROM antt
                WHERE pax_total > 0
                AND ano = {ano}
                AND (ori_des_localidade_nome LIKE '%{origem}%' OR ori_des_localidade_nome LIKE '%{destino}%')
                ORDER BY ano ASC, empresa ASC, prefixo ASC, pax_total DESC

                """)
                print("Gravando a consulta em formato xlsx.")
                if origem == destino:
                    df_temp.toPandas().to_excel(f"./drive/My Drive/data/export/interestadual_filtro/{origem}_{ano}.xlsx", index = False)
                else:
                    df_temp.toPandas().to_excel(f"./drive/My Drive/data/export/interestadual_filtro/{origem}_{destino}_{ano}.xlsx", index = False)
        print("Gravação realizada.")
    # ---------------------------------------------------------------------------------------------------------
    elif int(select_start) == 3: # 3 | por ano / por empresa
        print(f"Sua seleção foi:\nAno(s): {select_anos}\nEmpresa(s): {select_empresas}")
        print("*" * 100)
        for empresa in select_empresas:
            for ano in select_anos:
                ano = ano
                empresa = empresa

                df_3 = spark.sql(f"""

                SELECT ano, mes, empresa, prefixo, ori_des_localidade_nome, pax_total
                FROM antt
                WHERE pax_total > 0
                AND empresa LIKE '%{empresa}%'
                AND ano = {ano}
                ORDER BY ano ASC, empresa ASC, prefixo ASC, pax_total DESC

                """)
                print("Gravando a consulta em formato xlsx.")

                df_3.toPandas().to_excel(f"./drive/My Drive/data/export/interestadual_filtro/{empresa}_{ano}.xlsx", index = False)
    # ---------------------------------------------------------------------------------------------------------
    elif int(select_start) == 4: # 3 | por cidade full
        print(f"Sua seleção foi:\nCidade(s): {select_cidades}")
        print("*" * 100)
        for cidade in select_cidades:
            origem = cidade
            destino = origem
            df_4 = spark.sql(f"""

            SELECT '{origem}' AS cidade_base, ano, mes, empresa, prefixo, ori_des_localidade_nome, pax_total
            FROM antt
            WHERE pax_total > 0
            AND (ori_des_localidade_nome LIKE '%{origem}%' OR ori_des_localidade_nome LIKE '%{destino}%')
            ORDER BY ano ASC, empresa ASC, prefixo ASC, pax_total DESC

            """)

            if origem == destino:
                df_4.toPandas().to_excel(f"./drive/My Drive/data/export/interestadual_filtro/{origem}_todos.xlsx", index = False)
            else:
                df_4.toPandas().to_excel(f"./drive/My Drive/data/export/interestadual_filtro/{origem}_{destino}_todos.xlsx", index = False)

        print("Gravação realizada.")
    # ---------------------------------------------------------------------------------------------------------
    else:
        print("Selecione uma opção válida de consulta.")

# Executando a função
select_anos = [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]
select_anos = [2006]
select_cidades = ["FLORIANOPOLIS"]
select_empresas = ["SALUTARIS"]
select_antt()