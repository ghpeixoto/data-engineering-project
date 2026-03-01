import os
import sys

os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/microsoft-11.jdk/Contents/Home"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, unix_timestamp, round

def main():
    # 1. iniciando
    print("Ligando o motor do Spark...")
    spark = SparkSession.builder \
        .appName("Analise_Exploratoria_PySpark") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    # 2. LENDO OS ARQUIVOS
    df_aval = spark.read.csv("data/base_avaliacoes.csv", header=True, inferSchema=True)
    df_pessoas = spark.read.csv("data/base_pessoas.csv", header=True, inferSchema=True)
    df_tel = spark.read.csv("data/base_telefonia.csv", header=True, inferSchema=True)

    print("\n" + "="*50)
    print(" 📊 ANÁLISE SOBRE VENDAS ")
    print("="*50)

    # Filtrando vendas reais
    df_vendas_reais = df_tel.filter((col("Motivo") == "Venda") & (col("Valor venda").isNotNull()))

    # --- PERGUNTA 1: Top 5 Vendedores ---
    top_vendedores = df_vendas_reais.groupBy("Username") \
        .agg(round(_sum("Valor venda"), 2).alias("Total_Vendido")) \
        .join(df_pessoas.select("Username", "Nome"), on="Username", how="inner") \
        .orderBy(col("Total_Vendido").desc()) \
        .limit(5)
    
    print("\n Os 5 vendedores com o maior valor total de vendas:")
    top_vendedores.select("Nome", "Total_Vendido").show(truncate=False)

    # --- PERGUNTA 2: Ticket Médio ---
    ticket_medio = df_vendas_reais.groupBy("Username") \
        .agg(round(_avg("Valor venda"), 2).alias("Ticket_Medio")) \
        .join(df_pessoas.select("Username", "Nome"), on="Username", how="inner") \
        .orderBy(col("Ticket_Medio").desc())
    
    print("\nTicket médio por vendedor (Mostrando os 5 maiores):")
    ticket_medio.select("Nome", "Ticket_Medio").show(5, truncate=False)

    # --- PERGUNTA 3: Tempo médio das ligações ---
    df_tempo = df_tel.withColumn("inicio_ts", unix_timestamp("inicio_ligacao")) \
                     .withColumn("fim_ts", unix_timestamp("fim_ligação")) \
                     .withColumn("duracao_minutos", (col("fim_ts") - col("inicio_ts")) / 60.0)
    
    tempo_medio = df_tempo.filter(col("duracao_minutos") >= 0) \
                          .select(round(_avg("duracao_minutos"), 2).alias("Tempo_Medio_Minutos"))
    
    print("\nTempo médio das ligações (Ignorando erros sistêmicos de datas invertidas):")
    tempo_medio.show()


    print("\n" + "="*50)
    print(" ANÁLISE SOBRE AVALIAÇÕES ")
    print("="*50)

    # --- PERGUNTA 4: Nota média geral ---
    nota_geral = df_aval.select(round(_avg("Nota"), 2).alias("Nota_Media_Geral"))
    print("\n📈 Nota média de avaliação geral de toda a operação:")
    nota_geral.show()

    # --- PERGUNTA 5: Pior vendedor avaliado ---
    pior_vendedor = df_aval.groupBy("Username") \
        .agg(round(_avg("Nota"), 2).alias("Media_Avaliacao")) \
        .join(df_pessoas.select("Username", "Nome"), on="Username", how="inner") \
        .orderBy(col("Media_Avaliacao").asc()) \
        .limit(1)

    print("\n📉 Vendedor com a pior média de avaliação:")
    pior_vendedor.select("Nome", "Media_Avaliacao").show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
