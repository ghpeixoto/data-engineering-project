import os
import sys

os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/microsoft-11.jdk/Contents/Home"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

def main():
    # 1. iniciando...
    print("A ligar o motor do SparkSQL...")
    spark = SparkSession.builder \
        .appName("Analise_Exploratoria_SparkSQL") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    # 2. LER OS FICHEIROS E CRIAR "TABELAS VIRTUAIS"
    df_aval = spark.read.csv("data/base_avaliacoes.csv", header=True, inferSchema=True)
    df_aval.createOrReplaceTempView("avaliacoes")

    df_pessoas = spark.read.csv("data/base_pessoas.csv", header=True, inferSchema=True)
    df_pessoas.createOrReplaceTempView("pessoas")

    df_tel = spark.read.csv("data/base_telefonia.csv", header=True, inferSchema=True)
    df_tel.createOrReplaceTempView("telefonia")

    print("\n" + "="*50)
    print(" 📊 ANÁLISE SOBRE VENDAS (Via SQL) ")
    print("="*50)

    # --- PERGUNTA 1: Top 5 Vendedores ---
    top_vendedores_sql = """
        SELECT 
            p.Nome, 
            ROUND(SUM(t.`Valor venda`), 2) AS Total_Vendido
        FROM telefonia t
        INNER JOIN pessoas p ON t.Username = p.Username
        WHERE t.Motivo = 'Venda' AND t.`Valor venda` IS NOT NULL
        GROUP BY p.Nome
        ORDER BY Total_Vendido DESC
        LIMIT 5
    """
    print("\n🏆 Os 5 vendedores com o maior valor total de vendas:")
    spark.sql(top_vendedores_sql).show(truncate=False)

    # --- PERGUNTA 2: Ticket Médio ---
    ticket_medio_sql = """
        SELECT 
            p.Nome, 
            ROUND(AVG(t.`Valor venda`), 2) AS Ticket_Medio
        FROM telefonia t
        INNER JOIN pessoas p ON t.Username = p.Username
        WHERE t.Motivo = 'Venda' AND t.`Valor venda` IS NOT NULL
        GROUP BY p.Nome
        ORDER BY Ticket_Medio DESC
        LIMIT 5
    """
    print("\n💰 Ticket médio por vendedor (Amostra dos 5 maiores):")
    spark.sql(ticket_medio_sql).show(truncate=False)

    # --- PERGUNTA 3: Tempo médio das ligações ---
    tempo_medio_sql = """
        SELECT 
            ROUND(AVG((unix_timestamp(`fim_ligação`) - unix_timestamp(inicio_ligacao)) / 60.0), 2) AS Tempo_Medio_Minutos
        FROM telefonia
        WHERE (unix_timestamp(`fim_ligação`) - unix_timestamp(inicio_ligacao)) >= 0
    """
    print("\n⏱️ Tempo médio das ligações (A ignorar os erros do sistema com datas invertidas):")
    spark.sql(tempo_medio_sql).show()

    print("\n" + "="*50)
    print(" ⭐ ANÁLISE SOBRE AVALIAÇÕES (Via SQL) ")
    print("="*50)

    # --- PERGUNTA 4: Nota média geral ---
    nota_geral_sql = """
        SELECT ROUND(AVG(Nota), 2) AS Nota_Media_Geral 
        FROM avaliacoes
    """
    print("\n📈 Nota média de avaliação geral de toda a operação:")
    spark.sql(nota_geral_sql).show()

    # --- PERGUNTA 5: Pior vendedor avaliado ---
    pior_vendedor_sql = """
        SELECT 
            p.Nome, 
            ROUND(AVG(a.Nota), 2) AS Media_Avaliacao
        FROM avaliacoes a
        INNER JOIN pessoas p ON a.Username = p.Username
        GROUP BY p.Nome
        ORDER BY Media_Avaliacao ASC
        LIMIT 1
    """
    print("\n📉 Vendedor com a pior média de avaliação:")
    spark.sql(pior_vendedor_sql).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
