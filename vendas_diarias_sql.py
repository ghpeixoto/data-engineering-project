import os
import sys

# --- CHAVES DE SEGURANÇA DO MAC ---
os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/microsoft-11.jdk/Contents/Home"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

def main():
    # 1. INICIANDO...
    print("A iniciar o ETL de Vendas Diárias com SparkSQL...")
    spark = SparkSession.builder \
        .appName("Vendas_Diarias_SparkSQL") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    # 2. PREPARAR AS TABELAS VIRTUAIS
    df_tel = spark.read.csv("data/base_telefonia.csv", header=True, inferSchema=True)
    df_tel.createOrReplaceTempView("telefonia")

    df_pessoas = spark.read.csv("data/base_pessoas.csv", header=True, inferSchema=True)
    df_pessoas.createOrReplaceTempView("pessoas")

    # 3. CONSULTA 
    # - TO_DATE: Deixar como data '2025-03-10' para agrupamento diário.
    # - COALESCE: Se o nome do líder estiver vazio (NULL), escreve 'Sem Lideranca'.
    vendas_diarias_query = """
        SELECT 
            TO_DATE(t.inicio_ligacao) AS Data,
            COALESCE(p.`Líder da Equipe`, 'Sem Lideranca') AS lideranca,
            ROUND(SUM(t.`Valor venda`), 2) AS Valor_Total_Vendas
        FROM telefonia t
        INNER JOIN pessoas p ON t.Username = p.Username
        WHERE t.Motivo = 'Venda' AND t.`Valor venda` IS NOT NULL
        GROUP BY 
            TO_DATE(t.inicio_ligacao), 
            COALESCE(p.`Líder da Equipe`, 'Sem Lideranca')
    """
    
    # Executamos a consulta SQL...
    df_resultado = spark.sql(vendas_diarias_query)

    # 4. GUARDAR O RESULTADO (Agora dentro da pasta Script em SparkSQL)
    caminho_saida = "Script em SparkSQL/output_vendas_diarias_sql_parquet"
    print(f"A guardar os dados particionados em: {caminho_saida} ...")
    
    # repartition(1) = 1 ficheiro por pasta
    # partitionBy = Quebra em pastas pela liderança
    df_resultado.repartition(1) \
        .write \
        .mode("overwrite") \
        .partitionBy("lideranca") \
        .parquet(caminho_saida)

    print(f"Sucesso! Ficheiros gerados na diretoria '{caminho_saida}'.")
    
    # finalizamos
    spark.stop()

if __name__ == "__main__":
    main()