Este projeto tem como objetivo processar, limpar e analisar dados de telefonia, vendas e avaliacoes de uma central de atendimento, alem de construir um pipeline de dados (ETL) otimizado para producao.

## Objetivos do Projeto

O teste foi dividido em duas frentes principais:
1. Analise Exploratoria de Dados: Responder a perguntas de negocio sobre performance de vendedores, ticket medio, tempo de atendimento e qualidade das avaliacoes.
2. Pipeline de Dados: Desenvolver um script para gerar uma tabela consolidada com o valor total de vendas diarias agrupadas por lideranca, salva em formato otimizado.

---

## Tecnologias Utilizadas
* Python 3.12
* Apache Spark / PySpark 
* SparkSQL 
* Jupyter Notebook 
* Parquet 

---

## Diferenciais e Regras de Negocio Aplicadas (Data Quality)

Durante a exploracao dos dados, identifiquei e tratei anomalias e regras de negocio estritas para garantir a integridade da analise final:

* Tratamento de Anomalias de Tempo: Foram identificados registros na base de telefonia onde a data de fim_ligacao era anterior a data de inicio_ligacao (gerando tempos negativos). Esses registros foram isolados para nao corromper a media de tempo de atendimento.
* Filtro de "Vendas Falsas": O calculo de Ticket Medio foi blindado para considerar estritamente os registros onde a coluna Motivo e igual a "Venda", ignorando contatos de "Reclamacao" ou "Informacao" que pudessem ter valores preenchidos indevidamente.
* Preservacao de Faturamento da Alta Gestao: Na base de pessoas, cargos de alto escalao (como Gerencia) nao possuem um Lider direto (valor NULL). No pipeline de ETL, aplicou-se a funcao COALESCE (e .fillna()) para atribuir a tag "Sem Lideranca", garantindo que o faturamento diario desses colaboradores nao fosse descartado nos Joins e Agrupamentos.

---

## Arquitetura da Solucao e Entregaveis

Para demonstrar versatilidade, a solucao foi desenvolvida em tres formatos distintos, atendendo a diferentes perfis de revisao tecnica:

1. Jupyter Notebooks/: Arquivos .ipynb contendo a analise passo a passo, ideal para Data Storytelling e documentacao visual.
2. Scripts em PySpark/: Scripts Python (.py) prontos para producao, utilizando a API de DataFrames do PySpark de forma modular.
3. Script em SparkSQL/: Abordagem utilizando a API SQL do Spark (spark.sql()), demonstrando dominio sobre a linguagem universal de dados.

### Requisitos Técnicos de Saida Atendidos
O pipeline de Vendas Diarias gera a saida respeitando rigorosamente os criterios estipulados:
* Formato Parquet.
* Particionado pela coluna de Lideranca.
* Contendo exatamente 1 unico arquivo por particao (utilizando .repartition(1)).

---

## Como Executar o Projeto Localmente

1. Clone o repositorio:
```bash
git clone [https://github.com/ghpeixoto/teste-tecnico-dados.git](https://github.com/ghpeixoto/teste-tecnico-dados.git)
cd teste-tecnico-dados
Instale as dependencias:

Bash
pip install pyspark==3.5.1
Execute os pipelines de ETL:
Para rodar a versao em PySpark :

Bash
python3 "Scripts em PySpark/vendas_diarias.py"
Para rodar a versao em SparkSQL:

Bash
python3 "Script em SparkSQL/vendas_diarias_sql.py"
Nota: Os arquivos de saida serao gerados automaticamente nas pastas output correspondentes a cada script.

Desenvolvido por Priscila Peixoto
