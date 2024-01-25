# Databricks notebook source
# MAGIC %md
# MAGIC PONTO DE CONEXÃO
# MAGIC

# COMMAND ----------

import pandas as pd

# COMMAND ----------

dbutils.fs.mount(
    source = "wasbs://dadoseducacao@strdatavikingprd.blob.core.windows.net"
    ,mount_point = "/mnt/dadoseducacao/"
    ,extra_configs = {"fs.azure.account.key.strdatavikingprd.blob.core.windows.net" :dbutils.secrets.get(scope = "scopo-kv-dataviking-prd", key = "secret-strdatavikingprd")}
)


# COMMAND ----------

# Agora você pode usar esse caminho para ler ou manipular os arquivos na pasta "raw"
pasta_raw = "/mnt/dadoseducacao/raw"

# Por exemplo, para listar os arquivos na pasta:
arquivos = dbutils.fs.ls(pasta_raw)
for file_info in arquivos:
    print(file_info.path)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Criar uma sessão Spark
spark = SparkSession.builder.appName("ExemploLeituraCSV").getOrCreate()

# Caminho do arquivo CSV
caminho_arquivo = "/mnt/dadoseducacao/raw/MICRODADOS_CADASTRO_CURSOS_2022.CSV"

# Ler o arquivo CSV para um DataFrame Spark com encoding correto
df_spark = spark.read.option("header", "true").option("delimiter", ";").option("encoding", "ISO-8859-1").csv(caminho_arquivo)

# Corrigir os nomes de colunas com caracteres especiais
df_spark = df_spark.select([col(c).alias(c.encode('utf-8').decode('ISO-8859-1')) for c in df_spark.columns])

# Mostrar as primeiras linhas do DataFrame
df_spark.show()



# COMMAND ----------

# Convertendo o DataFrame Spark para um DataFrame Pandas
dados_cursos = df_spark.toPandas()

# Exibindo o DataFrame Pandas
pd.set_option('display.max_columns', None)
display(dados_cursos)


# COMMAND ----------

# MAGIC %md
# MAGIC Distribuição por Região e Estado:
# MAGIC
# MAGIC Número de cursos por região e estado.
# MAGIC Número de instituições de ensino por região e estado.
# MAGIC Características dos Cursos:
# MAGIC
# MAGIC Número de cursos por categoria administrativa (pública, privada, etc.).
# MAGIC Distribuição de cursos por modalidade de ensino (presencial, EAD).
# MAGIC Distribuição de cursos por grau acadêmico.
# MAGIC Ingresso de Alunos:
# MAGIC
# MAGIC Total de alunos ingressantes por modalidade de ensino.
# MAGIC Proporção de alunos ingressantes por sexo.
# MAGIC Idade média dos ingressantes.
# MAGIC Proporção de alunos ingressantes por faixa etária.
# MAGIC Vagas e Inscrições:
# MAGIC
# MAGIC Total de vagas oferecidas.
# MAGIC Proporção de vagas por turno (diurno, noturno, EAD).
# MAGIC Número total de inscrições.
# MAGIC Proporção de inscrições por tipo de seleção (vestibular, ENEM, etc.).
# MAGIC Perfil dos Alunos:
# MAGIC
# MAGIC Número de alunos por faixa etária.
# MAGIC Proporção de alunos por sexo.
# MAGIC Número de alunos por raça/cor.
# MAGIC Desempenho e Conclusão:
# MAGIC
# MAGIC Número de alunos matriculados.
# MAGIC Número de alunos concluintes.
# MAGIC Taxa de conclusão.
# MAGIC Taxa de evasão.
# MAGIC Apoio Social e Atividades Extracurriculares:
# MAGIC
# MAGIC Número de alunos com apoio social.
# MAGIC Distribuição de alunos participantes de atividades extracurriculares.
# MAGIC Mobilidade Acadêmica:
# MAGIC
# MAGIC Número de alunos envolvidos em mobilidade acadêmica (inbound e outbound).
# MAGIC Reserva de Vagas e Ações Afirmativas:
# MAGIC
# MAGIC Número de alunos beneficiados por reserva de vagas.
# MAGIC Distribuição de alunos por tipo de reserva de vaga (racial, social, etc.).
# MAGIC Financiamento Estudantil:
# MAGIC
# MAGIC Número de alunos que utilizam financiamento estudantil.
# MAGIC Distribuição de alunos por tipo de financiamento (FIES, ProUni, etc.).

# COMMAND ----------



# COMMAND ----------


# Número de cursos por região e estado
num_cursos_por_regiao_estado = dados_cursos.groupby(['Região', 'Estado']).size().reset_index(name='Número de Cursos')
'''
# Número de instituições de ensino por região e estado
num_instituicoes_por_regiao_estado = dados_cursos.groupby(['Região', 'Estado'])['Instituição'].nunique().reset_index(name='Número de Instituições')

# Características dos Cursos (exemplo: média da carga horária por região e estado)
caracteristicas_cursos = dados_cursos.groupby(['Região', 'Estado'])['Carga Horária'].mean().reset_index(name='Média de Carga Horária')

# Visualizar os resultados
print("Número de Cursos por Região e Estado:")
print(num_cursos_por_regiao_estado)

print("\nNúmero de Instituições de Ensino por Região e Estado:")
print(num_instituicoes_por_regiao_estado)

print("\nCaracterísticas dos Cursos:")
print(caracteristicas_cursos)
'''

# COMMAND ----------


