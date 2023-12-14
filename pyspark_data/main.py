from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col

# Configurar diretório temporário do Spark
dir_temporario = "/projetos/tmp"  # Substitua pelo caminho desejado
spark_conf = SparkConf().set("spark.local.dir", dir_temporario)

# Inicializar uma sessão Spark com as configurações
spark = SparkSession.builder \
    .appName("ds_store") \
    .config("spark.jars", "/projetos/jars/postgresql-42.7.1.jar") \
    .config(conf=spark_conf) \
    .getOrCreate()

# Definir as informações de conexão com o PostgreSQL
url = "jdbc:postgresql://pg_database:5432/postgres"

propriedades = {
    "user": "postgres",
    "password": "123456",
    "driver": "org.postgresql.Driver"
}

# Caminho para o arquivo CSV na sua máquina local
caminho_do_arquivo_account_csv = "/projetos/data/Tables/accounts"
caminho_do_arquivo_city_csv = "/projetos/data/Tables/city"
caminho_do_arquivo_country_csv = "/projetos/data/Tables/country"
caminho_do_arquivo_customers_csv = "/projetos/data/Tables/customers"
caminho_do_arquivo_d_month_csv = "/projetos/data/Tables/d_month"
caminho_do_arquivo_d_time_csv = "/projetos/data/Tables/d_time"
caminho_do_arquivo_d_week_csv = "/projetos/data/Tables/d_week"
caminho_do_arquivo_d_weekday_csv = "/projetos/data/Tables/d_weekday"
caminho_do_arquivo_d_year_csv = "/projetos/data/Tables/d_year"
caminho_do_arquivo_pix_movements_csv = "/projetos/data/Tables/pix_movements"
caminho_do_arquivo_state_csv = "/projetos/data/Tables/state"
caminho_do_arquivo_transfer_ins_csv = "/projetos/data/Tables/transfer_ins"
caminho_do_arquivo_transfer_outs_csv = "/projetos/data/Tables/transfer_outs"

# Ler o arquivo CSV e criar um DataFrame
df_tmp_accounts = spark.read.csv(caminho_do_arquivo_account_csv, header=True, inferSchema=True)
df_tmp_city = spark.read.csv(caminho_do_arquivo_city_csv, header=True, inferSchema=True)
df_tmp_country = spark.read.csv(caminho_do_arquivo_country_csv, header=True, inferSchema=True)
df_tmp_customers = spark.read.csv(caminho_do_arquivo_customers_csv, header=True, inferSchema=True)
df_tmp_d_month = spark.read.csv(caminho_do_arquivo_d_month_csv, header=True, inferSchema=True)
df_tmp_time = spark.read.csv(caminho_do_arquivo_d_time_csv, header=True, inferSchema=True)
df_tmp_week = spark.read.csv(caminho_do_arquivo_d_week_csv, header=True, inferSchema=True)
df_tmp_weekday = spark.read.csv(caminho_do_arquivo_d_weekday_csv, header=True, inferSchema=True)
df_tmp_year = spark.read.csv(caminho_do_arquivo_d_year_csv, header=True, inferSchema=True)
df_tmp_pix_movements = spark.read.csv(caminho_do_arquivo_pix_movements_csv, header=True, inferSchema=True, sep=',')
df_tmp_state = spark.read.csv(caminho_do_arquivo_state_csv, header=True, inferSchema=True)
df_tmp_transfer_ins = spark.read.csv(caminho_do_arquivo_transfer_ins_csv, header=True, inferSchema=True, sep=';')
df_tmp_transfer_outs = spark.read.csv(caminho_do_arquivo_transfer_outs_csv, header=True, inferSchema=True, sep=';')

# Realizar as conversões de tipo de dado
df_tmp_pix_movements = (
    df_tmp_pix_movements
    .withColumn("id", col("id").cast("bigint"))
    .withColumn("account_id", col("account_id").cast("bigint"))
    .withColumn("pix_amount", col("pix_amount").cast("double"))
)

df_tmp_transfer_ins = (
    df_tmp_transfer_ins
    .withColumn("id", col("id").cast("bigint"))
    .withColumn("account_id", col("account_id").cast("bigint"))
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("transaction_requested_at", col("transaction_requested_at").cast("bigint"))
)

df_tmp_transfer_outs = (
    df_tmp_transfer_outs
    .withColumn("id", col("id").cast("bigint"))
    .withColumn("account_id", col("account_id").cast("bigint"))
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("transaction_requested_at", col("transaction_requested_at").cast("bigint"))
)

# Mostrar o conteúdo do DataFrame na tela
df_tmp_accounts.write \
    .jdbc(url, "tmp_accounts", mode="overwrite", properties=propriedades)

df_tmp_city.write \
    .jdbc(url, "tmp_city", mode="overwrite", properties=propriedades)

df_tmp_country.write \
    .jdbc(url, "tmp_country", mode="overwrite", properties=propriedades)

df_tmp_customers.write \
    .jdbc(url, "tmp_customers", mode="overwrite", properties=propriedades)

df_tmp_d_month.write \
    .jdbc(url, "tmp_d_month", mode="overwrite", properties=propriedades)

df_tmp_time.write \
    .jdbc(url, "tmp_time", mode="overwrite", properties=propriedades)

df_tmp_week.write \
    .jdbc(url, "tmp_week", mode="overwrite", properties=propriedades)

df_tmp_weekday.write \
    .jdbc(url, "tmp_weekday", mode="overwrite", properties=propriedades)

df_tmp_year.write \
    .jdbc(url, "tmp_year", mode="overwrite", properties=propriedades)

#df_tmp_pix_movements.write \
#    .jdbc(url, "tmp_pix_movements", mode="append", properties=propriedades)

df_tmp_state.write \
    .jdbc(url, "tmp_state", mode="overwrite", properties=propriedades)

#df_tmp_transfer_ins.write \
#    .jdbc(url, "tmp_transfer_ins", mode="append", properties=propriedades)

#df_tmp_transfer_outs.write \
#    .jdbc(url, "tmp_transfer_outs", mode="append", properties=propriedades)


# Encerrar a sessão Spark
spark.stop()
