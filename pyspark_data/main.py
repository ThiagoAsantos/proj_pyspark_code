from pyspark.sql import SparkSession

# Inicializar uma sessão Spark
spark = SparkSession.builder.appName("ds_store").getOrCreate()

# Caminho para o arquivo CSV na sua máquina local
caminho_do_arquivo_csv = "/projetos/data/Tables/accounts"

# Ler o arquivo CSV e criar um DataFrame
data_frame = spark.read.csv(caminho_do_arquivo_csv, header=True, inferSchema=True)

# Mostrar o conteúdo do DataFrame na tela
data_frame.show()

# Encerrar a sessão Spark
spark.stop()
