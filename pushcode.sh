#!/bin/bash

# Nome do diretório local
local_directory="/home/thiago/projetos/proj_pyspark_code"

# Especificação do contêiner e destino
container_spec="pyspark:/projetos"

# Imprime o caminho do diretório (debug)
echo "Caminho do diretório local: $local_directory"

# Execute docker-compose down para parar os serviços
docker-compose down
wait

# Lista de arquivos .py no diretório local
py_files=$(find "$local_directory" -maxdepth 1 -type f -name "*.py")

# Imprime a lista de arquivos .py (opcional)
echo "Arquivos .py encontrados:"
echo "$py_files"

# Execute o comando docker cp para copiar os arquivos .py
for file in $py_files; do
    docker cp "$file" "$container_spec"
done

# Execute docker-compose up -d para iniciar novamente os serviços
docker-compose up -d

# Aguarda até que os serviços subam completamente
wait

# Conecta ao contêiner pyspark
docker exec -it pyspark bash
