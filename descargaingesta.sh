#!/bin/bash

# URL del archivo a descargar
URL="https://github.com/fpineyro/homework-0/blob/master/starwars.csv"

# Directorio local donde se descargará el archivo
LOCAL_DIR="/home/hadoop/landing"

# Ruta en HDFS donde se moverá el archivo
HDFS_DIR="/ingest"

# Nombre del archivo a descargar
FILENAME=$(basename "$URL")

# Descarga el archivo
wget -P "$LOCAL_DIR" "$URL"

# Mueve el archivo a HDFS
hdfs dfs -put "$LOCAL_DIR/$FILENAME" "$HDFS_DIR"

# Mensaje de confirmación
echo "Archivo $FILENAME descargado y movido a HDFS en $HDFS_DIR"