from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

spark = SparkSession.builder \
    .appName("EDA_Accidentes_Colombia") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ruta = "/home/vboxuser/Proyecto_Accidentes/resultados/datos_limpios"

salida = "/home/vboxuser/Proyecto_Accidentes/resultados"

df = spark.read.csv(ruta, header=True, inferSchema=True)

print("\n========== ANALISIS EXPLORATORIO ==========")

print("\nTOTAL DE REGISTROS:")
print(df.count())

analisis = {
    "tipo_vehiculo": "resumen_tipo_vehiculo",
    "gravedad_accidente": "resumen_gravedad",
    "departamento_accidente": "resumen_departamento",
    "marca_vehiculo": "resumen_marcas",
    "municipio_accidente": "resumen_municipios"
}

for columna, carpeta in analisis.items():

    print("\n==========", columna.upper(), "==========")

    resultado = df.groupBy(columna) \
        .agg(count("*").alias("total_accidentes")) \
        .orderBy(desc("total_accidentes"))

    resultado.show(10, truncate=False)

    resultado.write.mode("overwrite") \
        .option("header", True) \
        .csv(salida + "/" + carpeta)

print("\n========== ANALISIS FINALIZADO ==========")

spark.stop()
