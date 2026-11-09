Análisis de accidente de tránsito en Colombia utilizando big data.
¿De que trata el proyecto? 
En este proyecto se realizó un análisis de información relacionada con los accidentes de tránsito en Colombia, utilizando herramientas de big data como python, apache spark y apache Kafka.

La idea principal fue trabajar con datos reales para así conocer mejor cómo se presentaron los accidentes, qué tipos de vehículos aparecen con mayor frecuencia y cómo se distribuyen los registros según su gravedad, departamento, municipio y marca del vehículo. 

Datos utilizados.
Para el desarrollo de este proyecto se utilizó un conjunto de datos públicos obtenido de datos abiertos en Colombia, relacionado con vehículos involucrados en accidentes de tránsito. 

Fuente: https://www.datos.gov.co/

Herramientas Que se se utilizaron.

Python para trabajar con los datos. 
Apache spark para procesar y analizar la información. 
Apache kafka para simular la llegada de los datos en tiempo real. 
Java 11 como en torno de ejecución. 
Unbuntu Linux para trabajar con herramientas instaladas. 

Proceso Batch.

Se cargaron los datos originales, realizando una limpieza de la información y posteriormente se desarrolló un análisis exploratorio para conocer los resultados más importantes. 

Para esta parte se crearon dos archivos:

Análisis_accidentes.py: que permitió cargar, limpiar y transformar los datos. 
Eda_accidentes.py: permitiendo analizar la información y obtener los resultados agrupados por diferentes categorías de vehículos y accidentes. 

Procesamiento streaming.
También se desarrolló una simulación de llegada de información tiempo real mediante apache kafka. Posteriormente, apache spark structured streaming recibe esos datos y realiza su procedimiento y análisis. 

Resultados obtenidos. 
Con el desarrollo de esta actividad fue posible trabajar con un conjunto de datos de gran tamaño, conocer diferentes características de los accidentes de tránsito y poner en práctica herramientas utilizadas en el procesamiento de grandes volúmenes de información. 

Este proyecto también me permitió comprender la diferencia entre procesamientos Batch y el procesamiento streaming, y cómo se utilizan estas tecnologías para analizar información.
