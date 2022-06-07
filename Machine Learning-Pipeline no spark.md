# Machine Learning-Pipeline no spark

Material resumido do curso do Prof. Fernando Amaral formação spark com pysaprk

Problema: Efetuar a técnica declassificação para verificar se o cliente cadastrado vai sair ou não.
Então é uma classificação binária.

1) Vamos criar classificador
2) Fazer a previsão
3) Avaliar modelo
4) Testar o modelo

Mão na massa

### Entrar no payspark  
sena@sena-VirtualBox:~$ `pyspark`

```
Python 3.10.4 (main, Apr  2 2022, 09:04:19) [GCC 11.2.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
22/06/04 03:58:08 WARN Utils: Your hostname, sena-VirtualBox resolves to a loopback address: 127.0.1.1; using 10.0.2.15 instead (on interface enp0s3)
22/06/04 03:58:08 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/opt/spark/jars/spark-unsafe_2.12-3.1.3.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
22/06/04 03:58:20 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.1.3
      /_/

Using Python version 3.10.4 (main, Apr  2 2022 09:04:19)
Spark context Web UI available at http://10.0.2.15:4040
Spark context available as 'sc' (master = local[*], app id = local-1654333179677).
SparkSession available as 'spark'.

```

## Importando as bibliotecas
`from pyspark.ml.regression import LinearRegression`

`from pyspark.ml.evaluation import RegressionEvaluator`

`from pyspark.ml.feature import VectorAssembler`

## Lendo o arquivo carro.csv
Carros_temp = spark.read.csv("/home/sena/download/Carros.csv",inferSchema=True, header=True, sep=";")

## Listando
`Carros_temp.show(5)`
```
+-------+---------+-----------+---------------+----+-----+---------+-----------+-------+-----------+---+
|Consumo|Cilindros|Cilindradas|RelEixoTraseiro|Peso|Tempo|TipoMotor|Transmissao|Marchas|Carburadors| HP|
+-------+---------+-----------+---------------+----+-----+---------+-----------+-------+-----------+---+
|     21|        6|        160|             39| 262| 1646|        0|          1|      4|          4|110|
|     21|        6|        160|             39|2875| 1702|        0|          1|      4|          4|110|
|    228|        4|        108|            385| 232| 1861|        1|          1|      4|          1| 93|
|    214|        6|        258|            308|3215| 1944|        1|          0|      3|          1|110|
|    187|        8|        360|            315| 344| 1702|        0|          0|      3|          2|175|
+-------+---------+-----------+---------------+----+-----+---------+-----------+-------+-----------+---+
only showing top 5 rows
```

##  Escolhendo apenas as colunas que vamos usar
`Carros = Carros_temp.select("Consumo","Cilindros","Cilindradas","HP")`

## Listando
`Carros.show(10)` 

```
+-------+---------+-----------+---+
|Consumo|Cilindros|Cilindradas| HP|
+-------+---------+-----------+---+
|     21|        6|        160|110|
|     21|        6|        160|110|
|    228|        4|        108| 93|
|    214|        6|        258|110|
|    187|        8|        360|175|
|    181|        6|        225|105|
|    143|        8|        360|245|
|    244|        4|       1467| 62|
|    228|        4|       1408| 95|
|    192|        6|       1676|123|
+-------+---------+-----------+---+
only showing top 10 rows
```


### dividir em treino e teste
CarrosTreino, CarrosTeste = Carros.randomSplit([0.7,0.3])


## criar vetor de caracteristicas
`veccaracteristicas = VectorAssembler(inputCols=[("Consumo"),("Cilindros"),("Cilindradas")],outputCol="caracteristicas")`

## aplicamos em dados de treino
`vec_CarrosTreino = veccaracteristicas.transform(CarrosTreino)`

### Listando
`vec_CarrosTreino.show()`
```
+-------+---------+-----------+---+------------------+
|Consumo|Cilindros|Cilindradas| HP|   caracteristicas|
+-------+---------+-----------+---+------------------+
|     21|        6|        160|110|  [21.0,6.0,160.0]|
|     26|        4|       1203| 91| [26.0,4.0,1203.0]|
|    104|        8|        460|215| [104.0,8.0,460.0]|
|    143|        8|        360|245| [143.0,8.0,360.0]|
|    147|        8|        440|230| [147.0,8.0,440.0]|
|    152|        8|        304|150| [152.0,8.0,304.0]|
|    164|        8|       2758|180|[164.0,8.0,2758.0]|
|    173|        8|       2758|180|[173.0,8.0,2758.0]|
|    181|        6|        225|105| [181.0,6.0,225.0]|
|    187|        8|        360|175| [187.0,8.0,360.0]|
|    192|        8|        400|175| [192.0,8.0,400.0]|
|    197|        6|        145|175| [197.0,6.0,145.0]|
|    214|        6|        258|110| [214.0,6.0,258.0]|
|    215|        4|       1201| 97|[215.0,4.0,1201.0]|
|    228|        4|        108| 93| [228.0,4.0,108.0]|
|    228|        4|       1408| 95|[228.0,4.0,1408.0]|
|    244|        4|       1467| 62|[244.0,4.0,1467.0]|
|    273|        4|         79| 66|  [273.0,4.0,79.0]|
|    304|        4|        757| 52| [304.0,4.0,757.0]|
|    304|        4|        951|113| [304.0,4.0,951.0]|
+-------+---------+-----------+---+------------------+
```
## Criado modelo 
## A coluna caracteristicas foi criada pelo VectorAssembler

`reglin = LinearRegression(featuresCol="caracteristicas", labelCol="HP")`

`modelo = reglin.fit(vec_CarrosTreino)`

```
output
22/06/06 20:35:38 WARN Instrumentation: [f3f32a71] regParam is zero, which might cause numerical instability and overfitting.
```

## Pipelines permite criar um fluxo do processo
`from pyspark.ml import Pipeline`

## 2 fases (veccaracteristicas,reglin)
`pipeline = Pipeline(stages=[veccaracteristicas,reglin])`

## Vamos agora criar o modelo  
`pipelineModel = pipeline.fit(Carros)`
```
Output
22/06/06 20:43:39 WARN Instrumentation: [c891862e] regParam is zero, which might cause numerical instability and overfitting.
```

## Previsão
`previsao = pipelineModel.transform(Carros)`

`previsao.show(15)`
```
Output
+-------+---------+-----------+---+------------------+------------------+
|Consumo|Cilindros|Cilindradas| HP|   caracteristicas|        prediction|
+-------+---------+-----------+---+------------------+------------------+
|     21|        6|        160|110|  [21.0,6.0,160.0]|162.32154816816646|
|     21|        6|        160|110|  [21.0,6.0,160.0]|162.32154816816646|
|    228|        4|        108| 93| [228.0,4.0,108.0]| 82.51715587712931|
|    214|        6|        258|110| [214.0,6.0,258.0]|141.86680518718754|
|    187|        8|        360|175| [187.0,8.0,360.0]|202.93528239714834|
|    181|        6|        225|105| [181.0,6.0,225.0]| 145.4980634611832|
|    143|        8|        360|245| [143.0,8.0,360.0]|   207.41448530972|
|    244|        4|       1467| 62|[244.0,4.0,1467.0]| 69.69282676584851|
|    228|        4|       1408| 95|[228.0,4.0,1408.0]| 71.80767356085781|
|    192|        6|       1676|123|[192.0,6.0,1676.0]|132.42483285541724|
|    178|        6|       1676|123|[178.0,6.0,1676.0]|133.85003378214458|
|    164|        8|       2758|180|[164.0,8.0,2758.0]|185.52180807776818|
|    173|        8|       2758|180|[173.0,8.0,2758.0]|184.60560748201488|
|    152|        8|       2758|180|[152.0,8.0,2758.0]| 186.7434088721059|
|    104|        8|        472|205| [104.0,8.0,472.0]| 210.4620247994542|
+-------+---------+-----------+---+------------------+------------------+
only showing top 15 rows
```

### Embora tem sido um fluxo pequeno, pequeno com apenas dois estágios.A gente pode utilizar esse conceito para criar pipelines bastante grandes aqui que acontecem eventualmente. 