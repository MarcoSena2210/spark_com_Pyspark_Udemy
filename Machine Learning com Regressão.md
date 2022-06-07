
# Machine Learning com Regressão

Material resumido do curso do Prof. Fernando Amaral formação spark com pysaprk

Problema: Prevê a potência (HP) de um carro baseado em um conjunto de dados.Vamos utlizar um subconjunto 
dos dados apresentados 

1) Vamos criar um regressão linear
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

## Importar as bibliotecas de regressão
`from pyspark.ml.regression import LinearRegression, RandomForestRegressor`
`

## Biblioteca para avaliar o modelo e verificar quanto de bom o modelo foi.

`from pyspark.ml.evaluation import RegressionEvaluator`

## Vai transformar os dados categoricos em dados numéricos 
`from pyspark.ml.feature import VectorAssembler`

## Agora vamos importar o arquivo carros.csv, deixar o spark inferir o schema, observe que esse arquivo tem cabeçalho e que está separado por ponto e virgula.

## Em seguida listar os primeiros 5 registros para conhecermos os dados. 
`Carros_temp = spark.read.csv("/home/sena/download/Carros.csv",inferSchema=True, header=True, sep=";")`

## Listando
`Carros_temp.show(5)`

```
Output
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
## Vamos criar outro dataframe selecionando apenas as colunas que desejamos trabalhar.
## Temos 3 varáveis independentes para criar o HP
`Carros = Carros_temp.select("Consumo","Cilindros","Cilindradas","HP")`
`Carros.show()`
```
Output
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
|    178|        6|       1676|123|
|    164|        8|       2758|180|
|    173|        8|       2758|180|
|    152|        8|       2758|180|
|    104|        8|        472|205|
|    104|        8|        460|215|
|    147|        8|        440|230|
|    324|        4|        787| 66|
|    304|        4|        757| 52|
|    339|        4|        711| 65|
+-------+---------+-----------+---+
only showing top 20 rows
```

## Vamos criar um vetor de característica para criar o nosso modelo com um atributo novo.
## Iremos usar o método vector assembler e informar os parâmetros.

1-Informar quais são as variáveis independentes, passados no "inputCols" e o outro outputCol que é o nome que essa nova coluna será terá, aqui reemos chamar característica.

## Criando vetor de caracteristicas
veccaracteristicas = VectorAssembler(inputCols=[("Consumo"),("Cilindros"),("Cilindradas")],outputCol="caracteristicas")

## Aplicado em dados de treino no vetor carro, 
`Carros = veccaracteristicas.transform(Carros)`

# Dica: Oberservamos que foi criada a nova coluna chamada caracteristicas contendo um vetor com os valores das 3 colunas independentes. 
`Carros.show(5)`
```
Output
+-------+---------+-----------+---+-----------------+
|Consumo|Cilindros|Cilindradas| HP|  caracteristicas|
+-------+---------+-----------+---+-----------------+
|     21|        6|        160|110| [21.0,6.0,160.0]|
|     21|        6|        160|110| [21.0,6.0,160.0]|
|    228|        4|        108| 93|[228.0,4.0,108.0]|
|    214|        6|        258|110|[214.0,6.0,258.0]|
|    187|        8|        360|175|[187.0,8.0,360.0]|
+-------+---------+-----------+---+-----------------+
only showing top 5 rows
```
## A próxima etapa é dividir os dados em treino e teste.
## Dica: Os dados de teste deve ser dados diferentes dentro do modelo.Vamos fazer essa divisão com o método "randomSplit" passando 70% para treino e 30% para teste.


`CarrosTreino, CarrosTeste = Carros.randomSplit([0.7,0.3])`
## Listando
`print(CarrosTreino.count())`
```
Output
19
```
`print(CarrosTeste.count())`
```
Output
13
```
### Agora estamos prontos para criar o modelo e fazer as previsões. Vamos criar o primeiro modelo de REGRESSÃO LINEAR.Precisamos passar os dos parametros, as colunas independentes quechamamos de caracteristicae e a coluna label que no nosso caso é a coluna HP.

1º Criar o objeto do modelo de regressão linear.Chamado reglin (regressao lienar) e usaremos o método Fit para fazer o treino.

reglin = `LinearRegression(featuresCol="caracteristicas", labelCol="HP")`

modelo = reglin.fit(CarrosTreino)

Dica: Irá dar alguns warn's mas irá funcionar corretamente

```
Output
22/06/04 17:51:45 WARN Instrumentation: [78e30bc3] regParam is zero, which might cause numerical instability and overfitting.
22/06/04 17:51:47 WARN BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS
22/06/04 17:51:47 WARN BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeRefBLAS
22/06/04 17:51:48 WARN LAPACK: Failed to load implementation from: com.github.fommil.netlib.NativeSystemLAPACK
22/06/04 17:51:48 WARN LAPACK: Failed to load implementation from: com.github.fommil.netlib.NativeRefLAPACK
```
# Agora podemos prever - método transform do modelo

`previsao = modelo.transform(CarrosTeste)`

`previsao.show()`
```
Output
+-------+---------+-----------+---+------------------+------------------+
|Consumo|Cilindros|Cilindradas| HP|   caracteristicas|        prediction|
+-------+---------+-----------+---+------------------+------------------+
|     26|        4|       1203| 91| [26.0,4.0,1203.0]| 64.42965576425537|
|    104|        8|        472|205| [104.0,8.0,472.0]| 221.4871679909533|
|    152|        8|       2758|180|[152.0,8.0,2758.0]|193.06905894088044|
|    155|        8|        318|150| [155.0,8.0,318.0]| 222.3782734876018|
|    164|        8|       2758|180|[164.0,8.0,2758.0]|192.84263280446538|
|    181|        6|        225|105| [181.0,6.0,225.0]|148.14116909389986|
|    192|        8|        400|175| [192.0,8.0,400.0]|220.69324208257837|
|    197|        6|        145|175| [197.0,6.0,145.0]| 148.8020813846084|
|    214|        4|        121|109| [214.0,4.0,121.0]|  73.9043696856043|
|    215|        4|       1201| 97|[215.0,4.0,1201.0]| 60.88751446086569|
|    228|        4|       1408| 95|[228.0,4.0,1408.0]|58.150938756908786|
|    304|        4|        757| 52| [304.0,4.0,757.0]| 64.55180390535789|
|    324|        4|        787| 66| [324.0,4.0,787.0]| 63.81337183410942|
+-------+---------+-----------+---+------------------+------------------+
```

## Dica: Usaremos agora o valores de CarroTeste.Observamos que para os valores temos as previsões.exemplo.
```
HP  91 foi previsto 64;
HP 205 foi previsto 221;
HP 180 foi previsto 193;
HP 150 foi previsto 222;
HP 180 foi previsto 192;

Importante é identificar o quanto estão acertivas essas previsões.Para isso é preciso usar algumas métricas de performance. Quanto menor a diferença melhor. A melhor forma de avaliar isso é a gente criar mais de um modelo  e comparar as performanse.
```

# Para avaliar performance, vamos usar RegressionEvaluator e passar como parâmetro a coluna previsão "prediction", a classe "HP" e a métrica "rmse",  
`avaliar = RegressionEvaluator(predictionCol="prediction", labelCol="HP",metricName='rmse')`

`rmse = avaliar.evaluate(previsao)`

## Listando o resultado
`print(rmse)`
```
Output
34.242931885212776
``` 

## Agora vamos usar outro modelo usando o método chamado London Landon Forest.

## Criando outro modelo e comparar a performance
`rfreg = RandomForestRegressor(featuresCol="caracteristicas", labelCol="HP") `

## Criando o 2º modelode treinamento
`modelo2 = rfreg.fit(CarrosTreino)`
```
Output
22/06/04 18:46:54 WARN DecisionTreeMetadata: DecisionTree reducing maxBins from 32 to 19 (= number of training instances)
```

`previsao2 = modelo2.transform(CarrosTeste)`
## Listando a 2ª previsão
`previsao2.show()`
```
Output
+-------+---------+-----------+---+------------------+------------------+
|Consumo|Cilindros|Cilindradas| HP|   caracteristicas|        prediction|
+-------+---------+-----------+---+------------------+------------------+
|     26|        4|       1203| 91| [26.0,4.0,1203.0]| 144.7210714285714|
|    104|        8|        472|205| [104.0,8.0,472.0]|215.24853174603177|
|    152|        8|       2758|180|[152.0,8.0,2758.0]|192.58186507936512|
|    155|        8|        318|150| [155.0,8.0,318.0]|200.17757936507942|
|    164|        8|       2758|180|[164.0,8.0,2758.0]|198.73186507936512|
|    181|        6|        225|105| [181.0,6.0,225.0]|             120.0|
|    192|        8|        400|175| [192.0,8.0,400.0]|161.36579365079368|
|    197|        6|        145|175| [197.0,6.0,145.0]|106.36857142857143|
|    214|        4|        121|109| [214.0,4.0,121.0]| 98.76107142857143|
|    215|        4|       1201| 97|[215.0,4.0,1201.0]| 95.37607142857141|
|    228|        4|       1408| 95|[228.0,4.0,1408.0]| 81.32249999999999|
|    304|        4|        757| 52| [304.0,4.0,757.0]|           80.7725|
|    324|        4|        787| 66| [324.0,4.0,787.0]| 77.32249999999999|
+-------+---------+-----------+---+------------------+------------------+
```

## Avaliar a previsão 2
`rmse2 = avaliar.evaluate(previsao2)`

## Listando
`print(rmse2)`

```
Output
30.87160586030543
```

`print(rmse)`
```
Output
34.242931885212776
```

# Concluímos que o nosso 2º modelo teve uma performace melhor, pois seu resultado foi menor.

# Lembrando que a métrica rmse nos informa que quanto menor a diferença melhor. 










