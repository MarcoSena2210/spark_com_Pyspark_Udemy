
# Machine Learning-Classificação Binária

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

## Importar as bibliotecas 
 
`from pyspark.ml.feature import RFormula`

### Classificador de arvore de decisão que é umdos mais simples "DecisionTreeClassifier"
`from pyspark.ml.classification import DecisionTreeClassifier`

### Para usar uma métrica de classificação binária "BinaryClassificationEvaluator"
`from pyspark.ml.evaluation import BinaryClassificationEvaluator`

## Importando os dados
`churn = spark.read.csv("/home/sena/download/Churn.csv",inferSchema=True, header=True, sep=";") `

## listando
`churn.show(3)`
```
Output 
+-----------+---------+------+---+------+-------+-------------+---------+--------------+---------------+------+
|CreditScore|Geography|Gender|Age|Tenure|Balance|NumOfProducts|HasCrCard|IsActiveMember|EstimatedSalary|Exited|
+-----------+---------+------+---+------+-------+-------------+---------+--------------+---------------+------+
|        619|   France|Female| 42|     2|      0|            1|        1|             1|       10134888|     1|
|        608|    Spain|Female| 41|     1|8380786|            1|        0|             1|       11254258|     0|
|        502|   France|Female| 42|     8|1596608|            3|        1|             0|       11393157|     1|
+-----------+---------+------+---+------+-------+-------------+---------+--------------+---------------+------+
only showing top 3 rows
```
# Precisamos definir o nome das colunas para fit (ajuste), coluna label (classe) e os dados inválidos que vamos dizer para ignorar.

## A classe é igual coluna "Exited" e o ~ .(til e o ponto) significa que todo os outros atributos serão usadas como  variáveis dependentes.


### RFormula faz vários tipos de transformações, "features" e "label" são os nomes das coluans que desejamos trabalhar:

`formula = RFormula(formula="Exited ~ . ",featuresCol="features", labelCol="label", handleInvalid="skip")`

### Aplicamos a formula e selecionamos apenas as colunas que interessam
`churn_trans = formula.fit(churn).transform(churn).select("features","label")`


### Resultado, precisamos truncar o resultado pois será enorme.
`churn_trans.show(truncate=False)`
```
Output
+----------------------------------------------------------------+-----+
|features                                                        |label|
+----------------------------------------------------------------+-----+
|[619.0,1.0,0.0,0.0,42.0,2.0,0.0,1.0,1.0,1.0,1.0134888E7]        |1.0  |
|[608.0,0.0,0.0,0.0,41.0,1.0,8380786.0,1.0,0.0,1.0,1.1254258E7]  |0.0  |
|[502.0,1.0,0.0,0.0,42.0,8.0,1596608.0,3.0,1.0,0.0,1.1393157E7]  |1.0  |
|(11,[0,1,4,5,7,10],[699.0,1.0,39.0,1.0,2.0,9382663.0])          |0.0  |
|[850.0,0.0,0.0,0.0,43.0,2.0,1.2551082E7,1.0,1.0,1.0,790841.0]   |0.0  |
|[645.0,0.0,0.0,1.0,44.0,8.0,1.1375578E7,2.0,1.0,0.0,1.4975671E7]|1.0  |
|[822.0,1.0,0.0,1.0,50.0,7.0,0.0,2.0,1.0,1.0,100628.0]           |0.0  |
|[376.0,0.0,1.0,0.0,29.0,4.0,1.1504674E7,4.0,1.0,0.0,1.1934688E7]|1.0  |
|[501.0,1.0,0.0,1.0,44.0,4.0,1.4205107E7,2.0,0.0,1.0,749405.0]   |0.0  |
|[684.0,1.0,0.0,1.0,27.0,2.0,1.3460388E7,1.0,1.0,1.0,7172573.0]  |0.0  |
|[528.0,1.0,0.0,1.0,31.0,6.0,1.0201672E7,2.0,0.0,0.0,8018112.0]  |0.0  |
|[497.0,0.0,0.0,1.0,24.0,3.0,0.0,2.0,1.0,0.0,7639001.0]          |0.0  |
|[476.0,1.0,0.0,0.0,34.0,10.0,0.0,2.0,1.0,0.0,2626098.0]         |0.0  |
|(11,[0,1,4,5,7,10],[549.0,1.0,25.0,5.0,2.0,1.9085779E7])        |0.0  |
|[635.0,0.0,0.0,0.0,35.0,7.0,0.0,2.0,1.0,1.0,6595165.0]          |0.0  |
|[616.0,0.0,1.0,1.0,45.0,3.0,1.4312941E7,2.0,0.0,1.0,6432726.0]  |0.0  |
|[653.0,0.0,1.0,1.0,58.0,1.0,1.3260288E7,1.0,1.0,0.0,509767.0]   |1.0  |
|[549.0,0.0,0.0,0.0,24.0,9.0,0.0,2.0,1.0,1.0,1440641.0]          |0.0  |
|(11,[0,3,4,5,7,10],[587.0,1.0,45.0,6.0,1.0,1.5868481E7])        |0.0  |
|[726.0,1.0,0.0,0.0,24.0,6.0,0.0,2.0,1.0,1.0,5472403.0]          |0.0  |
+----------------------------------------------------------------+-----+
only showing top 20 rows
``` 

Dica: esse algoritimo já transformou as colunas categoricas em um vetor e aplicou Adhoc in coding trasformando os dados da classe em números.Isso é fantastico.

## Vamos agora dividir nossos dados em treino e teste, na proporção 70 para treino e 30 para teste.


churnTreino, churnTeste = churn_trans.randomSplit([0.7,0.3])

## listando
`print(churnTreino.count())`
```
Output
7053
```

```print(churnTeste.count())`
```
Output
2947
```

## Criando nosso modelo usando o DecisionTreeClassifier
`dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")`

`modelo = dt.fit(churnTreino)`

## Criando a previsão
`previsao = modelo.transform(churnTeste)`

`previsao.show()`

```
+--------------------+-----+--------------+--------------------+----------+
|            features|label| rawPrediction|         probability|prediction|
+--------------------+-----+--------------+--------------------+----------+
|(11,[0,1,3,4,7,10...|  0.0|  [137.0,28.0]|[0.83030303030303...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
|(11,[0,1,4,5,7,10...|  1.0|  [35.0,236.0]|[0.12915129151291...|       1.0|
|(11,[0,1,4,5,7,10...|  1.0|  [32.0,102.0]|[0.23880597014925...|       1.0|
|(11,[0,1,4,5,7,10...|  1.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
|(11,[0,1,4,5,7,10...|  1.0|  [32.0,102.0]|[0.23880597014925...|       1.0|
|(11,[0,1,4,5,7,10...|  0.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|  [137.0,28.0]|[0.83030303030303...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
|(11,[0,1,4,5,7,10...|  1.0|  [32.0,102.0]|[0.23880597014925...|       1.0|
|(11,[0,1,4,5,7,10...|  0.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
|(11,[0,1,4,5,7,10...|  0.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
|(11,[0,1,4,5,7,10...|  1.0|[4397.0,505.0]|[0.89698082415340...|       0.0|
+--------------------+-----+--------------+--------------------+----------+
only showing top 20 rows
```

`previsao.show(truncate=False)`

```
Output
-----------------+----------+
|features                                                 |label|rawPrediction |probability                             |prediction|
+---------------------------------------------------------+-----+--------------+----------------------------------------+----------+
|(11,[0,1,3,4,7,10],[668.0,1.0,1.0,46.0,2.0,2938802.0])   |0.0  |[137.0,28.0]  |[0.8303030303030303,0.1696969696969697] |0.0       |
|(11,[0,1,4,5,7,10],[411.0,1.0,36.0,10.0,1.0,1.2069435E7])|0.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
|(11,[0,1,4,5,7,10],[476.0,1.0,40.0,4.0,2.0,1.8254704E7]) |0.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
|(11,[0,1,4,5,7,10],[507.0,1.0,35.0,1.0,2.0,9213154.0])   |0.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
|(11,[0,1,4,5,7,10],[512.0,1.0,31.0,7.0,2.0,4932607.0])   |0.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
|(11,[0,1,4,5,7,10],[515.0,1.0,28.0,9.0,2.0,9414175.0])   |0.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
|(11,[0,1,4,5,7,10],[523.0,1.0,73.0,7.0,2.0,1308839.0])   |1.0  |[35.0,236.0]  |[0.12915129151291513,0.8708487084870848]|1.0       |
|(11,[0,1,4,5,7,10],[535.0,1.0,49.0,3.0,1.0,6182041.0])   |1.0  |[32.0,102.0]  |[0.23880597014925373,0.7611940298507462]|1.0       |
|(11,[0,1,4,5,7,10],[537.0,1.0,38.0,10.0,1.0,5233797.0])  |1.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
|(11,[0,1,4,5,7,10],[547.0,1.0,49.0,2.0,1.0,6546693.0])   |1.0  |[32.0,102.0]  |[0.23880597014925373,0.7611940298507462]|1.0       |
|(11,[0,1,4,5,7,10],[549.0,1.0,34.0,4.0,2.0,1.3946357E7]) |0.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
|(11,[0,1,4,5,7,10],[556.0,1.0,46.0,10.0,2.0,1.0918424E7])|0.0  |[137.0,28.0]  |[0.8303030303030303,0.1696969696969697] |0.0       |
|(11,[0,1,4,5,7,10],[591.0,1.0,42.0,10.0,2.0,1.7109922E7])|0.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
|(11,[0,1,4,5,7,10],[601.0,1.0,37.0,5.0,1.0,207086.0])    |0.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
|(11,[0,1,4,5,7,10],[619.0,1.0,48.0,4.0,1.0,1809496.0])   |1.0  |[32.0,102.0]  |[0.23880597014925373,0.7611940298507462]|1.0       |
|(11,[0,1,4,5,7,10],[620.0,1.0,41.0,9.0,2.0,8885247.0])   |0.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
|(11,[0,1,4,5,7,10],[626.0,1.0,35.0,3.0,1.0,8019036.0])   |0.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
|(11,[0,1,4,5,7,10],[639.0,1.0,38.0,5.0,2.0,9371638.0])   |0.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
|(11,[0,1,4,5,7,10],[641.0,1.0,35.0,4.0,2.0,1.2598618E7]) |0.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
|(11,[0,1,4,5,7,10],[641.0,1.0,40.0,9.0,1.0,1.5164866E7]) |1.0  |[4397.0,505.0]|[0.8969808241534067,0.10301917584659323]|0.0       |
+---------------------------------------------------------+-----+--------------+----------------------------------------+----------+
only showing top 20 rows

```





## Avaliando a performance,usando accuracy
`avaliar = BinaryClassificationEvaluator(rawPredictionCol="prediction",labelCol="label",metricName="areaUnderROC")`

`areaUnderROC = avaliar.evaluate(previsao)`

`print(areaUnderROC)`

```
Output
0.6876886734789269
```

## Dica : Avaliação pela acuracia quanto mais próximo de 1 melhor o seu modelo.