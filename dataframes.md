# Dataframes

### Características:

- Tabelas com linhas e colunas 
- Imutáveis
- Com schema conhecido
- Linhagem preservada-Preserva as etapas de tranformação do dataframe. 
- Colunas podem ter tipos diferentes
- Existem análises comuns: Agrupar, ordenar, filtrar
- Spark pode otimizar estas analises através de planos de execução.Semelhante ao que acontece no banco de dados.

###   Tipos de dados 
- ByteType
- ShortType
- IntegerType
- LongType
- FloatType
- DoubleType
- DecimalType
- StringType
- BinaryType
- BooleanType
- TimestampType
- DateType
- ArrayType
- MapType
- StructType
- StructField

### Schema 
Você pode deixar para o Spark inferir a partir de
parte dos dados ou Você pode definir o schema

⚠ Dica: 

Definir tem vantagens:
• Tipo correto

• Sem overhead

Quando o spark tenta descobrir leva um tempo de processamento a mais.

⚠ Dica: O processamento de transformação de fato só ocorre quando há uma 
#### Ação: Lazy  Evaluation


### Criar um data frame simples, sem schema

`from pyspark.sql import SparkSession
 `

⚠ Dica: Podemos definir o schema e os valores.

Inicialmente iremos definir apenas os valores.

Os 2 ficaram entre cochetes "[]".Para exibir precisamos chamar o dataframe.show()

### Criar um dataframe simples, sem schema
`from pyspark.sql import SparkSession`

`df1 = spark.createDataFrame([("Pedro",10),("Maria",20),("José",40)]) `

### Show é ação, então tudo o que foi feito anteriormente é executado, lazy
`df1.show()`

```
output
+-----+---+
|   _1| _2|
+-----+---+
|Pedro| 10|
|Maria| 20|
| José| 40|
+-----+---+
```


⚠ Dica: Observamos que foi nomeado as colunas com _1 e _2 automaticamente por inferência de tipo.

Agora vamos criar um dataframe informando o schema.

### Criar df com schema
`schema = "Id INT, Nome STRING"`

`dados = [[1,"Pedro"],[2,"Maria"]]` 

`df2 = spark.createDataFrame(dados, schema)`

`df2.show()`

``` 
output
+---+-----+
| Id| Nome|
+---+-----+
|  1|Pedro|
|  2|Maria|
+---+-----+
``` 
Vamos importar outra biblioteca de agragação sum.

## Com transformação
### Biblioteca de agregação
`from pyspark.sql.functions import sum `
### Atribuindo os valores do schema e devendas
`schema2 = "Produtos STRING, Vendas INT" `
`vendas = [["Caneta",10],["Lápis",20],["Caneta",40]] `

### Criando o dataframe inferindo o schema
`df3 = spark.createDataFrame(vendas , schema2 )` 

### Exibindoo resultado.
> `df3.show()`

```
output
+--------+------+
|Produtos|Vendas|
+--------+------+
|  Caneta|    10|
|   Lápis|    20|
|  Caneta|    40|
+--------+------+
```
`agrupado = df3.groupBy("Produtos").agg(sum("Vendas")) `

`agrupado.show()`

```
output
+--------+-----------+
|Produtos|sum(Vendas)|
+--------+-----------+
|  Caneta|         50|
|   Lápis|         20|
+--------+-----------+
```
### Podemos contatenar as operações, neste caso sem persitir
` df3.groupBy("Produtos").agg(sum("Vendas")).show()`

```
output
+--------+-----------+
|Produtos|sum(Vendas)|
+--------+-----------+
|  Caneta|         50|
|   Lápis|         20|
+--------+-----------+
```

### Selecionar colunas específicas
`df3.select("Produtos").show()`
```
output
+--------+
|Produtos|
+--------+
|  Caneta|
|   Lápis|
|  Caneta|
+--------+
```

 `df3.select("Produtos","Vendas").show()`
```
output
+--------+------+
|Produtos|Vendas|
+--------+------+
|  Caneta|    10|
|   Lápis|    20|
|  Caneta|    40|
+--------+------+
```

### Expressões e select

`from pyspark.sql.functions import expr` 

`df3.select("Produtos", "Vendas", expr("Vendas * 0.2")).show()`

```
output
+--------+------+--------------+
|Produtos|Vendas|(Vendas * 0.2)|
+--------+------+--------------+
|  Caneta|    10|           2.0|
|   Lápis|    20|           4.0|
|  Caneta|    40|           8.0|
+--------+------+--------------+
``` 

### Para ver o schema
`df3.schema`

```
output
StructType(List(StructField(Produtos,StringType,true),StructField(Vendas,IntegerType,true)))
```

### Ver colunas
`df3.columns`
```
output
['Produtos', 'Vendas']
```

### Importar dados definindo schema
### Vamos deixar a data como string de propósito.
`from pyspark.sql.types import *`

`arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING" `

⚠ Dica: O caminho pode mudar, download é a pasta que estão os dados nesse de exemplo.
No nosso caso "/home/sena/download/"

`despachantes = spark.read.csv("/home/sena/download/despachantes.csv", header=False, schema=arqschema) `

`despachantes.show()`

```
output
+---+-------------------+------+-------------+------+----------+
| id|               nome|status|       cidade|vendas|      data|
+---+-------------------+------+-------------+------+----------+
|  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
|  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
|  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
|  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
|  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
|  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
|  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
|  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
|  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
| 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
+---+-------------------+------+-------------+------+----------+

```

### Outro exemplo, inferindo schema, usando load e informado tipo
`desp_autoschema = spark.read.load("/home/sena/download/despachantes.csv",format="csv", sep=",", inferSchema=True, header=False)`

`desp_autoschema.show()`

```
output
+---+-------------------+-----+-------------+---+----------+
|_c0|                _c1|  _c2|          _c3|_c4|       _c5|
+---+-------------------+-----+-------------+---+----------+
|  1|   Carminda Pestana|Ativo|  Santa Maria| 23|2020-08-11|
|  2|    Deolinda Vilela|Ativo|Novo Hamburgo| 34|2020-03-05|
|  3|   Emídio Dornelles|Ativo| Porto Alegre| 34|2020-02-05|
|  4|Felisbela Dornelles|Ativo| Porto Alegre| 36|2020-02-05|
|  5|     Graça Ornellas|Ativo| Porto Alegre| 12|2020-02-05|
|  6|   Matilde Rebouças|Ativo| Porto Alegre| 22|2019-01-05|
|  7|    Noêmia   Orriça|Ativo|  Santa Maria| 45|2019-10-05|
|  8|      Roque Vásquez|Ativo| Porto Alegre| 65|2020-03-05|
|  9|      Uriel Queiroz|Ativo| Porto Alegre| 54|2018-05-05|
| 10|   Viviana Sequeira|Ativo| Porto Alegre|  0|2020-09-05|
+---+-------------------+-----+-------------+---+----------+

```
### O collect() retorna uma lista de dados 
`despachantes.collect()`

```
output
[Row(id=1, nome='Carminda Pestana', status='Ativo', cidade='Santa Maria', vendas=23, data='2020-08-11'), Row(id=2, nome='Deolinda Vilela', status='Ativo', cidade='Novo Hamburgo', vendas=34, data='2020-03-05'), Row(id=3, nome='Emídio Dornelles', status='Ativo', cidade='Porto Alegre', vendas=34, data='2020-02-05'), Row(id=4, nome='Felisbela Dornelles', status='Ativo', cidade='Porto Alegre', vendas=36, data='2020-02-05'), Row(id=5, nome='Graça Ornellas', status='Ativo', cidade='Porto Alegre', vendas=12, data='2020-02-05'), Row(id=6, nome='Matilde Rebouças', status='Ativo', cidade='Porto Alegre', vendas=22, data='2019-01-05'), Row(id=7, nome='Noêmia   Orriça', status='Ativo', cidade='Santa Maria', vendas=45, data='2019-10-05'), Row(id=8, nome='Roque Vásquez', status='Ativo', cidade='Porto Alegre', vendas=65, data='2020-03-05'), Row(id=9, nome='Uriel Queiroz', status='Ativo', cidade='Porto Alegre', vendas=54, data='2018-05-05'), Row(id=10, nome='Viviana Sequeira', status='Ativo', cidade='Porto Alegre', vendas=0, data='2020-09-05')]
```

### Comparando os schemas
`desp_autoschema.schema`

```
output
StructType(List(StructField(_c0,IntegerType,true),StructField(_c1,StringType,true),StructField(_c2,StringType,true),StructField(_c3,StringType,true),StructField(_c4,IntegerType,true),StructField(_c5,StringType,true)))
```

`despachantes.schema`

```
output
StructType(List(StructField(id,IntegerType,true),StructField(nome,StringType,true),StructField(status,StringType,true),StructField(cidade,StringType,true),StructField(vendas,IntegerType,true),StructField(data,StringType,true)))
```
## Aula 2.4
### Importando a biblioteca functions 
`from pyspark.sql import functions as Func `

### Condição lógica com where, obriga´torio Func.col()
`despachantes.select("id","nome","vendas").where(Func.col("vendas") > 20).show()` 

```
output
+---+-------------------+------+
| id|               nome|vendas|
+---+-------------------+------+
|  1|   Carminda Pestana|    23|
|  2|    Deolinda Vilela|    34|
|  3|   Emídio Dornelles|    34|
|  4|Felisbela Dornelles|    36|
|  6|   Matilde Rebouças|    22|
|  7|    Noêmia   Orriça|    45|
|  8|      Roque Vásquez|    65|
|  9|      Uriel Queiroz|    54|
+---+-------------------+------+
```
## ⚠ Dica: IMPORTANTE!!! 
### & é usado para and,
### | é usado para or, 
### ~ é usado para not

Exemplo:
`despachantes.select("id","nome","vendas").where((Func.col("vendas") > 20) & (Func.col("vendas") < 40)).show()`

```
output
+---+-------------------+------+
| id|               nome|vendas|
+---+-------------------+------+
|  1|   Carminda Pestana|    23|
|  2|    Deolinda Vilela|    34|
|  3|   Emídio Dornelles|    34|
|  4|Felisbela Dornelles|    36|
|  6|   Matilde Rebouças|    22|
+---+-------------------+------+
```

### Renomear coluna
`novodf = despachantes.withColumnRenamed("nome","nomes")`

`novodf.columns`

['id', 'nomes', 'status', 'cidade', 'vendas', 'data']

`from pyspark.sql.functions import * `

### Coluna data está como string, vamos transformar em timestamp

` despachantes2 = despachantes.withColumn("data2", to_timestamp(Func.col("data"),"yyyy-MM-dd")) `

`despachantes2.schema `
``` 
output
StructType(List(StructField(id,IntegerType,true),StructField(nome,StringType,true),StructField(status,StringType,true),StructField(cidade,StringType,true),StructField(vendas,IntegerType,true),StructField(data,StringType,true),StructField(data2,TimestampType,true)))
``` 

### Operações sobre datas
`despachantes2.select(year("data")).show()`

```
output
+----------+
|year(data)|
+----------+
|      2020|
|      2020|
|      2020|
|      2020|
|      2020|
|      2019|
|      2019|
|      2020|
|      2018|
|      2020|
+----------+
```

`despachantes2.select(year("data")).distinct().show()`

```
output  
+----------+
|year(data)|
+----------+
|      2018|
|      2019|
|      2020|
+----------+
``` 

`despachantes2.select("nome",year("data")).orderBy("nome").show()`

```
output
+-------------------+----------+
|               nome|year(data)|
+-------------------+----------+
|   Carminda Pestana|      2020|
|    Deolinda Vilela|      2020|
|   Emídio Dornelles|      2020|
|Felisbela Dornelles|      2020|
|     Graça Ornellas|      2020|
|   Matilde Rebouças|      2019|
|    Noêmia   Orriça|      2019|
|      Roque Vásquez|      2020|
|      Uriel Queiroz|      2018|
|   Viviana Sequeira|      2020|
+-------------------+----------+
``` 

`despachantes2.select("data").groupBy(year("data")).count().show()`

```
output
+----------+-----+
|year(data)|count|
+----------+-----+
|      2018|    1|
|      2019|    2|
|      2020|    7|
+----------+-----+
```

 `despachantes2.select(Func.sum("vendas")).show()`

```
output
+-----------+
|sum(vendas)|
+-----------+
|        325|
+-----------+
``` 

### Salvar em vários formatos.São diretórios 
`despachantes.write.format("parquet").save("/home/sena/dfimportparquet")`

`despachantes.write.format("csv").save("/home/sena/dfimportcsv")`

`despachantes.write.format("json").save("/home/sena/dfimportjson")`

`despachantes.write.format("orc").save("/home/sena/dfimportorc")`


### Ler dados
`par = spark.read.format("parquet").load("/home/sena/dfimportparquet/despachantes.parquet")`

````
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/readwriter.py", line 158, in load
    return self._df(self._jreader.load(path))
  File "/opt/spark/python/lib/py4j-0.10.9.3-src.zip/py4j/java_gateway.py", line 1321, in __call__
  File "/opt/spark/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: Path does not exist: file:/home/sena/dfimportparquet/despachantes.parquet
>>> par.show()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'par' is not defined
````

`par.schema()`

`js = spark.read.format("json").load("/home/sena/dfimportjson/despachantes.json")`


``` 
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/readwriter.py", line 158, in load
    return self._df(self._jreader.load(path))
  File "/opt/spark/python/lib/py4j-0.10.9.3-src.zip/py4j/java_gateway.py", line 1321, in __call__
  File "/opt/spark/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: Path does not exist: file:/home/sena/dfimportjson/despachantes.json
``` 


`js.show()`
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'js' is not defined
`js.schema()`


`orc = spark.read.format("orc").load("/home/sena/dfimportorc/despachantes.orc")`

```` 
  File "<stdin>", line 1
    or = spark.read.format("orc").load("/home/sena/dfimportorc/despachantes.orc")
    ^
SyntaxError: invalid syntax
````

`or.show()`
  File "<stdin>", line 1
    or.show()
    ^
SyntaxError: invalid syntax
 `or.schema()` 

## ⚠ Dica: Solução:
 Foi renomeiado os arquivos para os nomes que estava sendo lido.Dessa forma deu certo

 par = spark.read.format("parquet").load("/home/sena/dfimportparquet/despachantes.parquet")

`par.show()`

```
output
+---+-------------------+------+-------------+------+----------+
| id|               nome|status|       cidade|vendas|      data|
+---+-------------------+------+-------------+------+----------+
|  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
|  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
|  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
|  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
|  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
|  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
|  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
|  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
|  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
| 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
+---+-------------------+------+-------------+------+----------+

```
`par.schema`
```
output

StructType(List(StructField(id,IntegerType,true),StructField(nome,StringType,true),StructField(status,StringType,true),StructField(cidade,StringType,true),StructField(vendas,IntegerType,true),StructField(data,StringType,true)))

``` 

### Importando clientes no formato parquet

`clientes_df =  spark.read.format("parquet").load("/home/sena/download/Atividades/Clientes.parquet")`


`from pyspark.sql import functions as Func`

### Mostrando apenas os campos Cliente,Estado e Status 

`clientes_df.select("Cliente","Estado","Status").show()`
```
output 
+--------------------+------+--------+
|             Cliente|Estado|  Status|
+--------------------+------+--------+
|Adelina Buenaventura|    RJ|  Silver|
|        Adelino Gago|    RJ|  Silver|
|     Adolfo Patrício|    PE|  Silver|
|    Adriana Guedelha|    RO|Platinum|
|       Adélio Lisboa|    SE|  Silver|
|       Adérito Bahía|    MA|  Silver|
|       Aida Dorneles|    RN|  Silver|
|   Alarico Quinterno|    AC|  Silver|
|    Alberto Cezimbra|    AM|  Silver|
|    Alberto Monsanto|    RN|    Gold|
|       Albino Canela|    AC|  Silver|
|     Alceste Varanda|    RR|  Silver|
|  Alcides Carvalhais|    RO|  Silver|
|        Aldo Martins|    GO|  Silver|
|   Alexandra Tabares|    MG|  Silver|
|      Alfredo Cotrim|    SC|  Silver|
|     Almeno Figueira|    SC|  Silver|
|      Alvito Peralta|    AM|  Silver|
|     Amadeu Martinho|    RN|  Silver|
|      Amélia Estévez|    PE|  Silver|
+--------------------+------+--------+
```

### Clientes em order descrecente por varios campos

`clie_order_desc =  clientes_df.orderBy(  Func.col("Cliente").desc(), Func.col("Estado").desc(), Func.col("Status").desc() ) `

`clie_order_desc.show()`

```
output
+---------+--------------------+------+------+--------+
|ClienteID|             Cliente|Estado|Genero|  Status|
+---------+--------------------+------+------+--------+
|      250|Joaquina Vasconcelos|    SC|     F|  Silver|
|      249|      Joaquim Mieiro|    TO|     M|  Silver|
|      248|     Joaquim Hurtado|    AP|     M|  Silver|
|      247|          Joana Ataí|    GO|     F|Platinum|
|      246|Jandaíra Albuquerque|    SP|     F|  Silver|
|      245|    Jacinto Dorneles|    MG|     M|  Silver|
|      244|       Iuri Guterres|    GO|     M|  Silver|
|      243|       Israel Canela|    RN|     M|  Silver|
|      242|    Isabel Meirelles|    RO|     F|  Silver|
|      241|    Irene Villanueva|    AC|     F|  Silver|
|      240|      Irene Meireles|    MS|     F|  Silver|
|      239|    Irani Jaguariúna|    AM|     F|  Silver|
|      238|    Iraci Alcoforado|    MS|     F|  Silver|
|      237|   Iracema Rodríguez|    BA|     F|    Gold|
|      236|     Iolanda Rabello|    PB|     F|  Silver|
|      235|          Inês Neres|    CE|     F|  Silver|
|      234|        Ingrit Mayor|    SC|     M|  Silver|
|      233|      Ilduara Chávez|    MT|     F|  Silver|
|      232|      Ifigénia Pires|    PA|     F|  Silver|
|      231|    Ifigénia Lustosa|    PE|     F|  Silver|
+---------+--------------------+------+------+--------+
```

### Ordenando cliente em order decrescente de ID

`clie_orderID_desc = clientes_df.orderBy(  Func.col("ClienteID").desc() ) `

` clie_orderID_desc.show() `

```
output
+---------+--------------------+------+------+--------+
|ClienteID|             Cliente|Estado|Genero|  Status|
+---------+--------------------+------+------+--------+
|      250|Joaquina Vasconcelos|    SC|     F|  Silver|
|      249|      Joaquim Mieiro|    TO|     M|  Silver|
|      248|     Joaquim Hurtado|    AP|     M|  Silver|
|      247|          Joana Ataí|    GO|     F|Platinum|
|      246|Jandaíra Albuquerque|    SP|     F|  Silver|
|      245|    Jacinto Dorneles|    MG|     M|  Silver|
|      244|       Iuri Guterres|    GO|     M|  Silver|
|      243|       Israel Canela|    RN|     M|  Silver|
|      242|    Isabel Meirelles|    RO|     F|  Silver|
|      241|    Irene Villanueva|    AC|     F|  Silver|
|      240|      Irene Meireles|    MS|     F|  Silver|
|      239|    Irani Jaguariúna|    AM|     F|  Silver|
|      238|    Iraci Alcoforado|    MS|     F|  Silver|
|      237|   Iracema Rodríguez|    BA|     F|    Gold|
|      236|     Iolanda Rabello|    PB|     F|  Silver|
|      235|          Inês Neres|    CE|     F|  Silver|
|      234|        Ingrit Mayor|    SC|     M|  Silver|
|      233|      Ilduara Chávez|    MT|     F|  Silver|
|      232|      Ifigénia Pires|    PA|     F|  Silver|
|      231|    Ifigénia Lustosa|    PE|     F|  Silver|
+---------+--------------------+------+------+--------+
``` 

### Melhorando a resposta, ordenando por status

` clientes_df.select("ClienteId","Cliente","Status").where((Func.col("Status") == "Platinum") |  (Func.col("Status")== "Gold") ).orderBy(Func.col("Status")).show()`
```
output
+---------+-------------------+--------+
|ClienteId|            Cliente|  Status|
+---------+-------------------+--------+
|       68|      Carminda Dias|    Gold|
|       10|   Alberto Monsanto|    Gold|
|      166|   Firmino Meireles|    Gold|
|      220|Honorina Villaverde|    Gold|
|      237|  Iracema Rodríguez|    Gold|
|       83|      Cláudio Jorge|    Gold|
|       28|      Anna Carvajal|    Gold|
|      121|    Dionísio Saltão|    Gold|
|       49|      Bento Quintão|    Gold|
|        4|   Adriana Guedelha|Platinum|
|      170|      Flor Vilanova|Platinum|
|      230|    Ibijara Botelho|Platinum|
|      247|         Joana Ataí|Platinum|
+---------+-------------------+--------+
```

### Outra forma, exibindo todos os campos com o coringa "*"

 `clientes_df.select("*").where((Func.col("Status") == "Platinum") | (Func.col("Status")== "Gold") ).orderBy(Func.col("Status")).show() `

 ```
 Output
+---------+-------------------+------+------+--------+
|ClienteID|            Cliente|Estado|Genero|  Status|
+---------+-------------------+------+------+--------+
|       68|      Carminda Dias|    AM|     F|    Gold|
|       10|   Alberto Monsanto|    RN|     M|    Gold|
|      166|   Firmino Meireles|    AM|     M|    Gold|
|      220|Honorina Villaverde|    PE|     F|    Gold|
|      237|  Iracema Rodríguez|    BA|     F|    Gold|
|       83|      Cláudio Jorge|    TO|     M|    Gold|
|       28|      Anna Carvajal|    RS|     F|    Gold|
|      121|    Dionísio Saltão|    PR|     M|    Gold|
|       49|      Bento Quintão|    SP|     M|    Gold|
|        4|   Adriana Guedelha|    RO|     F|Platinum|
|      170|      Flor Vilanova|    CE|     M|Platinum|
|      230|    Ibijara Botelho|    RR|     F|Platinum|
|      247|         Joana Ataí|    GO|     F|Platinum|
+---------+-------------------+------+------+--------+
```
