# Spark - JOIN com DataFrames e SQL

## Lendo arquivo reclamações com schema definido,sem cabeçalho 

>>> `recschema = "idrec INT, datarec STRING, iddesp INT"`

>>> `reclamacoes = spark.read.csv("/home/sena/download/reclamacoes.csv", header=False, schema=recschema)`

## Salvando a tabela 

>>> `reclamacoes.write.saveAsTable("reclamacoes")`

## Exibinso os dados
>>> `spark.sql("select * from reclamacoes").show()`

```
Output
+-----+----------+------+
|idrec|   datarec|iddesp|
+-----+----------+------+
|    1|2020-09-12|     2|
|    2|2020-09-11|     2|
|    3|2020-10-05|     4|
|    4|2020-10-02|     5|
|    5|2020-12-06|     5|
|    6|2020-01-09|     5|
|    7|2020-01-05|     9|
+-----+----------+------+
```

### Inner join
spark.sql("select reclamacoes.*, despachantes.nome from 
despachantes inner join reclamacoes  on (despachantes.id = reclamacoes.iddesp)").show()

>>> spark.sql("select reclamacoes.*,despachantes.nome from despachantes inner join reclamacoes on ( despachantes.id = reclamacoes.iddesp) ").show()
+-----+----------+------+-------------------+
|idrec|   datarec|iddesp|               nome|
+-----+----------+------+-------------------+
|    1|2020-09-12|     2|    Deolinda Vilela|
|    2|2020-09-11|     2|    Deolinda Vilela|
|    3|2020-10-05|     4|Felisbela Dornelles|
|    4|2020-10-02|     5|     Graça Ornellas|
|    5|2020-12-06|     5|     Graça Ornellas|
|    6|2020-01-09|     5|     Graça Ornellas|
|    7|2020-01-05|     9|      Uriel Queiroz|
+-----+----------+------+-------------------+

### righ join deve trazer o mesmo resultado, pois todas as reclamações tem um despachante

>>> spark.sql("select reclamacoes.*,despachantes.nome from despachantes right join reclamacoes on ( despachantes.id = reclamacoes.iddesp) ").show()
+-----+----------+------+-------------------+
|idrec|   datarec|iddesp|               nome|
+-----+----------+------+-------------------+
|    1|2020-09-12|     2|    Deolinda Vilela|
|    2|2020-09-11|     2|    Deolinda Vilela|
|    3|2020-10-05|     4|Felisbela Dornelles|
|    4|2020-10-02|     5|     Graça Ornellas|
|    5|2020-12-06|     5|     Graça Ornellas|
|    6|2020-01-09|     5|     Graça Ornellas|
|    7|2020-01-05|     9|      Uriel Queiroz|
+-----+----------+------+-------------------+


### Um left join traz mais colunas e campos nulos, pois alguns despachantes não tem reclações

>>> `spark.sql("select reclamacoes.*, despachantes.nome,despachantes.id from despachantes left join reclamacoes  on (despachantes.id = reclamacoes.iddesp)").show()`

## ⚠ Dica:
 O despachante decódigo 1, não tem reclamações. 
```
Output
+-----+----------+------+-------------------+---+
|idrec|   datarec|iddesp|               nome| id|
+-----+----------+------+-------------------+---+
| null|      null|  null|   Carminda Pestana|  1|
|    2|2020-09-11|     2|    Deolinda Vilela|  2|
|    1|2020-09-12|     2|    Deolinda Vilela|  2|
| null|      null|  null|   Emídio Dornelles|  3|
|    3|2020-10-05|     4|Felisbela Dornelles|  4|
|    6|2020-01-09|     5|     Graça Ornellas|  5|
|    5|2020-12-06|     5|     Graça Ornellas|  5|
|    4|2020-10-02|     5|     Graça Ornellas|  5|
| null|      null|  null|   Matilde Rebouças|  6|
| null|      null|  null|    Noêmia   Orriça|  7|
| null|      null|  null|      Roque Vásquez|  8|
|    7|2020-01-05|     9|      Uriel Queiroz|  9|
| null|      null|  null|   Viviana Sequeira| 10|
+-----+----------+------+-------------------+---+
```
## ⚠ Dica: 
## Fazendo as mesmas operações usando agora dataframe, usando api do spark.
## A grande diferença é que estamos usandoos objetos em memória

### inner join
>>> `despachantes.join(reclamacoes, despachantes.id == reclamacoes.iddesp, "inner" ).select("idrec","datarec","nome" ).show()`
```
Output
+-----+----------+-------------------+
|idrec|   datarec|               nome|
+-----+----------+-------------------+
|    1|2020-09-12|    Deolinda Vilela|
|    2|2020-09-11|    Deolinda Vilela|
|    3|2020-10-05|Felisbela Dornelles|
|    4|2020-10-02|     Graça Ornellas|
|    5|2020-12-06|     Graça Ornellas|
|    6|2020-01-09|     Graça Ornellas|
|    7|2020-01-05|      Uriel Queiroz|
+-----+----------+-------------------+
```

### right
>>> `despachantes.join(reclamacoes,despachantes.id == reclamacoes.iddesp, "right").select("idrec","datarec","iddesp","nome").show()`
```
Output
+-----+----------+------+-------------------+
|idrec|   datarec|iddesp|               nome|
+-----+----------+------+-------------------+
|    1|2020-09-12|     2|    Deolinda Vilela|
|    2|2020-09-11|     2|    Deolinda Vilela|
|    3|2020-10-05|     4|Felisbela Dornelles|
|    4|2020-10-02|     5|     Graça Ornellas|
|    5|2020-12-06|     5|     Graça Ornellas|
|    6|2020-01-09|     5|     Graça Ornellas|
|    7|2020-01-05|     9|      Uriel Queiroz|
+-----+----------+------+-------------------+
```

### left
>>> `despachantes.join(reclamacoes,despachantes.id == reclamacoes.iddesp, "left").select("idrec","datarec","iddesp","id","nome").show()`
```
Output 
+-----+----------+------+---+-------------------+
|idrec|   datarec|iddesp| id|               nome|
+-----+----------+------+---+-------------------+
| null|      null|  null|  1|   Carminda Pestana|
|    2|2020-09-11|     2|  2|    Deolinda Vilela|
|    1|2020-09-12|     2|  2|    Deolinda Vilela|
| null|      null|  null|  3|   Emídio Dornelles|
|    3|2020-10-05|     4|  4|Felisbela Dornelles|
|    6|2020-01-09|     5|  5|     Graça Ornellas|
|    5|2020-12-06|     5|  5|     Graça Ornellas|
|    4|2020-10-02|     5|  5|     Graça Ornellas|
| null|      null|  null|  6|   Matilde Rebouças|
| null|      null|  null|  7|    Noêmia   Orriça|
| null|      null|  null|  8|      Roque Vásquez|
|    7|2020-01-05|     9|  9|      Uriel Queiroz|
| null|      null|  null| 10|   Viviana Sequeira|
+-----+----------+------+---+-------------------+
```
# Usando o shell spark-sql
## Vamos sair do pyspark (ctrl + D) e no prompt digitar spark-sql

```
sena@sena-VirtualBox:~$ spark-sql
22/05/23 22:13:04 WARN Utils: Your hostname, sena-VirtualBox resolves to a loopback address: 127.0.1.1; using 10.0.2.15 instead (on interface enp0s3)
22/05/23 22:13:04 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/opt/spark/jars/spark-unsafe_2.12-3.1.2.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
22/05/23 22:13:05 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
22/05/23 22:13:12 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
22/05/23 22:13:12 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
22/05/23 22:13:17 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
22/05/23 22:13:17 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore sena@127.0.1.1
Spark master: local[*], Application Id: local-1653354788894
spark-sql>
```

### show databases;
spark-sql> `show databases;`

```
Output

default
desp
vendasvarejo
Time taken: 4.607 seconds, Fetched 3 row(s)
spark-sql>
```
# Dica: Os comandos são finalizados por ponto e virgula 
### use desp
### show tables;
### Select * from Despachantes;
### Select nome,vendas from Despachantes;

spark-sql> `use desp;`

``` 
22/05/23 22:21:45 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
Time taken: 3.359 seconds
```
## Mostrando as tabelas
spark-sql> `show tables;`
desp    despachantes    false
desp    despachantes_ng false
desp    reclamacoes     false
Time taken: 1.072 seconds, Fetched 3 row(s)

spark-sql> `Select * from Despachantes; `

```
Output 
1       Carminda Pestana        Ativo   Santa Maria     23      2020-08-11
2       Deolinda Vilela Ativo   Novo Hamburgo   34      2020-03-05
3       Emídio Dornelles        Ativo   Porto Alegre    34      2020-02-05
4       Felisbela Dornelles     Ativo   Porto Alegre    36      2020-02-05
5       Graça Ornellas  Ativo   Porto Alegre    12      2020-02-05
6       Matilde Rebouças        Ativo   Porto Alegre    22      2019-01-05
7       Noêmia   Orriça Ativo   Santa Maria     45      2019-10-05
8       Roque Vásquez   Ativo   Porto Alegre    65      2020-03-05
9       Uriel Queiroz   Ativo   Porto Alegre    54      2018-05-05
10      Viviana Sequeira        Ativo   Porto Alegre    0       2020-09-05
Time taken: 4.574 seconds, Fetched 10 row(s)
```

spark-sql> `Select nome,vendas from Despachantes; `
``` 
Carminda Pestana        23
Deolinda Vilela 34
Emídio Dornelles        34
Felisbela Dornelles     36
Graça Ornellas  12
Matilde Rebouças        22
Noêmia   Orriça 45
Roque Vásquez   65
Uriel Queiroz   54
Viviana Sequeira        0
Time taken: 0.505 seconds, Fetched 10 row(s)
spark-sql>
```
# Listando as vendas > 20
# condição lógica
Select nome,vendas from Despachantes where vendas > 20;

# ⚠ Dica: 
## Deu o erro abaixo, pois esquecemos do "ponto e virgula" no final do comando, e # foi fechado o shell. # O quê possibilitou o spark-sql não saber identificar qual o # # banco-tabelas continha a tabela despachante.
# Pois ele buscou no default     

```
spark-sql> Select nome,vendas from Despachantes where vendas > 20;
Error in query: Table or view not found: Despachantes; line 1 pos 24;
'Project ['nome, 'vendas]
+- 'Filter ('vendas > 20)
   +- 'UnresolvedRelation [Despachantes], [], false

spark-sql> show tables;
22/05/23 22:31:28 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
default clientes        false

Time taken: 3.857 seconds, Fetched 1 row(s)
```
## Solução: Informar qualbanco desejamos usar.

spark-sql> `use desp;`
```
Time taken: 0.092 seconds
```

spark-sql> `show tables;`
```
Output 
desp    despachantes    false
desp    despachantes_ng false
desp    reclamacoes     false
Time taken: 0.085 seconds, Fetched 3 row(s)
```

spark-sql> `Select nome,vendas from Despachantes where vendas > 20;`

```
Output
Carminda Pestana        23
Deolinda Vilela 34
Emídio Dornelles        34
Felisbela Dornelles     36
Matilde Rebouças        22
Noêmia   Orriça 45
Roque Vásquez   65
Uriel Queiroz   54
Time taken: 4.304 seconds, Fetched 8 row(s)
spark-sql>
```

### agrupar
spark-sql>`Select cidade,sum(vendas) from Despachantes group by cidade order by 2 desc;`

```
Porto Alegre    223
Santa Maria     68
Novo Hamburgo   34
Time taken: 11.447 seconds, Fetched 3 row(s)
spark-sql>
```


### join
spark-sql> `select reclamacoes.*, despachantes.nome from despachantes inner join reclamacoes  on (despachantes.id = reclamacoes.iddesp);`

```
Output
1       2020-09-12      2       Deolinda Vilela
2       2020-09-11      2       Deolinda Vilela
3       2020-10-05      4       Felisbela Dornelles
4       2020-10-02      5       Graça Ornellas
5       2020-12-06      5       Graça Ornellas
6       2020-01-09      5       Graça Ornellas
7       2020-01-05      9       Uriel Queiroz
Time taken: 6.155 seconds, Fetched 7 row(s)
spark-sql>
```      
spark-sql> `select reclamacoes.*, despachantes.nome from despachantes left join reclamacoes  on (despachantes.id = reclamacoes.iddesp);`

```
Output
NULL    NULL    NULL    Carminda Pestana
2       2020-09-11      2       Deolinda Vilela
1       2020-09-12      2       Deolinda Vilela
NULL    NULL    NULL    Emídio Dornelles
3       2020-10-05      4       Felisbela Dornelles
6       2020-01-09      5       Graça Ornellas
5       2020-12-06      5       Graça Ornellas
4       2020-10-02      5       Graça Ornellas
NULL    NULL    NULL    Matilde Rebouças
NULL    NULL    NULL    Noêmia   Orriça
NULL    NULL    NULL    Roque Vásquez
7       2020-01-05      9       Uriel Queiroz
NULL    NULL    NULL    Viviana Sequeira
Time taken: 6.421 seconds, Fetched 13 row(s)
spark-sql>
```

#teste de pesistência
from pyspark.sql import SparkSession

spark.sql("use desp").show()
#mostrar que a tabela ainda existe
spark.sql("select * from despachantes").show()
despachantes.show()

#o resultado de uma consulta sem um show gera um dataframe
despachantes = spark.sql("select * from despachantes")
despachantes.show()


