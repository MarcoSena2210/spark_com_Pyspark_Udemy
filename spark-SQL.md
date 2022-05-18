
#  SPARK SQL

Material baseado nas aulas do Prof. Fernando Amaral (Udemy 18052022)

### Dica:
    
    Spark Utiliza o Metastore do Hive
    Não é preciso ter o Hive instalado para usar o Spark

# Tabela: 
• Persistente

• Objeto Tabular que reside em um banco
de dados

• Pode ser gerenciado e consultado
utilizando SQL

• Totalmente interoperável com DataFrame

• Ex: Você pode transformar um DataFrame
que importamos (Parquet, json, orc csv)
em tabela

```
TABELA <---> DATAFRAME
```
Pode ser trasformado dataframe em tabela e o inverso.


# VIEW

• Mesmo conceito de banco de dados relacionais

• São um “alias” para uma tabela (por exemplo,
vendas_rs pode mostrar vendas do estado já com
filtro aplicado)

• Não contém dados

## As *Views* podem ser:

LOBAIS: VISÍVEIS EM TODAS AS SESSÕES

SESSÃO: VISÍVEIS APENAS NA PRÓPRIA SESSÃO

## As tabelas podem ser gerenciadas e não gerenciadas
### Gerenciadas ou manager: 
  
    Spark gerencia dados e metadados 
    Armazenadas no warehouse do spark
    Se excluirmos, tudo é apagado (dados e metadados)

### Não Gerenciadas (External): Spark
    
    Apenas gerencia metadados
    Informamos onde a tabela está (arquivo, por exemplo orc)
    Se excluirmos, Spark só exclui os metadados, dados permanecem onde estavam


# Vamos para prática chamando o pyspark no prompt de comando.
`pyspark` 



# Importando as bibliotecas

>>> `from pyspark.sql import SparkSession`
>>> `from pyspark.sql.types import *`

# mostrar bancos de dados e tabelas
>>> `spark.sql("show databases").show()`

``` 
22/05/18 13:10:27 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does n                                                                                        ot exist
22/05/18 13:10:27 WARN HiveConf: HiveConf of name hive.stats.retries.wait does n                                                                                        ot exist
22/05/18 13:10:45 WARN ObjectStore: Version information not found in metastore.                                                                                         hive.metastore.schema.verification is not enabled so recording the schema versio                                                                                        n 2.3.0
22/05/18 13:10:45 WARN ObjectStore: setMetaStoreSchemaVersion called but recordi                                                                                        ng version is disabled: version = 2.3.0, comment = Set by MetaStore sena@127.0.1                                                                                        .1
22/05/18 13:10:45 WARN ObjectStore: Failed to get database default, returning No                                                                                        SuchObjectException
+---------+
|namespace|
+---------+
|  default|
+---------+
```


### criar banco de dados desp que representa os despachantes
spark.sql("create database desp")

>>> `spark.sql("create database desp")`

``` 
22/05/18 13:16:19 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
22/05/18 13:16:19 WARN ObjectStore: Failed to get database desp, returning NoSuchObjectException
DataFrame[]

*Obs:Embora tenha dado um WARN, o banco foi criado
>>> spark.sql("show databases").show()
+---------+
|namespace|
+---------+
|  default|
|     desp|
+---------+
``` 

### Vamos informar para o banco qual a database iremostrabalhar. 
`spark.sql("use desp").show()`

``` 
>>> spark.sql("use desp").show()
++
||
++
++

>>>
``` 

### Agora vamos criar uma tabela gerenciada a partir do dataframe despachante.

`arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING" `

## Dica: Vamos lê o arquivo e transformar em um dataframe.Se você for reproduzir, lembre de trocar a pasta para o seu user.No meca caso "sena"  

despachantes = spark.read.csv("/home/sena/download/despachantes.csv", header=False, schema=arqschema)

>>>` despachantes = spark.read.csv("/home/sena/download/despachantes.csv", header=False, schema=arqschema)`

>>> `despachantes.show()`

``` 
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

### Agora vamos transformar em uma tabela no banco de dados.Conforme abaixo, entre os parentese devemos colocar o nome da tabela no banco de dados.

# Dica: Se não tivessemos criado o banco e indicado seu uso com o comando "use desp"  , seria criado no banco default,  

`despachantes.write.saveAsTable("Despachantes")`

### *Criando a tabela

>>>`despachantes.write.saveAsTable("Despachantes")`
``` 
22/05/18 13:31:46 WARN SessionState: METASTORE_FILTER_HOOK will be ignored, since hive.security.authorization.manager is set to instance of HiveAuthorizerFactory.
22/05/18 13:31:47 WARN HiveConf: HiveConf of name hive.internal.ss.authz.settings.applied.marker does not exist
22/05/18 13:31:47 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
22/05/18 13:31:47 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
>>>
``` 

### Verificando que a tabela existe, listando os registros...
spark.sql("select * from despachantes").show()

>>>` spark.sql("select * from despachantes").show()`
``` 
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

# Exibindo as tabelas do banco aberto 
`spark.sql("show tables").show()`

``` 
+--------+------------+-----------+
|database|   tableName|isTemporary|
+--------+------------+-----------+
|    desp|despachantes|      false|
+--------+------------+-----------+
``` 

# Vamos mudar o banco de dados para o default, e repetir a consulta.  
`spark.sql("use default").show()`

``` 
++
||
++
++
>>>
``` 


# executa novamente e mostrar que da erro
`spark.sql("select * from despachantes").show()`

``` 
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/session.py", line 723, in sql
    return DataFrame(self._jsparkSession.sql(sqlQuery), self._wrapped)
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1304, in __call__
  File "/opt/spark/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: Table or view not found: despachantes; line 1 pos 14;
'Project [*]
+- 'UnresolvedRelation [despachantes], [], false

>>>
``` 

## Dica observamos que foi gerado um erro, pois não existe a tabela despachante no banco default 

# voltar ao nosso banco de dados
`spark.sql("use desp").show()`

``` 
++
||
++
++

>>>
```  

### Quando é criado uma tabela existe o parametro mode:
   overwrite -> Para recriar um nova tabela

   append    -> Para noveos registros   


## overwrite e append
`despachantes.write.mode("overwrite").saveAsTable("Despachantes")`

## Dica: Se sairmos da sessão agora o dataframe será apagado.E quanto a tabela o que acontecerá?
Vamos sair com o ctrl +d (fechar a sessão pyspark)

sena@sena-VirtualBox:~$

# Entrar no pyspark novamente e verificar 
sena@sena-VirtualBox:~$ `pyspark`

``` 
Python 3.10.4 (main, Apr  2 2022, 09:04:19) [GCC 11.2.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
22/05/18 17:27:28 WARN Utils: Your hostname, sena-VirtualBox resolves to a loopback address: 127.0.1.1; using 10.0.2.15 instead (on interface enp0s3)
22/05/18 17:27:28 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/opt/spark/jars/spark-unsafe_2.12-3.1.2.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
22/05/18 17:27:29 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.1.2
      /_/

Using Python version 3.10.4 (main, Apr  2 2022 09:04:19)
Spark context Web UI available at http://10.0.2.15:4040
Spark context available as 'sc' (master = local[*], app id = local-1652905653507).
SparkSession available as 'spark'.
>>>
``` 

# Verificando o dataframe 
`despachantes.show()`

``` 
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'despachantes' is not defined
>>>
``` 

# Dica: Não existe
# habilitando o uso do banco desp = despachantes
`spark.sql("use desp").show()`

``` 
22/05/18 17:30:40 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
22/05/18 17:30:40 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
22/05/18 17:30:49 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
22/05/18 17:30:49 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore sena@127.0.1.1
22/05/18 17:30:50 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
++
||
++
++
```  

# Verificando se a tabela ainda existe

`spark.sql("select * from despachantes").show()` 

``` 
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

>>>
``` 

## Dica: 
Concluimos que a tabela foi persistida, mesmo após o fechamento da sessão a tabela permanece e o dataframe não.
Essa tabela é gerenciada.Se apagarmos com o drop table ela e seus metadados serão apagados.


Como perdemos o dataframe, se dermos um comando para ler a tabela é retornado um "dataframe'.

### Exemplo:
### O resultado de uma consulta sem um show gera um dataframe
`despachantes = spark.sql("select * from despachantes")`
`despachantes.show()`

```
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

### Agora vamos criar uma tabela Não gerenciada.
## Lembrando que tabela externa ou não gerenciada, se trata de um arquivo em disco que o spark pode gerencia-lo.

## salvamos novamente no formato parquet, em outro diretorio
`despachantes.write.format("parquet").save("/home/sena/desparquet")`


Basicamente para criar uma tabela não gerenciada o que muda é <b>informar o caminho</b>. Quando você informa o caminho o spark já entende que se trata de um alias para uma tabela externa ou não gerenciada

 `despachantes.write.option("path", "/home/sena/desparquet").saveAsTable("Despachantes_ng")`



### como saber se uma tabela é gerenciada ou não?
# Dica: Usando o create, podemos observar que despachantes não mostra o caminho.Ele traz o comando de criação da tabela.

``spark.sql("show create table Despachantes").show(truncate=False)`

```
+-------------------------------------------------------------------------------------------------------------------------------------------------------+
|createtab_stmt                                                                                                                                         |
+-------------------------------------------------------------------------------------------------------------------------------------------------------+
|CREATE TABLE `desp`.`Despachantes` (
  `id` INT,
  `nome` STRING,
  `status` STRING,
  `cidade` STRING,
  `vendas` INT,
  `data` STRING)
USING parquet
|
+-------------------------------------------------------------------------------------------------------------------------------------------------------

```


# Agora vamos vê a tabela externa despachantes_ng nos mostra! indicando que é não gerenciada
`spark.sql("show create table Despachantes_ng").show(truncate=False)`

```
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|createtab_stmt                                                                                                                                                                                  |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|CREATE TABLE `desp`.`Despachantes_ng` (
  `id` INT,
  `nome` STRING,
  `status` STRING,
  `cidade` STRING,
  `vendas` INT,
  `data` STRING)
USING parquet
LOCATION 'file:/home/sena/desparquet'
|
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

>>>
```
## Dica: Observe que temos o location (o caminho).O que significa que é uma tabela não gerenciada ou externa



#outra forma:
`spark.catalog.listTables()`
``` 
[Table(name='despachantes', database='desp', description=None, tableType='MANAGED', isTemporary=False),
 Table(name='despachantes_ng', database='desp', description=None, tableType='EXTERNAL', isTemporary=False)]
>>>
```


# Mas onde está a tabela gerenciada?
## Em /home/sena/spark-warehouse/desp.db/
/home/sena/spark-warehouse/desp.db/despachantes
