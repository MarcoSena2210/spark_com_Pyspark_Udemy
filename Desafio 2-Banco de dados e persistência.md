
![DR V](image/Modelo_DW_Atividade2.png )  


# Desafio 2 - Banco de dados e persistência das tabelas

### 1.Crie um banco de dados no DW do Spark  chamado VendasVarejo, e persista todas as tabelas neste banco de dados.


### 2.Crie uma consulta que mostre de cada item
vendido: Nome do Cliente, Data da Venda,
Produto, Vendedor e Valor Total do item.

# RESPOSTAS:
### No prompt de comando chamar o pyspark.
`pyspark`  

### Importar as bibliotecas
>>> `from pyspark.sql import SparkSession`

>>> `from pyspark.sql.types import *`

### Listar as databases
>>> `spark.sql("show databases").show()`

```
Output

22/05/22 18:57:25 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
22/05/22 18:57:25 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
22/05/22 18:57:32 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
22/05/22 18:57:32 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore sena@127.0.1.1
+---------+
|namespace|
+---------+
|  default|
|     desp|
+---------+

```` 
### Criar o banco de dados VendasVarejo

## ⚠ Dica: 
## Embora seja dado um warn, é criado o novo banco.

# Resposta: Item 1  
 >>> `spark.sql("create database VendasVarejo")`

``` 
Output
22/05/22 19:03:31 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
22/05/22 19:03:31 WARN ObjectStore: Failed to get database vendasvarejo, returning NoSuchObjectException
DataFrame[]
>>> spark.sql("show databases").show()
+------------+
|   namespace|
+------------+
|     default|
|        desp|
|vendasvarejo|
+------------+
>>>
```` 

### Informar qual o banco que iremos usar.
>>> `spark.sql("use vendasvarejo").show()`

```
Output

++
||
++
++

>>>
```

# Resposta: Item 2  

## ⚠ Dica: 
## Para persistir as tabelas precisamos, identificar quais tabelas e onde estão. Vamos sair do pyspark ctrl + C ou abrir outro terminal e localizar onde estão.

sena@sena-VirtualBox:~$ `ls -la /home/sena/download/Atividades/`

```
Output 

total 64
drwxrwxr-x 2 sena sena  4096 ago 24  2021 .
drwxrwxr-x 6 sena sena  4096 mai 17 20:55 ..
-rw-rw-r-- 1 sena sena  9311 ago 24  2021 Clientes.parquet
-rw-rw-r-- 1 sena sena 21142 ago 24  2021 ItensVendas.parquet
-rw-rw-r-- 1 sena sena  3490 ago 24  2021 Produtos.parquet
-rw-rw-r-- 1 sena sena 11882 ago 24  2021 Vendas.parquet
-rw-rw-r-- 1 sena sena  2589 ago 24  2021 Vendedores.parquet
sena@sena-VirtualBox:~$
```

## ⚠ Dica: 
## Chamar novamente o pyspark, lê os arquivos e transformá-los em dataframe.


### -- Cliente
>>> `clientes = spark.read.load("/home/sena/download/Atividades/Clientes.parquet")`

###  Estrutura
>>> `clientes.printSchema()`
```
root
 |-- ClienteID: long (nullable = true)
 |-- Cliente: string (nullable = true)
 |-- Estado: string (nullable = true)
 |-- Genero: string (nullable = true)
 |-- Status: string (nullable = true)
``` 

### -- Vendas
>>> `vendas = spark.read.load("/home/sena/download/Atividades/Vendas.parquet")`

###  Estrutura
>>> `vendas.printSchema()`
````
root
 |-- VendasID: long (nullable = true)
 |-- VendedorID: long (nullable = true)
 |-- ClienteID: long (nullable = true)
 |-- Data: string (nullable = true)
 |-- Total: double (nullable = true)
````

### -- ItensVendas
>>> `itensvendas = spark.read.load("/home/sena/download/Atividades/ItensVendas.parquet")`

###  Estrutura
>>> `itensvendas.printSchema()`
```
root
 |-- ProdutoID: long (nullable = true)
 |-- VendasID: long (nullable = true)
 |-- Quantidade: long (nullable = true)
 |-- ValorUnitario: double (nullable = true)
 |-- ValorTotal: double (nullable = true)
 |-- Desconto: string (nullable = true)
 |-- TotalComDesconto: double (nullable = true)
``` 

### -- Produtos
>>> `produtos = spark.read.load("/home/sena/download/Atividades/Produtos.parquet")`

###  Estrutura
>>> `produtos.printSchema()`
```
root
 |-- ProdutoID: long (nullable = true)
 |-- Produto: string (nullable = true)
 |-- Preco: string (nullable = true)
``` 

### -- Vendedores
>>> `vendedores = spark.read.load("/home/sena/download/Atividades/Vendedores.parquet")`

###  Estrutura
>>> `vendedores.printSchema()`
```
root
 |-- VendedorID: long (nullable = true)
 |-- Vendedor: string (nullable = true)
```

## ⚠ Dica: 
## Persistir os dataframes em tabelas no banco vendas verejo.
## Obs: Como tinhamos saído do sparl, e não especificamos qual o banco iamos usae, o comando abaixo criou a tabela Erradamente no banco default.Justamente porque não informamos onde deveria ser criado.  

>>> `clientes.write.saveAsTable("clientes")`

```
22/05/22 20:30:02 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
22/05/22 20:30:02 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
22/05/22 20:30:07 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
22/05/22 20:30:07 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore sena@127.0.1.1
22/05/22 20:30:12 WARN SessionState: METASTORE_FILTER_HOOK will be ignored, since hive.security.authorization.manager is set to instance of HiveAuthorizerFactory.
22/05/22 20:30:13 WARN HiveConf: HiveConf of name hive.internal.ss.authz.settings.applied.marker does not exist
22/05/22 20:30:13 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
22/05/22 20:30:13 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
>>>
```` 

>>> `spark.sql("show tables").show()`

```
Output
+--------+---------+-----------+
|database|tableName|isTemporary|
+--------+---------+-----------+
| default| clientes|      false|
+--------+---------+-----------+
``` 

## Infomando o banco onde deve ser criada as tabelas.
>>> `spark.sql("use VendasVarejo")`

```
Output
22/05/22 20:31:06 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
DataFrame[]
```

### Criando as tabelas 

>>> `vendas.write.saveAsTable("vendas")`

>>> `itensvendas.write.saveAsTable("itensvendas")`

>>> `produtos.write.saveAsTable("produtos")`

>>> `vendedores.write.saveAsTable("vendedores")`

>>> `clientes.write.saveAsTable("clientes")` 

### Verificando se foi criado corretamente.

>>> `spark.sql("show tables").show()`

```
+------------+-----------+-----------+
|    database|  tableName|isTemporary|
+------------+-----------+-----------+
|vendasvarejo|   clientes|      false|
|vendasvarejo|itensvendas|      false|
|vendasvarejo|   produtos|      false|
|vendasvarejo|     vendas|      false|
|vendasvarejo| vendedores|      false|
+------------+-----------+-----------+
```
### Listados alguns registros de clientes.

>>> `spark.sql("select *  from clientes").show()`

```
Output
+---------+--------------------+------+------+--------+
|ClienteID|             Cliente|Estado|Genero|  Status|
+---------+--------------------+------+------+--------+
|        1|Adelina Buenaventura|    RJ|     M|  Silver|
|        2|        Adelino Gago|    RJ|     M|  Silver|
|        3|     Adolfo Patrício|    PE|     M|  Silver|
|        4|    Adriana Guedelha|    RO|     F|Platinum|
|        5|       Adélio Lisboa|    SE|     M|  Silver|
|        6|       Adérito Bahía|    MA|     M|  Silver|
|        7|       Aida Dorneles|    RN|     F|  Silver|
|        8|   Alarico Quinterno|    AC|     M|  Silver|
|        9|    Alberto Cezimbra|    AM|     M|  Silver|
|       10|    Alberto Monsanto|    RN|     M|    Gold|
|       11|       Albino Canela|    AC|     M|  Silver|
|       12|     Alceste Varanda|    RR|     F|  Silver|
|       13|  Alcides Carvalhais|    RO|     M|  Silver|
|       14|        Aldo Martins|    GO|     M|  Silver|
|       15|   Alexandra Tabares|    MG|     F|  Silver|
|       16|      Alfredo Cotrim|    SC|     M|  Silver|
|       17|     Almeno Figueira|    SC|     M|  Silver|
|       18|      Alvito Peralta|    AM|     M|  Silver|
|       19|     Amadeu Martinho|    RN|     M|  Silver|
|       20|      Amélia Estévez|    PE|     F|  Silver|
+---------+--------------------+------+------+--------+
only showing top 20 rows
```
