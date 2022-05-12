Desafio 1

No cenário hipotético de uma empresa Ciclismo e Art do ramo de ciclismo, que trabalha com 10 produtos, tem no seu quadro funcional 10 vendedores.
Atualmente possue 250 clientes.
Costuma caracterizar os clientes em 3 níveis de fidelização.
Sendo:
Gold
Platinum


Deseja fazer algumas análises para tomada de decisão conforme abaixo.


Diagrama Relacional, neste caso tem o foco apenas facilitar o entendimento do négocio, mesmo porque dever ser feita as análise usado dataframe.  

![DR V](image/dr_desafio1.png)  




# 1 Crie uma consulta que mostre, nesta ordem, Cliente, Estados e Status



## Ir para o local onde estão os arquivos
sena@sena-VirtualBox:~/download$ `cd Atividades`
## Verificando o que tem na pasta 
sena@sena-VirtualBox:~/download/Atividades$ `ls -la`

``` 
total 64
drwxrwxr-x 2 sena sena  4096 ago 24  2021 .
drwxrwxr-x 6 sena sena  4096 mai  7 19:57 ..
-rw-rw-r-- 1 sena sena  9311 ago 24  2021 Clientes.parquet
-rw-rw-r-- 1 sena sena 21142 ago 24  2021 ItensVendas.parquet
-rw-rw-r-- 1 sena sena  3490 ago 24  2021 Produtos.parquet
-rw-rw-r-- 1 sena sena 11882 ago 24  2021 Vendas.parquet
-rw-rw-r-- 1 sena sena  2589 ago 24  2021 Vendedores.parquet
```
## *No prompt de comando chamar o pyspark 

sena@sena-VirtualBox:~/ms-pyspark$ `pyspark`

![tela inicial do spark](image/tela-Inicial_spark.png)

# Resposta:
clientes_df =  spark.read.format("parquet").load("/home/sena/download/Atividades/Clientes.parquet")

vendas_df =  spark.read.format("parquet").load("/home/sena/download/Atividades/Vendas.parquet")


from pyspark.sql import functions as Func

 clientes_df.select("Cliente","Estado","Status").show()
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

# 2 Crie uma consulta que mostre apenas os clientes do Status “platinum” e “gold”


# Resposta:

`clientes_df.select("ClienteId","Cliente","Status").where((Func.col("Status") == "Platinum") |  (Func.col("Status")== "Gold") ).show() `

```
+---------+-------------------+--------+
|ClienteId|            Cliente|  Status|
+---------+-------------------+--------+
|        4|   Adriana Guedelha|Platinum|
|       10|   Alberto Monsanto|    Gold|
|       28|      Anna Carvajal|    Gold|
|       49|      Bento Quintão|    Gold|
|       68|      Carminda Dias|    Gold|
|       83|      Cláudio Jorge|    Gold|
|      121|    Dionísio Saltão|    Gold|
|      166|   Firmino Meireles|    Gold|
|      170|      Flor Vilanova|Platinum|
|      220|Honorina Villaverde|    Gold|
|      230|    Ibijara Botelho|Platinum|
|      237|  Iracema Rodríguez|    Gold|
|      247|         Joana Ataí|Platinum|
+---------+-------------------+--------+
```

# Melhorando a resposta, ordenando por status
` clientes_df.select("ClienteId","Cliente","Status").where((Func.col("Status") == "Platinum") |  (Func.col("Status")== "Gold") ).orderBy(Func.col("Status")).show()  `
```
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

# 3 Demostre quanto cada Status de Clientes representa em vendas?

## Lendo o arquivo de vendas 
`vendas_df = spark.read.format("parquet").load("/home/sena/download/Atividades/Vendas.parquet")`

`vendas_df.show()`

```
+--------+----------+---------+---------+--------+
|VendasID|VendedorID|ClienteID|     Data|   Total|
+--------+----------+---------+---------+--------+
|       1|         1|       91| 1/1/2019|  8053.6|
|       2|         6|      185| 1/1/2020|   150.4|
|       3|         7|       31| 2/1/2020|  6087.0|
|       4|         5|       31| 2/1/2019| 13828.6|
|       5|         5|       31| 3/1/2018|26096.66|
|       6|         5|       31| 4/1/2020| 18402.0|
|       7|         5|       31| 6/1/2019|  7524.2|
|       8|         5|      186| 6/1/2019| 12036.6|
|       9|         7|       91| 6/1/2020| 2804.75|
|      10|         2|      202| 6/1/2020|  8852.0|
|      11|         7|       58| 8/1/2019|16545.25|
|      12|         7|       58| 9/1/2018|11411.88|
|      13|         7|       58|10/1/2019| 15829.7|
|      14|         3|      249|12/1/2020| 6154.36|
|      15|         4|      249|12/1/2018| 3255.08|
|      16|         7|      192|13/1/2020| 2901.25|
|      17|         2|       79|13/1/2019| 15829.7|
|      18|        10|       79|14/1/2019|16996.36|
|      19|        10|      191|14/1/2019|   155.0|
|      20|         9|      218|15/1/2018|  131.75|
+--------+----------+---------+---------+--------+
```
>>> vendas_df.show(2)
+--------+----------+---------+--------+------+
|VendasID|VendedorID|ClienteID|    Data| Total|
+--------+----------+---------+--------+------+
|       1|         1|       91|1/1/2019|8053.6|
|       2|         6|      185|1/1/2020| 150.4|
+--------+----------+---------+--------+------+
only showing top 2 rows

>>> clientes_df.show(2)
+---------+--------------------+------+------+------+
|ClienteID|             Cliente|Estado|Genero|Status|
+---------+--------------------+------+------+------+
|        1|Adelina Buenaventura|    RJ|     M|Silver|
|        2|        Adelino Gago|    RJ|     M|Silver|
+---------+--------------------+------+------+------+
only showing top 2 rows



vendas.join(clientes, vendas.ClienteID ==clientes.ClienteID ).groupBy(clientes.Status).agg(sum("Total")).orderBy(Func.col("sum(Total)").desc())


vendas_df.join(clientes, vendas_df.ClienteID ==clientes_df.ClienteID ).groupBy(clientes_df.Status).agg(sum("Total")).orderBy(Func.col("sum(Total)").desc()).show()


xxxxxxxxxxxxxxx

vendas_agrupada = vendas_df.join(clientes_df, vendas_df.ClienteID == clientes_df.ClienteID).groupBy(clientes_df.Status).agg(sum("Total")).orderBy(Func.col(sum("Total")).orderBy(Func.col("sum(Total)").desc())

> vendas_por_status = 
 vendas_df.join(clientes_df,vendas_df.ClienteID == clientes_df.ClienteID).groupBy(clientes_df.Status).agg(sum("Total")).orderBy(Func.col("sum(Total)").desc())

Traceback (most recent call last):

XXXXXX

cond = [df.name == df3.name, df.age == df3.age]
df.join(df3, cond, 'outer').select(df.name, df3.age).collect()

XXXXXX



`condicao =[vendas_df.ClienteID == clientes_df.ClienteID]`
`vendas_df.join(clientes_df,condicao,'inner').select(clientes_df.Status,vendas_df.Total).show()`

.groupBy(clientes.Status)

````
+------+--------+
|Status|   Total|
+------+--------+
|Silver|  8053.6|
|Silver|   150.4|
|Silver|  6087.0|
|Silver| 13828.6|
|Silver|26096.66|
|Silver| 18402.0|
|Silver|  7524.2|
|Silver| 12036.6|
|Silver| 2804.75|
|Silver|  8852.0|
|Silver|16545.25|
|Silver|11411.88|
|Silver| 15829.7|
|Silver| 6154.36|
|Silver| 3255.08|
|Silver| 2901.25|
|Silver| 15829.7|
|Silver|16996.36|
|Silver|   155.0|
|Silver|  131.75|
+------+--------+
``` 

# *** Falta agrupar

vendas_df.join(clientes_df,condicao,'inner').select(clientes_df.Status,vendas_df.Total).agg(sum("Total")).show()

.groupBy(clientes_df.Status)