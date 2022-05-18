

# VIEWS -No spark

Material baseado nas aulas do Prof. Fernando Amaral (Udemy 18052022)



GLOBAIS: VISÍVEIS EM TODAS AS SESSÕES

SESSÃO: VISÍVEIS APENAS NA PRÓPRIA SESSÃO

Podem ser criadas atraves de api dataframe ou comando sql

# Criando uma view usando api do dataframe
despachantes.createOrReplaceTempView("Despachantes_view1")
# Consultando a view
`spark.sql("select * from Despachantes_view1").show()`

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
##  Dica: É possível fazer qualquer transformação nos dados e criar uma View para ser usada como se fosse uma view em um banco relacional. A view criada acima é temporária.


### Vamos criar uma segunda view CLOBAL e fazer a mesma consulta:
`despachantes.createOrReplaceGlobalTempView("Despachantes_view2")`

## Vamos consultar 
`spark.sql("select * from Despachantes_view2").show()`

Dica: Deu erro pois a forma de consultar correta é acrescentando um prefixo global_temp.:

`spark.sql("select * from global_temp.Despachantes_view2").show()`

```
# Erro
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/session.py", line 723, in sql
    return DataFrame(self._jsparkSession.sql(sqlQuery), self._wrapped)
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1304, in __call__
  File "/opt/spark/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: Table or view not found: Despachantes_view2; line 1 pos 14;
'Project [*]
+- 'UnresolvedRelation [Despachantes_view2], [], false

>>>
Traceback (most recent call last):
  File "/opt/spark/python/pyspark/context.py", line 285, in signal_handler
    raise KeyboardInterrupt()
KeyboardInterrupt
>>>
```
## Forma correta de consultar view global
 `spark.sql("select * from global_temp.Despachantes_view2").show()`

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

## Criando um view temporaria usando o sql

`spark.sql("CREATE OR REPLACE TEMP VIEW DESP_VIEW AS select * from Despachantes")`
```

DataFrame[]
>>>
```
## Consultando essa view.
`spark.sql("select * from DESP_VIEW").show()`
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
### Agora para criar uma view global usando o sql: 
` spark.sql("CREATE OR REPLACE GLOBAL TEMP VIEW DESP_VIEW2 AS select * from Despachantes")`

````
DataFrame[]
````

### Consultando essa nova view com sql, da mesma forma precisamos acrescentar o prefixo "global_temp.".
`spark.sql("select * from global_temp.DESP_VIEW2").show()`

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

