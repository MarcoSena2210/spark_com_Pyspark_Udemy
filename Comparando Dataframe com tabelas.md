Comparando Dataframe com tabelas

Material baseado nas aulas do Prof. Fernando Amaral (Udemy 18052022)


## Parte III consultas

`from pyspark.sql import functions as Func`

`from pyspark.sql.functions import sum`

>>> `spark.sql("use desp")`

### Mostrar a tabela
spark.sql("Select * from Despachantes").show()

```
Output
DataFrame[]
```
>>> `park.sql("Select * from Despachantes").show()`
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
### Armazenado o resultado a despachantees e exibindo seus dados 

>>> `despachantes = spark.sql("Select * from Despachantes")`

#### listando

>>> `despachantes.show()`
```
Output

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

# Mostrar algumas colunas

>>> `spark.sql("Select nome,vendas from Despachantes").show()`
```
+-------------------+------+
|               nome|vendas|
+-------------------+------+
|   Carminda Pestana|    23|
|    Deolinda Vilela|    34|
|   Emídio Dornelles|    34|
|Felisbela Dornelles|    36|
|     Graça Ornellas|    12|
|   Matilde Rebouças|    22|
|    Noêmia   Orriça|    45|
|      Roque Vásquez|    65|
|      Uriel Queiroz|    54|
|   Viviana Sequeira|     0|
+-------------------+------+
```

>>> `despachantes.select("nome","vendas").show()`
```
Output
+-------------------+------+
|               nome|vendas|
+-------------------+------+
|   Carminda Pestana|    23|
|    Deolinda Vilela|    34|
|   Emídio Dornelles|    34|
|Felisbela Dornelles|    36|
|     Graça Ornellas|    12|
|   Matilde Rebouças|    22|
|    Noêmia   Orriça|    45|
|      Roque Vásquez|    65|
|      Uriel Queiroz|    54|
|   Viviana Sequeira|     0|
+-------------------+------+
```


### Condição lógica
### Vendas maior que 20
>>> `spark.sql("Select nome,vendas from Despachantes where vendas > 20").show()`
```
Output

+-------------------+------+
|               nome|vendas|
+-------------------+------+
|   Carminda Pestana|    23|
|    Deolinda Vilela|    34|
|   Emídio Dornelles|    34|
|Felisbela Dornelles|    36|
|   Matilde Rebouças|    22|
|    Noêmia   Orriça|    45|
|      Roque Vásquez|    65|
|      Uriel Queiroz|    54|
+-------------------+------+
```
>>> `despachantes.select("id","nome","vendas").where(Func.col("vendas") > 20).show()`

```
Output
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
## Listando as vendas por cidade m ordem decrescente

>>> `spark.sql("Select cidade,sum(vendas) from Despachantes group by cidade by 2 desc").show()`


## ⚠ Dica: 
## "by 2 desc" o 2 represnta o index da coluna 
``` 
[Stage 21:==>                                                    (10 + 1)
+-------------+-----------+
|       cidade|sum(vendas)|
+-------------+-----------+
| Porto Alegre|        223|
|  Santa Maria|         68|
|Novo Hamburgo|         34|
+-------------+-----------+
``` 

>>> `despachantes.groupBy("cidade").agg(sum("vendas")).orderBy(Func.col("sas)").desc()).show()`

```
Output 
+-------------+-----------+
|       cidade|sum(vendas)|
+-------------+-----------+
| Porto Alegre|        223|
|  Santa Maria|         68|
|Novo Hamburgo|         34|
+-------------+-----------+
```
