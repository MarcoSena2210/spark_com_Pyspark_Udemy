sapk-JOIN


## Lendo arquivo reclamações com schema definido,sem cabeçalho 

>>> `recschema = "idrec INT, datarec STRING, iddesp INT"`

>>> `reclamacoes = spark.read.csv("/home/sena/download/reclamacoes.csv", header=False, schema=recschema)`

## Salvando a tabela 

>>> `reclamacoes.write.saveAsTable("reclamacoes")`

# Inner join
spark.sql("select reclamacoes.*, despachantes.nome from 
despachantes inner join reclamacoes  on (despachantes.id = reclamacoes.iddesp)").show()

#righ join deve trazer o mesmo resultado, pois todas as reclamações tem um despachante

spark.sql("select reclamacoes.*, despachantes.nome from despachantes right join reclamacoes  
on (despachantes.id = reclamacoes.iddesp)").show()

#um left join traz mais colunas e campos nulos, pois alguns despachantes não tem reclações
spark.sql("select reclamacoes.*, despachantes.nome from despachantes left join reclamacoes  on (despachantes.id = reclamacoes.iddesp)").show()

#inner join
despachantes.join(reclamacoes,despachantes.id == reclamacoes.iddesp, "inner").select("idrec","datarec","iddesp","nome").show()

#right
despachantes.join(reclamacoes,despachantes.id == reclamacoes.iddesp, "right").select("idrec","datarec","iddesp","nome").show()

#left
despachantes.join(reclamacoes,despachantes.id == reclamacoes.iddesp, "right").select("idrec","datarec","iddesp","nome").show()

##spark-sql
show databases;
use desp;
show tables;

Select * from Despachantes;

Select nome,vendas from Despachantes;

#condição lógica
Select nome,vendas from Despachantes where vendas > 20;

#agrupar
Select cidade,sum(vendas) from Despachantes group by cidade order by 2 desc;

join
select reclamacoes.*, despachantes.nome from despachantes inner join reclamacoes  on (despachantes.id = reclamacoes.iddesp);





#teste de pesistência
from pyspark.sql import SparkSession


spark.sql("use desp").show()
#mostrar que a tabela ainda existe
spark.sql("select * from despachantes").show()
despachantes.show()

#o resultado de uma consulta sem um show gera um dataframe
despachantes = spark.sql("select * from despachantes")
despachantes.show()


