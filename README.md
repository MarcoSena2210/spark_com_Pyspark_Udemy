# spark_com_Pyspark_Udemy
Curso Formação Spark com Pyspark 

- Nome: Marco Antonio de Sena Campos
- Instituição: Udemy
- Curso: Formação Spark com Pyspark(2022)
- Professor: Fernando Amaral

Linkedin: 
Git     :


#1 Crie uma consulta que mostre, nesta ordem, Cliente, Estados e Status
clientes.select("Cliente","Estado","Status").show()

#2 Crie uma consulta que mostre apenas os clientes do Status “platinum” e “gold”
clientes.select("*").where( (Func.col("Status") == "Gold") | (Func.col("Status") == "Platinum") ).show()

#3 Demostre quanto cada Status de Clientes representa em vendas?
vendas.join(clientes,vendas.ClienteID ==clientes.ClienteID ).groupBy(clientes.Status).agg(sum("Total")).orderBy(Func.col("sum(Total)").desc())