# App_monitora_pasta 
app_monitora_arq_straming

Exemplo de aplicação simples para monitorar stream de arquivo na pasta temporária e 
exibir a leitura dos dados na console, sempre que for inserido algum arquivo .json 
nesta pasta.




## Crie uma pasta o nomw "testestreaming" abaixo de download/streaming/:
sena@sena-VirtualBox:~/download/streaming$ `mkdir testestreaming`

## O pyspark irá criar outra pasta com o nome "temp"  abaixo de /home/<seu user>/ automaticamente.

## Existem a pasta /home/sena/download/streaming os estão armazenados os arquivos *.json  que iremos copiar para a pasta "testestreaming" que estamos monitorando.
## Dessa forma podermos comprovar o monitoramento.
```
Arquivos : 
-rw-rw-r-- 1 sena sena 1017 ago 20  2021 1.json14.json
-rw-rw-r-- 1 sena sena 3220 ago 20  2021 2.json44.json
-rw-rw-r-- 1 sena sena 1039 ago 20  2021 3.json14.json
-rw-rw-r-- 1 sena sena 1933 ago 20  2021 4.json26.json
-rw-rw-r-- 1 sena sena 2141 ago 20  2021 5.json29.json
-rw-rw-r-- 1 sena sena 1058 ago 20  2021 6.json14.json
-rw-rw-r-- 1 sena sena 4325 ago 20  2021 7.json59.json
```

  Conteúdo do app
``` 
 from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("appMonitoraStreaming").getOrCreate()
    
    # Definindo o shema
    jsonschema = "nome STRING, postagem STRING, data INT"
    
    # Objeto dataframe para armazenar os arquivos que serão movidos para 
    # a pasta /home/sena/testestream/ que será monitorada.  
    df = spark.readStream.json("/home/sena/testestream/", schema=jsonschema)

    # Guarda o estado da sesssão 
    diretorio = "/home/sena/temp"

    stcal = df.writeStream.format("console").outputMode("append").trigger(processingTime="5 second").option("checkpointlocation", diretorio).start()
    
    # Espera o termino do processo.
    stcal.awaitTermination()

    # Para executar o app, no prompt de comando digitar spark-submit <nome_da_aplicação.py>   
  ```
