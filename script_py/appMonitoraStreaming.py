# app tem como objetivo monitorar uma pasta que irá ficar recebendo arq.
# json.
# Simulando uma operação de stream.Ficará em wait (aguardando) até que seja gerada 
# uma interrupção pelo usuário ou outro evento que a interrompa..
  
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.appName("appMonitoraStreaming").getOrCreate()
    
    # Definindo o shema
    jsonschema = "nome STRING, postagem STRING, data INT"
    
    # Objeto dataframe para armazenar os arquivos que serão movidos para 
    # a pasta /home/sena/testestream/ que serámonitorada.  
    df = spark.readStream.json("/home/sena/testestream/", schema=jsonschema)

    # Guarda o estado da sesssão 
    diretorio = "/home/sena/temp"

    stcal = df.writeStream.format("console").outputMode("append").trigger(processingTime="5 second").option("checkpointlocation", diretorio).start()
    
    # Esperaotermino do processo.
    stcal.awaitTermination()

    #Para executar o app, no prompt de comando digitar spark-submit <nome_da_aplicação.py>   
    
