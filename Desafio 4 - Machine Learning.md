Desafio 4 - Machine Learning

1.Classe é a coluna class,portando Multiclass
2.Utilize MulticlassClassificationEvaluator
3.Use accuracy como métrica
4.Use um classificador diferente,por exemplo NaiveBayes

Meu objetivo vai ser criar um modelo para o famoso e conhecido conjunto de dados de Iris o Iris.
Esse conjunto de dados então ele são.Ele tem 150 estâncias 150 linhas.
São cinco atributos sendo os quatro primeiros são atributos numéricos e o quinto atributo é um atributo categórico.
Então o objetivo é prever a classe que é a espécie da planta então existem três diferentes espécies. Então a partir do comprimento da célula e da pétala da pétala e do comprimento da largura você deve
prever qual é a espécie.
Então vejam que este é um problema múltipla.
Até agora a gente viu regressão e a gente viu um problema de classificação mostra atual era binária.
Se o cliente ia sair ou não seria abandonar a empresa não.Agora tem um problema um multiplex que é a coluna Clécio então o que vai mudar você vai utilizar para
avaliar a performance desse modelo. O multiplex Classification Evaluation ok.
Então essa é a primeira mudança você precisa usar um avaliador para problemas de múltipla múltipla trouxe.
E aqui como requisito da atividade você deve usar a cura como métrica de acurácia.Por que você não exatamente porque essa é uma métrica bastante simples e ela simplesmente ela vai lhe
mostrar o percentual de acertos ok.Existem várias outras métricas que você pode se poderia utilizar para um programa de múltiplas classes.
O que mais a gente vai utilizar aqui é mais simples que é acurácia.
Então se por exemplo você tiver o resultado lá como 90 por cento quer dizer que o modelo acertou 90
por cento das previsões e o outro requisito da atividade é você utilizar um classificador diferente.
Um dos que a gente não utilizou até agora é sugestão é que utilizado na IBM certo.
Então vejam que no mais no restante com relação ao tratamento dos dados você pode utilizar a fórmula
você pode adicionar os dados.
Você usou o método Fit você usa o preditivo para através do Evaluation então mas isso não muda nada
certo é só alguns detalhes em função da natureza do tipo de dado que a gente está porque a gente está
avaliando.
Lembrando que este arquivo Wines pontos CSV está junto com os arquivos de download.
Ele é um arquivo separado por vírgula e não por ponto e vírgula.
Isso você tem que considerar no momento da importação.
E lembrando também que você deve tentar fazer e tentar resolver sozinho.
E no próximo tutorial eu apresento uma solução.
Caso você queira comparar com a sua teria tido alguma alguma dificuldade.

#Prof. Fernando Amaral
from pyspark.ml.feature import RFormula
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

iris = spark.read.csv("/home/sena/download/iris.csv",inferSchema=True, header=True)
iris.show(3)


## RFormula faz vários tipos de transformações
formula = RFormula(formula="class ~ . ",featuresCol="features", labelCol="label", handleInvalid="skip")

## aplicamos a formula e selecionamos apenas as colunas que interessam
iris_trans = formula.fit(iris).transform(iris).select("features","label")

## resultado
iris_trans.show(truncate=False)

## Gerando os dados de treino e teste
irisTreino, irisTeste = iris_trans.randomSplit([0.7,0.3])



nb = NaiveBayes(smoothing=2,   labelCol="label", featuresCol="features")


## Criando o modelo
modelo = nb.fit(irisTreino)


## Previsão
previsao = modelo.transform(irisTeste)
previsao.show()




## Avaliar a accuracy
avaliar = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="label",metricName="accuracy")
avaliar = avaliar.evaluate(previsao)
print(avaliar)