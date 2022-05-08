
# Ações e Transformações usando RDD usando pyspark 

## *No prompt de comando chamar o pyspark 

sena@sena-VirtualBox:~/ms-pyspark$ `pyspark`

![tela inicial do spark](image/tela-Inicial_spark.png)

# AÇÕES:
## Criar o RDD   
>` numeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10]) `

1. take() -> Exibe elementos do RDD
> `numeros.take(5)`

<prev>
[1, 2, 3, 4, 5]
</prev>

2. top(5) -> Exibe os 5 maiores
> `numeros.top(5)`

<prev>
[10, 9, 8, 7, 6]
<prev>

3. collect() -> Exibe os elementos
⚠ Dica: Não deve ser usado em anbiente de produção 
> `numeros.collect()`

<prev>
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
</prev>

## Operações aritiméticas
4. count() -> conta
> `numeros.count()`

<prev>
10
</prev>

5. mean() -> Média
> `numeros.mean()`

<prev>
5.5
</prev>

6. sum() -> Soma
> `numeros.sum()`

<prev>
55
</prev>

7. max() -> Maior
> `numeros.max()`
<prev>
10
</prev>

8. min() -> Menor
> `numeros.min()` 
<prev>
1
</prev>

9. stdev() -> Desvio padrão
> `numeros.stdev()`
<prev>
2.8722813232690143
</prev>

# TRANSFORMAÇÕES:
##  Filtro -> filter()
> `filtro = numeros.filter(lambda filtro: filtro > 2)`

>`filtro.collect()`

<prev>
[3, 4, 5, 6, 7, 8, 9, 10]
<prev>

## Amostra com reposição -> sample()
> `amostra = numeros.sample(True,0.5,1)`

> `amostra.collect()`

[2, 3, 4, 5, 9, 10]

## Aplica uma função --> map()
> `mapa = numeros.map(lambda mapa: mapa * 2)`

> `mapa.collect()`

[2, 4, 6, 8, 10, 12, 14, 16, 18, 20]

## Criaremos outro RDD (numero2 ) para fazermos algumas operações entre eles

> `numeros2 = sc.parallelize([6,7,8,9,10]) `

## Operador Union - gera rdd com os 2 conjuntos
> `uniao = numeros.union(numeros2) `

> `uniao.collect()`
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 6, 7, 8, 9, 10]

## Intersecção
> `interseccao = numeros.intersection(numeros2) `
> `interseccao.collect()`

[6, 8, 10, 7, 9]

## Subtração
> `subtrai =numeros.subtract(numeros2)`
> `subtrai.collect()`

[2, 4, 1, 3, 5]

## Produto cartesiano
> `cartesiano = numeros.cartesian(numeros2)`
> `cartesiano.collect()`

[(1, 6), (1, 7), (1, 8), (1, 9), (1, 10), (2, 6), (2, 7), (2, 8), (2, 9), (2, 10), (3, 6), (3, 7), (3, 8), (3, 9), (3, 10), (4, 6), (4, 7), (4, 8), (4, 9), (4, 10), (5, 6), (5, 7), (5, 8), (5, 9), (5, 10), (6, 6), (6, 7), (6, 8), (6, 9), (6, 10), (7, 6), (7, 7), (7, 8), (7, 9), (7, 10), (8, 6), (8, 7), (8, 8), (8, 9), (8, 10), (9, 6), (9, 7), (9, 8), (9, 9), (9, 10), (10, 6), (10, 7), (10, 8), (10, 9), (10, 10)]

> ## Ação, contar por valor numero de vezes que cada valor aparece
> `cartesiano.countByValue()`

defaultdict(<class 'int'>, {(1, 6): 1, (1, 7): 1, (1, 8): 1, (1, 9): 1, (1, 10): 1, (2, 6): 1, (2, 7): 1, (2, 8): 1, (2, 9): 1, (2, 10): 1, (3, 6): 1, (3, 7): 1, (3, 8): 1, (3, 9): 1, (3, 10): 1, (4, 6): 1, (4, 7): 1, (4, 8): 1, (4, 9): 1, (4, 10): 1, (5, 6): 1, (5, 7): 1, (5, 8): 1, (5, 9): 1, (5, 10): 1, (6, 6): 1, (6, 7): 1, (6, 8): 1, (6, 9): 1, (6, 10): 1, (7, 6): 1, (7, 7): 1, (7, 8): 1, (7, 9): 1, (7, 10): 1, (8, 6): 1, (8, 7): 1, (8, 8): 1, (8, 9): 1, (8, 10): 1, (9, 6): 1, (9, 7): 1, (9, 8): 1, (9, 9): 1, (9, 10): 1, (10, 6): 1, (10, 7): 1, (10, 8): 1, (10, 9): 1, (10, 10): 1})

## Criando o RDD compras,contendo código do cliente e valor
> `compras = sc.parallelize([(1,200),(2,300),(3,120),(4,250),(5,78)])`

## Separar chaves
> `chaves = compras.keys()`

> `chaves.collect()`
[1, 2, 3, 4, 5]

## Separar por valores
> `valores = compras.values()`

> `valores.collect()`
[200, 300, 120, 250, 78]

## Conta elementos por chave
> `compras.countByKey()`

defaultdict(<class 'int'>, {1: 1, 2: 1, 3: 1, 4: 1, 5: 1})

# Aplicar função no valor, sem aterar chave
> `soma = compras.mapValues(lambda soma: soma + 1)`
> `soma.collect()`

[(1, 201), (2, 301), (3, 121), (4, 251), (5, 79)]

#Agrupar por chave
> `agrupa = compras.groupByKey().mapValues(list)`

> `agrupa.collect()`

[(1, [200]), (2, [300]), (3, [120]), (4, [250]), (5, [78])]

## Criando debitos,com codigo cliente e valor
> `debitos = sc.parallelize([(1,20),(2,300)])`

## Inner join entre compras e debitos
> `resultado = compras.join(debitos)` 
> `resultado.collect()`

[(2, (300, 300)), (1, (200, 20))]

## Remove e mostra apenas quem tem débito
> `semdebito = compras.subtractByKey(debitos)`
> `semdebito.collect()`

[(4, 250), (3, 120), (5, 78)]
