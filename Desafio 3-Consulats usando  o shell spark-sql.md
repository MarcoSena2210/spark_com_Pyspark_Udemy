# Desafio 3-Consultas usando  o shell spark-sql

## 1.Crie um banco de dados no DW do Spark chamado VendasVarejo , e persista todas as tabelas neste banco de dados.

###  Dica: Já tinha feito no exercício anterior

spark-sql> `ùse vendasvarejo;`

```
Output
Time taken: 0.219 seconds
```

spark-sql> `show tables;` 
```
Output 
vendasvarejo    clientes        false
vendasvarejo    itensvendas     false
vendasvarejo    produtos        false
vendasvarejo    vendas  false
vendasvarejo    vendedores      false
Time taken: 0.905 seconds, Fetched 5 row(s)
spark-sql>
```

### 2.Crie uma consulta que mostre de cada item vendido: Nome do Cliente, Data da Venda,Produto, Vendedor e Valor Total do item.

spark-sql> `select c.cliente, v.Data, p.Produto, vd.Vendedor, iv.ValorTotal
         > from itensvendas iv
         > inner join produtos p on (p.Produtoid = iv.Produtoid)
         > inner join vendas v on (v.VendasID = iv.VendasID)
         > inner join vendedores vd on (vd.vendedorID = v.vendedorID)
         > inner join clientes c on (c.clienteid = v.clienteid);`

```
Output
Humberto Almeida        28/12/2019      Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Iberê Lacerda   18402.0
Bárbara Magalhães       15/12/2020      Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Hélio Liberato  18402.0
Artur Macedo    22/12/2018      Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Hélio Liberato  13784.4
Dinarte Tabalipa        1/12/2020       Bicicleta Trinc Câmbios Shimano Aro 29 F                                                                                        reio A Disco 24v        Daniel Pirajá   13018.6
Humberto Lemes  12/12/2019      Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Simão Rivero    14077.54
Antão Corte-Real        16/11/2018      Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Iberê Lacerda   16561.8
Cândido Sousa do Prado  10/11/2018      Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Daniel Pirajá   16561.8
Brígida Gusmão  23/12/2019      Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Hélio Liberato  9201.0
Antão Corte-Real        16/11/2018      Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Iberê Lacerda   15933.6
Gertrudes Rabello       5/9/2019        Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Hélio Liberato  16561.8
Adélio Lisboa   23/11/2019      Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Hélio Liberato  11716.74
Francisca Ramallo       9/12/2020       Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Jéssica Castelão        8280.9
Adélio Lisboa   5/12/2019       Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Armando Lago    9201.0
Brígida Gusmão  23/12/2019      Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Hélio Liberato  7524.2
Antão Corte-Real        15/10/2020      Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Armando Lago    16561.8
Cândido Sousa do Prado  24/11/2018      Bicicleta Trinc Câmbios Shimano Aro 29 F                                                                                        reio A Disco 24v        Jéssica Castelão        13018.6
Adélio Lisboa   5/12/2019       Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Armando Lago    7658.0
Adolfo Patrício 7/11/2020       Bicicleta Gts Advanced 1.0 Aro 29 Freio Disco Câ                                                                                        mbio Traseiro Shimano 24 Marchas        Hélio Liberato  8510.0
Brígida Gusmão  25/12/2019      Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Godo Capiperibe 6771.78
Adélio Lisboa   29/9/2020       Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Jéssica Castelão        13543.56
Adelina Buenaventura    13/12/2019      Bicicleta Trinc Câmbios Shimano Aro 29 F                                                                                        reio A Disco 24v        Jéssica Castelão        6892.2
Celestino Pereira       11/12/2020      Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Jéssica Castelão        7524.2
Gonçalo Figueiró        10/12/2019      Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Simão Rivero    8852.0
Cidália Miera   19/12/2020      Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Tobias Furtado  8852.0
Guadalupe Rodrigues     27/10/2020      Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Capitolino Bahía        15641.7
Eusébio Mata    2/11/2019       Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Capitolino Bahía        8280.9
Celestino Pereira       11/12/2020      Bicicleta Gometws Endorphine 7.3 - Shima                                                                                        no Alumínio Aro 29 - 24 Marchas Jéssica Castelão        5932.0
Godofredo Quiroga       22/11/2018      Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Hélio Liberato  9201.0
Ibijara Botelho 25/11/2020      Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Iberê Lacerda   9201.0
Antão Corte-Real        17/11/2018      Bicicleta Gts Advanced 1.0 Aro 29 Freio                                                                                         Disco Câmbio Traseiro Shimano 24 Marchas        Hélio Liberato  8510.0
Guida Beiriz    12/12/2020      Bicicleta Gts Advanced 1.0 Aro 29 Freio Disco Câ                                                                                        mbio Traseiro Shimano 24 Marchas        Godo Capiperibe 8510.0
Brígida Gusmão  26/9/2018       Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Tobias Furtado  15048.4
António Lousado 20/12/2018      Bicicleta Gometws Endorphine 7.3 - Shimano Alumí                                                                                        nio Aro 29 - 24 Marchas Jéssica Castelão        5932.0
Flamínia Miera  21/12/2019      Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Hélio Liberato  6771.78
David Brás      23/12/2018      Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Hélio Liberato  7658.0
Bento Quintão   21/12/2019      Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Hélio Liberato  8280.9
Antão Corte-Real        4/9/2019        Bicicleta Trinc Câmbios Shimano Aro 29 F                                                                                        reio A Disco 24v        Hélio Liberato  11716.74
Flamínia Miera  21/12/2019      Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Hélio Liberato  6892.2
Brígida Gusmão  5/11/2019       Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Hélio Liberato  8852.0
Brígida Gusmão  4/12/2019       Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Hélio Liberato  6509.3
Cidália Miera   2/12/2019       Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Daniel Pirajá   7966.8
Antão Corte-Real        1/11/2019       Bicicleta Gts Advanced 1.0 Aro 29 Freio                                                                                         Disco Câmbio Traseiro Shimano 24 Marchas        Godo Capiperibe 7659.0
Cidália Miera   1/11/2020       Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Daniel Pirajá   7658.0
Ana Homem       2/11/2018       Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Armando Lago    7820.85
Adélio Lisboa   23/11/2019      Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Hélio Liberato  7820.85
Brígida Gusmão  25/12/2019      Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Godo Capiperibe 7820.85
Antão Corte-Real        13/10/2018      Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Daniel Pirajá   8852.0
Iracema Rodríguez       6/10/2019       Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Hélio Liberato  9201.0
Dinarte Tabares 22/11/2018      Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Hélio Liberato  7360.8
Cidália Miera   3/12/2020       Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Daniel Pirajá   7038.77
Artur Macedo    30/11/2018      Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Simão Rivero    7524.2
Faustino Maranhão       20/12/2018      Bicicleta Trinc Câmbios Shimano Aro 29 F                                                                                        reio A Disco 24v        Simão Rivero    6892.2
Guadalupe Rodrigues     27/10/2020      Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Capitolino Bahía        8852.0
Brígida Gusmão  4/6/2020        Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Hélio Liberato  18402.0
Cidália Miera   10/8/2019       Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Daniel Pirajá   15316.0
Antão Corte-Real        27/8/2020       Bicicleta Trinc Câmbios Shimano Aro 29 F                                                                                        reio A Disco 24v        Hélio Liberato  15316.0
Cid Pardo       12/11/2019      Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Capitolino Bahía        8280.9
Cosme Ipanema   30/7/2018       Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Iberê Lacerda   15048.4
Derli Lozada    2/10/2019       Bicicleta Gts Advanced 1.0 Aro 29 Freio Disco Câ                                                                                        mbio Traseiro Shimano 24 Marchas        Capitolino Bahía        7659.0
Antero Milheiro 9/11/2018       Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Iberê Lacerda   7524.2
Aníbal Bastos   16/10/2018      Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Hélio Liberato  7820.85
Ginéculo Luz    28/10/2018      Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Simão Rivero    7820.85
Cidália Miera   1/11/2020       Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Daniel Pirajá   7820.85
Brígida Gusmão  3/11/2018       Bicicleta Gts Advanced 1.0 Aro 29 Freio Disco Câ                                                                                        mbio Traseiro Shimano 24 Marchas        Jéssica Castelão        7233.5
Humberto Vergueiro      17/10/2019      Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Hélio Liberato  7524.2
Adélio Lisboa   23/11/2019      Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Hélio Liberato  7081.6
Garibaldo Oleiro        30/11/2019      Bicicleta Gts Advanced 1.0 Aro 29 Freio                                                                                         Disco Câmbio Traseiro Shimano 24 Marchas        Jéssica Castelão        7659.0
Aníbal Bastos   17/9/2018       Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Jéssica Castelão        8852.0
Fabrício Varella        28/9/2018       Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Armando Lago    8852.0
Irene Villanueva        12/11/2019      Bicicleta Trinc Câmbios Shimano Aro 29 F                                                                                        reio A Disco 24v        Iberê Lacerda   6892.2
Cidália Miera   25/7/2018       Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Simão Rivero    13018.6
Cândido Sousa do Prado  24/11/2018      Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Jéssica Castelão        7524.2
Florinda Assunção       26/6/2018       Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Hélio Liberato  14721.6
Deise Farias    7/9/2020        Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Hélio Liberato  9201.0
Faustino Maranhão       2/11/2019       Bicicleta Trinc Câmbios Shimano Aro 29 F                                                                                        reio A Disco 24v        Armando Lago    7658.0
Dinarte Marino  17/6/2018       Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Simão Rivero    14077.54
Cláudio Lopes   18/10/2018      Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Hélio Liberato  7820.85
Brígida Gusmão  25/9/2018       Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Hélio Liberato  6892.2
Antão Corte-Real        19/9/2019       Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Simão Rivero    8852.0
Brígida Gusmão  24/12/2019      Bicicleta Gometws Endorphine 6.1 Shimano Alumíni                                                                                        o- Aro 26 - 21 Marchas  Armando Lago    5023.5
Jacinto Dorneles        4/12/2020       Bicicleta Gts Advanced 1.0 Aro 29 Freio                                                                                         Disco Câmbio Traseiro Shimano 24 Marchas        Jéssica Castelão        3255.08
Brígida Gusmão  2/6/2018        Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Hélio Liberato  13018.6
Antão Corte-Real        15/6/2020       Bicicleta Trinc Câmbios Shimano Aro 29 F                                                                                        reio A Disco 24v        Simão Rivero    13018.6
Gertrudes Hidalgo       5/8/2018        Bicicleta Trinc Câmbios Shimano Aro 29 F                                                                                        reio A Disco 24v        Napoleão Méndez 13018.6
Brígida Gusmão  18/8/2018       Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Tobias Furtado  13018.6
Eloi Pereira    9/9/2019        Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Napoleão Méndez 6509.3
Custódio Rolim  15/11/2019      Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Simão Rivero    6509.3
Arcidres Murici 25/9/2020       Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Tobias Furtado  7524.2
Eloi Vasques    26/10/2020      Bicicleta Gometws Endorphine 6.1 Shimano Alumíni                                                                                        o- Aro 26 - 21 Marchas  Jéssica Castelão        5023.5
Godinho ou Godim Fogaça 1/10/2020       Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Simão Rivero    7820.85
Cândida Silvestre       2/8/2020        Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Tobias Furtado  8852.0
Cidália Miera   1/11/2020       Bicicleta Gts Advanced 1.0 Aro 29 Freio Disco Câ                                                                                        mbio Traseiro Shimano 24 Marchas        Daniel Pirajá   4255.0
Antão Corte-Real        16/12/2019      Bicicleta Gts Advanced 1.0 Aro 29 Freio                                                                                         Disco Câmbio Traseiro Shimano 24 Marchas        Napoleão Méndez 4255.0
Irani Jaguariúna        6/9/2019        Bicicleta Gometws Endorphine 7.3 - Shima                                                                                        no Alumínio Aro 29 - 24 Marchas Hélio Liberato  5932.0
Flamínia Miera  21/12/2019      Bicicleta Gometws Endorphine 6.1 Shimano Alumíni                                                                                        o- Aro 26 - 21 Marchas  Hélio Liberato  2955.0
Doroteia Quintanilla    23/11/2018      Bicicleta Trinc Câmbios Shimano Aro 29 F                                                                                        reio A Disco 24v        Jéssica Castelão        5858.37
Greice Lameirinhas      27/6/2018       Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Tobias Furtado  16561.8
Antónia Canhão  9/8/2020        Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Capitolino Bahía        8280.9
Antônio Sobral  6/12/2019       Bicicleta Gometws Endorphine 6.1 Shimano Alumíni                                                                                        o- Aro 26 - 21 Marchas  Iberê Lacerda   2955.0
Gertrudes Hidalgo       5/8/2018        Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Napoleão Méndez 8852.0
Antão Corte-Real        11/8/2018       Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Hélio Liberato  8852.0
Cláudio Lopes   21/8/2019       Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Daniel Pirajá   8852.0
Cosme Zambujal  17/5/2019       Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Jéssica Castelão        18402.0
Brígida Gusmão  2/6/2018        Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Hélio Liberato  15641.7
Filipa Mattozo  20/8/2019       Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Iberê Lacerda   7820.85
Francisca Ramallo       9/12/2020       Bicicleta Gts Advanced 1.0 Aro 29 Freio                                                                                         Disco Câmbio Traseiro Shimano 24 Marchas        Jéssica Castelão        3616.75
Epaminondas Sousa de Arronches  9/10/2019       Bicicleta Aro 29 Mountain Bike E                                                                                        ndorphine 6.3 - 24 Marchas - Shimano - Alumínio Hélio Liberato  6771.78
Gisela Candeias 15/8/2019       Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Jéssica Castelão        8280.9
Celestino Pereira       27/9/2020       Bicicleta Trinc Câmbios Shimano Aro 29 F                                                                                        reio A Disco 24v        Jéssica Castelão        7658.0
Ana Homem       2/11/2018       Bicicleta Gts Advanced 1.0 Aro 29 Freio Disco Câ                                                                                        mbio Traseiro Shimano 24 Marchas        Armando Lago    4255.0
Cidália Miera   19/12/2020      Bicicleta Gts Advanced 1.0 Aro 29 Freio Disco Câ                                                                                        mbio Traseiro Shimano 24 Marchas        Tobias Furtado  4255.0
Antão Corte-Real        23/3/2020       Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Tobias Furtado  17704.0
Ilduara Chávez  28/7/2018       Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Simão Rivero    8852.0
Antão Corte-Real        31/8/2020       Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Simão Rivero    7966.8
Cláudio Lopes   18/10/2018      Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Hélio Liberato  5513.76
Antão Corte-Real        31/8/2020       Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas        Simão Rivero    7038.77
Godofredo Quiroga       22/11/2018      Bicicleta Aro 29 Mountain Bike Endorphin                                                                                        e 6.3 - 24 Marchas - Shimano - Alumínio Hélio Liberato  5417.42
Elsa Barreto    30/6/2018       Bicicleta Trinc Câmbios Shimano Aro 29 Freio A D                                                                                        isco 24v        Capitolino Bahía        13784.4
Antão Corte-Real        17/11/2020      Bicicleta Gts Advanced 1.0 Aro 29 Freio                                                                                         Disco Câmbio Traseiro Shimano 24 Marchas        Tobias Furtado  3829.5
Flávia Camacho  26/10/2020      Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Jéssica Castelão        5631.01
Brígida Gusmão  18/4/2019       Bicicleta Aro 29 Mountain Bike Endorphine 6.3 -                                                                                         24 Marchas - Shimano - Alumínio Simão Rivero    15933.6
Guadalupe Rodrigues     27/10/2020      Bicicleta Trinc Câmbios Shimano Aro 29 F                                                                                        reio A Disco 24v        Capitolino Bahía        4686.7
Cid Pardo       12/11/2019      Bicicleta Gts Advanced 1.0 Aro 29 Freio Disco Câ                                                                                        mbio Traseiro Shimano 24 Marchas        Capitolino Bahía        4255.0
Antónia Canhão  12/7/2018       Bicicleta Altools Stroll Aro 26 Freio À Disco 21                                                                                         Marchas        Tobias Furtado  9201.0
Antão Corte-Real        10/4/2019       Bicicleta Altools Stroll Aro 26 Freio À                                                                                         Disco 21 Marchas       

```
