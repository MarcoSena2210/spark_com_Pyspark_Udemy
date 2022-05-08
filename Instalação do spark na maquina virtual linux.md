
# Instalação do spark em máquina Virtual Linux

## 1.Download e Instalação Oracle Virtual Box


https://www.virtualbox.org/wiki/Downloads

## 2.Download de ISO do Ubuntu
Precisa ser o packege ISO.
https://ubuntu.com/download/desktop


## 3.Instalação do Ubuntu na VM

## 3.1.Preparação do linux -Atualizações

<code> sudo apt update </code>

<code> sudo apt upgrade </code>



## 4.Instalação do Spark (pré requisito: Java)

`sudo apt install curl mlocate default-jdk -y`

## 5 Baixando o spark-hadoop

`wget https://dlcdn.apache.org/spark/spark-3.2.1/spark/spark-3.2.1-bin-hadoop3.2.tgz`

## 6.Descompactando o spark compactado.

`tar xvf spark-3.2.1-bin-hadoop3.2.tgz`

## Boas Práticas. movendo o spak para pasta de app
`sudo mv spark-3.2.1-bin-hadoop3.2/ /opt/spark`

## Configurando as variáveis de memoria.Para isso vamos editar o arquivo .bashrc e colocar os parametros:

Para edita:
`sudo gedit ~/.bashrc`

Depois, incluir no final do arquivo as seguintes linhas:

`export SPARK_HOME=/opt/spark`
`export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin`

## 7.Agora vamos executar um comando para atualizar as variaveis de ambiente sem precisar reiniciar o linux.

`source -/.bashrc`

## Startando o spack usando a liguagem scala

`start-master.sh`

CTRL + C ou D  para sair do shell

## Startando o spack usando a liguagem scala
`start-shell`

CTRL + C ou D  para sair do shell


## Startando o spack usando a liguagem python
<code>pyspark </code>

CTRL + C ou D  para sair do shell

## 7.Instalando as bibliotecas que serão usadas no lab
<code>
sudo apt install python3-pip </code>

<code>
pip install numpy </code>
<code>
pip install pandas </code>