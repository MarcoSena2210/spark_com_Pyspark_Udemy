
# Instalação do spark em maquina Virtual

## 1.Download e Instalação Oracle Virtual Box

## 2.Download de ISO do Ubuntu
## 3.Instalação do Ubuntu na VM

## 3.1.Preparação do linux -Atualizações

<code> sudo apt update </code>

<code> sudo apt upgrade </code>



## 4.Instalação do Spark (pré requisito: Java)

`sudo apt install curl mlocate default-jdk -y`

## 5 Baixando o spark-hadoop
<code>
wget https://dlcdn.apache.org/spark/spark-3.2.1/spark/spark-3.2.1-bin-hadoop3.2.tgz
</code>
## 6.Descompactando o spark compactado.

<code>
tar xvf spark-3.2.1-bin-hadoop3.2.tgz
</code>

## Boas Práticas. movendo o spak para pasta de app
sudo mv spark-3.2.1-bin-hadoop3.2/ /opt/spark

## Configurando as variaveis de momemoria.Para isso vamos editar o arquivo .bashrc e colocar os parametros:

Para edita:
sudo gedit ~/.bashrc

Depois incluir no final do arquivo as seguintes linhas:
<code>
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
</code>

## 7.Agora vamos executar um comando para atualizar as variaveis de ambiente sem precisar reiniciar o linux.

<code>source -/.bashrc
</code>

## Startando o spack usando a liguagem scala
<code>start-master.sh </code>

CTRL + C ou D  para sair do shell

## Startando o spack usando a liguagem scala
<code>start-shell </code>

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