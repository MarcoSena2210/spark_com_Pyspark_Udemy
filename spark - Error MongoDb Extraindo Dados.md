# Extraindo Dados de MongoDb
É um banco de dados orientado a documentos, noSQL e pode facilmente interagir com o payspar.
Tem o conceito de coleção e documentos.
Vamos usar umpackage para conectar no mongoDB, uma especie de api de conexão.Vamos importar dados.   

## Etapas
### • 1) Instalar o mongoDb via terminal 

#### • 1.1) sena@sena-VirtualBox:~$ `wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | sudo apt-key add -`

```
Output 
[sudo] senha para sena:
Warning: apt-key is deprecated. Manage keyring files in trusted.gpg.d instead (see apt-key(8)).
OK
```

#### • 1.2)sena@sena-VirtualBox:~$ `echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-5.0.list `

```
Output
deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse
```

#### • 1.3) Atualizar o linux

sena@sena-VirtualBox:~$ `sudo apt-get update`
``` 
Output
Atingido:1 http://br.archive.ubuntu.com/ubuntu jammy InRelease
Obter:2 http://br.archive.ubuntu.com/ubuntu jammy-updates InRelease [109 kB]
Obter:3 http://security.ubuntu.com/ubuntu jammy-security InRelease [110 kB]
Ign:4 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 InRelease
Obter:5 http://br.archive.ubuntu.com/ubuntu jammy-backports InRelease [99,8 kB]
Obter:6 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 Release [4.417                                                                                         B]
Obter:7 http://br.archive.ubuntu.com/ubuntu jammy-updates/main amd64 Packages [2                                                                                        12 kB]
Obter:8 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 Release.gpg [8                                                                                        01 B]
Obter:9 http://br.archive.ubuntu.com/ubuntu jammy-updates/main i386 Packages [99                                                                                        ,6 kB]
Obter:10 http://br.archive.ubuntu.com/ubuntu jammy-updates/main amd64 DEP-11 Met                                                                                        adata [57,5 kB]
Obter:11 http://br.archive.ubuntu.com/ubuntu jammy-updates/universe i386 Package                                                                                        s [37,2 kB]
Obter:12 http://br.archive.ubuntu.com/ubuntu jammy-updates/universe amd64 Packag                                                                                        es [98,8 kB]
Obter:13 http://br.archive.ubuntu.com/ubuntu jammy-updates/universe amd64 DEP-11                                                                                         Metadata [87,9 kB]
Obter:14 http://br.archive.ubuntu.com/ubuntu jammy-backports/universe amd64 DEP-                                                                                        11 Metadata [1.196 B]
Obter:15 http://security.ubuntu.com/ubuntu jammy-security/main amd64 DEP-11 Meta                                                                                        data [11,4 kB]
Obter:16 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0/multiverse ar                                                                                        m64 Packages [13,5 kB]
Obter:17 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0/multiverse am                                                                                        d64 Packages [15,7 kB]
Atingido:18 http://apt.postgresql.org/pub/repos/apt jammy-pgdg InRelease
Baixados 959 kB em 4s (270 kB/s)
Lendo listas de pacotes... Pronto
W: https://repo.mongodb.org/apt/ubuntu/dists/focal/mongodb-org/5.0/Release.gpg:                                                                                         Key is stored in legacy trusted.gpg keyring (/etc/apt/trusted.gpg), see the DEPR                                                                                        ECATION section in apt-key(8) for details.
W: http://apt.postgresql.org/pub/repos/apt/dists/jammy-pgdg/InRelease: Key is st                                                                                        ored in legacy trusted.gpg keyring (/etc/apt/trusted.gpg), see the DEPRECATION s                                                                                        ection in apt-key(8) for details.
N: Skipping acquire of configured file 'main/binary-i386/Packages' as repository                                                                                         'http://apt.postgresql.org/pub/repos/apt jammy-pgdg InRelease' doesn't support                                                                                         architecture 'i386'
sena@sena-VirtualBox:~$
``` 

#### • 1.4) Instalar o mongoDB
sena@sena-VirtualBox:~$ `sudo apt-get install -y mongodb-org`

```
Output
Lendo listas de pacotes... Pronto
Construindo árvore de dependências... Pronto
Lendo informação de estado... Pronto
Alguns pacotes não puderam ser instalados. Isto pode significar que
você solicitou uma situação impossível ou, se você está usando a
distribuição instável, que alguns pacotes requeridos não foram
criados ainda ou foram retirados da "Incoming".
A informação a seguir pode ajudar a resolver a situação:

Os pacotes a seguir têm dependências desencontradas:
 mongodb-org-mongos : Depende: libssl1.1 (>= 1.1.1) mas não é instalável
 mongodb-org-server : Depende: libssl1.1 (>= 1.1.1) mas não é instalável
 mongodb-org-shell : Depende: libssl1.1 (>= 1.1.1) mas não é instalável
N: A ignorar o ficheiro 'mongodb-org-' no directório '/etc/apt/sources.list.d/' porque não tem extensão no nome do ficheiro
E: Impossível corrigir problemas, você manteve (hold) pacotes quebrados.
sena@sena-VirtualBox:~$
```


### •Importar Json exemplo,banco de dados Posts,coleção post
### •Instalar Package para Spark
### •Usar o Pyspark para ler, e gravar dados no Mongodb

sena@sena-VirtualBox:~$ `sudo systemctl start mongod`
```
Failed to start mongod.service: Unit mongod.service not found.
```
Pendente instalação:

1) Passo 1

`curl -fsSL https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add - `

2) Passo 2
`apt-key list`

3) Passo 3 
`echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list `

4) Passo 4
`sudo apt update`

5) Passo 5

# até aqui continuou sem funcionar.


You can force the installation of libssl1.1 by adding the ubuntu 21.10 source :

echo "deb http://security.ubuntu.com/ubuntu impish-security main" | sudo tee /etc/apt/sources.list.d/impish-security.list

sudo apt-get update
sudo apt-get install libssl1.1


sena@sena-VirtualBox:~/Downloads$ ` sudo apt-get install -y mongodb-org`

```
Output
Lendo listas de pacotes... Pronto
Construindo árvore de dependências... Pronto
Lendo informação de estado... Pronto
Os pacotes adicionais seguintes serão instalados:
  mongodb-database-tools mongodb-mongosh mongodb-org-database mongodb-org-database-tools-extra mongodb-org-mongos mongodb-org-server mongodb-org-shell
  mongodb-org-tools
Os NOVOS pacotes a seguir serão instalados:
  mongodb-database-tools mongodb-mongosh mongodb-org mongodb-org-database mongodb-org-database-tools-extra mongodb-org-mongos mongodb-org-server mongodb-org-shell
  mongodb-org-tools
0 pacotes atualizados, 9 pacotes novos instalados, 0 a serem removidos e 14 não atualizados.
É preciso baixar 142 MB de arquivos.
Depois desta operação, 455 MB adicionais de espaço em disco serão usados.
Obter:1 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4/multiverse amd64 mongodb-database-tools amd64 100.5.2 [46,5 MB]
Obter:2 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4/multiverse amd64 mongodb-mongosh amd64 1.4.2 [36,0 MB]
Obter:3 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0/multiverse amd64 mongodb-org-shell amd64 5.0.9 [14,4 MB]
Obter:4 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0/multiverse amd64 mongodb-org-server amd64 5.0.9 [26,4 MB]
Obter:5 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0/multiverse amd64 mongodb-org-mongos amd64 5.0.9 [18,5 MB]
Obter:6 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0/multiverse amd64 mongodb-org-database-tools-extra amd64 5.0.9 [7.756 B]
Obter:7 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0/multiverse amd64 mongodb-org-database amd64 5.0.9 [3.540 B]
Obter:8 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0/multiverse amd64 mongodb-org-tools amd64 5.0.9 [2.892 B]
Obter:9 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0/multiverse amd64 mongodb-org amd64 5.0.9 [2.932 B]
Baixados 142 MB em 1min 12s (1.969 kB/s)
N: A ignorar o ficheiro 'mongodb-org-' no directório '/etc/apt/sources.list.d/' porque não tem extensão no nome do ficheiro
A seleccionar pacote anteriormente não seleccionado mongodb-database-tools.
(Lendo banco de dados ... 195349 ficheiros e directórios actualmente instalados.)
A preparar para desempacotar .../0-mongodb-database-tools_100.5.2_amd64.deb ...
A descompactar mongodb-database-tools (100.5.2) ...
A seleccionar pacote anteriormente não seleccionado mongodb-mongosh.
A preparar para desempacotar .../1-mongodb-mongosh_1.4.2_amd64.deb ...
A descompactar mongodb-mongosh (1.4.2) ...
A seleccionar pacote anteriormente não seleccionado mongodb-org-shell.
A preparar para desempacotar .../2-mongodb-org-shell_5.0.9_amd64.deb ...
A descompactar mongodb-org-shell (5.0.9) ...
A seleccionar pacote anteriormente não seleccionado mongodb-org-server.
A preparar para desempacotar .../3-mongodb-org-server_5.0.9_amd64.deb ...
A descompactar mongodb-org-server (5.0.9) ...
A seleccionar pacote anteriormente não seleccionado mongodb-org-mongos.
A preparar para desempacotar .../4-mongodb-org-mongos_5.0.9_amd64.deb ...
A descompactar mongodb-org-mongos (5.0.9) ...
A seleccionar pacote anteriormente não seleccionado mongodb-org-database-tools-extra.
A preparar para desempacotar .../5-mongodb-org-database-tools-extra_5.0.9_amd64.deb ...
A descompactar mongodb-org-database-tools-extra (5.0.9) ...
A seleccionar pacote anteriormente não seleccionado mongodb-org-database.
A preparar para desempacotar .../6-mongodb-org-database_5.0.9_amd64.deb ...
A descompactar mongodb-org-database (5.0.9) ...
A seleccionar pacote anteriormente não seleccionado mongodb-org-tools.
A preparar para desempacotar .../7-mongodb-org-tools_5.0.9_amd64.deb ...
A descompactar mongodb-org-tools (5.0.9) ...
A seleccionar pacote anteriormente não seleccionado mongodb-org.
A preparar para desempacotar .../8-mongodb-org_5.0.9_amd64.deb ...
A descompactar mongodb-org (5.0.9) ...
Configurando mongodb-mongosh (1.4.2) ...
Configurando mongodb-org-server (5.0.9) ...
Adicionando usuário de sistema 'mongodb' (UID 130) ...
Adicionando novo usuário `mongodb' (UID 130) ao grupo `nogroup' ...
Sem criar diretório pessoal `/home/mongodb'.
Adicionando grupo `mongodb' (GID 138) ...
Concluído.
Adicionando o usuário `mongodb' ao grupo `mongodb' ...
Adicionando usuário mongodb ao grupo mongodb
Concluído.
Configurando mongodb-org-shell (5.0.9) ...
Configurando mongodb-database-tools (100.5.2) ...
Configurando mongodb-org-mongos (5.0.9) ...
Configurando mongodb-org-database-tools-extra (5.0.9) ...
Configurando mongodb-org-database (5.0.9) ...
Configurando mongodb-org-tools (5.0.9) ...
Configurando mongodb-org (5.0.9) ...
A processar 'triggers' para man-db (2.10.2-1) ...
N: A ignorar o ficheiro 'mongodb-org-' no directório '/etc/apt/sources.list.d/' porque não tem extensão no nome do ficheiro
sena@sena-VirtualBox:~/Downloads$
sena@sena-VirtualBox:~/Downloads$
sena@sena-VirtualBox:~/Downloads$ cd ..
sena@sena-VirtualBox:~$
```

sena@sena-VirtualBox:~/Downloads$ `cd ..`
```
Output
sena@sena-VirtualBox:~$
```

### Vamos startar o mongoDb
sena@sena-VirtualBox:~$ `sudo systemctl start mongod `
```
Output 
[sudo] senha para sena:
```

### habilitando para inicalizar automaticamente 
sena@sena-VirtualBox:~$ `sudo systemctl enable mongod`
```
Output 
Created symlink /etc/systemd/system/multi-user.target.wants/mongod.service → /lib/systemd/system/mongod.service.
sena@sena-VirtualBox:~$
```
### Vamos agora acessara console mongo
mongosh

sena@sena-VirtualBox:~$ `mongosh`

```
Output
Current Mongosh Log ID: 629803ff8166e5b4a41ecf6a
Connecting to:          mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.4.2
MongoNetworkError: connect ECONNREFUSED 127.0.0.1:27017
```

# Hoje, 02/06/2022 o mongodb não tem suporte para ser executado pelo ubuntu 22.04.Só nos resta aguardar. Reinstalamos porém contiua comerro.


MongoDB 5.0 Community Edition supports the following 64-bit Ubuntu LTS (long-term support) releases on x86_64 architecture:

20.04 LTS ("Focal")
18.04 LTS ("Bionic")
16.04 LTS ("Xenial")


1)wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | sudo apt-key add -


2)sudo apt-get install gnupg


3)wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | sudo apt-key add -

echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-5.0.list


Startando
sudo systemctl start mongod

sena@sena-VirtualBox:~$ sudo systemctl start mongod
sena@sena-VirtualBox:~$ sudo systemctl status mongod
× mongod.service - MongoDB Database Server
     Loaded: loaded (/lib/systemd/system/mongod.service; enabled; vendor preset: enabled)
     Active: failed (Result: core-dump) since Thu 2022-06-02 04:54:37 -05; 37s ago
       Docs: https://docs.mongodb.org/manual
    Process: 35281 ExecStart=/usr/bin/mongod --config /etc/mongod.conf (code=dumped, signal=ILL)
   Main PID: 35281 (code=dumped, signal=ILL)
        CPU: 110ms

jun 02 04:54:36 sena-VirtualBox systemd[1]: Started MongoDB Database Server.
jun 02 04:54:37 sena-VirtualBox systemd[1]: mongod.service: Main process exited, code=dumped, status=4/ILL
jun 02 04:54:37 sena-VirtualBox systemd[1]: mongod.service: Failed with result 'core-dump'.
sena@sena-VirtualBox:~$

https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-ubuntu/

Com erro para essa versão.
