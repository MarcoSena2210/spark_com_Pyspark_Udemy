# Extraindo Dados de MongoDb

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


### •Importar Json exemplo,banco de dados Posts,coleção post
### •Instalar Package para Spark
### •Usar o Pyspark para ler, e gravar dados no Mongodb


sudo apt-get install -y mongodb-org

sudo systemctl start mongod

Pendente instalação:

sena@sena-VirtualBox:~$ wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | sudo apt-key add -

[sudo] senha para sena:
Warning: apt-key is deprecated. Manage keyring files in trusted.gpg.d instead (see apt-key(8)).
OK
sena@sena-VirtualBox:~$ echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-5.0.list
deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse
sena@sena-VirtualBox:~$ sudo apt-get update
Atingido:1 http://br.archive.ubuntu.com/ubuntu jammy InRelease
Obter:2 http://br.archive.ubuntu.com/ubuntu jammy-updates InRelease [109 kB]
Obter:3 http://security.ubuntu.com/ubuntu jammy-security InRelease [110 kB]
Obter:4 http://br.archive.ubuntu.com/ubuntu jammy-backports InRelease [99,8 kB]
Obter:5 http://br.archive.ubuntu.com/ubuntu jammy-updates/main amd64 Packages [212 kB]
Obter:6 http://br.archive.ubuntu.com/ubuntu jammy-updates/main i386 Packages [99,7 kB]
Obter:7 http://br.archive.ubuntu.com/ubuntu jammy-updates/main amd64 DEP-11 Metadata [57,5 kB]
Obter:8 http://br.archive.ubuntu.com/ubuntu jammy-updates/universe amd64 Packages [98,8 kB]
Obter:9 http://br.archive.ubuntu.com/ubuntu jammy-updates/universe i386 Packages [37,2 kB]
Obter:10 http://br.archive.ubuntu.com/ubuntu jammy-updates/universe amd64 DEP-11 Metadata [87,8 kB]
Ign:11 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 InRelease
Ign:12 http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 InRelease
Obter:13 http://br.archive.ubuntu.com/ubuntu jammy-backports/universe amd64 DEP-11 Metadata [1.196 B]
Atingido:14 https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 Release
Obter:15 http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 Release [3.462 B]
Obter:16 http://security.ubuntu.com/ubuntu jammy-security/main amd64 DEP-11 Metadata [11,4 kB]
Obter:17 http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 Release.gpg [801 B]
Ign:17 http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 Release.gpg
Obter:19 http://apt.postgresql.org/pub/repos/apt jammy-pgdg InRelease [91,7 kB]
Lendo listas de pacotes... Pronto
W: https://repo.mongodb.org/apt/ubuntu/dists/focal/mongodb-org/5.0/Release.gpg: Key is stored in legacy trusted.gpg keyring (/etc/apt/trusted.gpg), see the DEPRECATION section in apt-key(8) for details.
W: Erro GPG: http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 Release: As assinaturas a seguir não puderam ser verificadas devido à chave pública não estar disponível: NO_PUBKEY D68FA50FEA312927
E: O repositório 'http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 Release' não está assinado.
N: Atualizações a partir de tal repositório não podem ser feitas de forma segura e estão, portanto, desativadas por definição.
N: See apt-secure(8) manpage for repository creation and user configuration details.
W: http://apt.postgresql.org/pub/repos/apt/dists/jammy-pgdg/InRelease: Key is stored in legacy trusted.gpg keyring (/etc/apt/trusted.gpg), see the DEPRECATION section in apt-key(8) for details.
N: Skipping acquire of configured file 'main/binary-i386/Packages' as repository 'http://apt.postgresql.org/pub/repos/apt jammy-pgdg InRelease' doesn't support architecture 'i386'
sena@sena-VirtualBox:~$ sudo apt-get install -y mongodb-org
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
E: Impossível corrigir problemas, você manteve (hold) pacotes quebrados.
sena@sena-VirtualBox:~$ sudo systemctl start mongod

# Failed to start mongod.service: Unit mongod.service not found.

sena@sena-VirtualBox:~$
