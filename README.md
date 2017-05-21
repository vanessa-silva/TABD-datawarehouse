# TABD-datawarehouse
Trabalho prático de Tópico Avançados em Base de Dados.


##Fazer download do CAOP e extrair os ficheiros:
wget http://ftp.igeo.pt/produtos/cadastro/caop/shapes_v5.htm
unzip cont_freg_v5.zip

##Exportar as tabelas do CAOP para a base de dados:
shp2pgsql -I -d -s 27492:4326 -W "latin1" Cont_Freg_V5.shp | psql -U guest guest

##Compilar o programa que irá gerar a data warehouse
python "trabalho-final.py"
