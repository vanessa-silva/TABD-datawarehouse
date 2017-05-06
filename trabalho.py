#!/usr/bin/python
import psycopg2
import sys
import pprint

#conn.set_client_encoding('LATIN9')
 
def main():


	#Define our connection string
	conn_string = "dbname='guest' user='guest' host = '127.0.0.1' password=''"
 
	# print the connection string we will use to connect
	#print "Connecting to database\n	->%s" % (conn_string)
 
	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
 
	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()
 
	cria_tabelas(cursor)

	transfereTaxiStands(cursor)
	local(cursor)

	conn.commit()
	cursor.close()
	conn.close()
	
def cria_tabelas(cursor):
	cursor.execute("DROP TABLE IF EXISTS dw_stand")
	cursor.execute("DROP TABLE IF EXISTS dw_local")	
	cursor.execute("DROP TABLE IF EXISTS dw_tempo")
	cursor.execute("DROP TABLE IF EXISTS dw_taxi")
	cursor.execute("DROP TABLE IF EXISTS dw_taxi_services")

	cursor.execute("create table dw_tempo(id int primary key not null, hora int not null, dia int not null, mes int not null)")
	cursor.execute("create table dw_local(id int primary key not null, stand_id int not null, freguesia text not null, concelho text not null)")
	cursor.execute("create table dw_taxi(id int primary key not null, n_licenca int not null)")
	cursor.execute("create table dw_stand(id int primary key not null, nome text not null, lotacao int null)")
	cursor.execute("create table dw_taxi_services(id int primary key not null, taxi_id int not null, tempo_id int not null, localI_id int not null, localF_id int not null, nViagens int not null, tempoTotal int not null)")


def transfereTaxiStands(cursor):

	cursor.execute("select count(*) from taxi_stands")
	nTuplos = cursor.fetchall()

	cursor.execute("select id from taxi_stands")
	ids = cursor.fetchall()

	cursor.execute("select name from taxi_stands")
	nomes = cursor.fetchall()
	
	for i in range(0,nTuplos[0][0]):
		cursor.execute("insert into dw_stand (id, nome) values (%s, %s)", (ids[i][0], nomes[i][0],))
	

def local(cursor):

	#Freguesia existentes no Porto
	cursor.execute("select distinct count(freguesia) from cont_freg_v5 where distrito like 'PORTO'")
	freguesia = cursor.fetchall()

	#Concelhos existentos no porto
	cursor.execute("select distinct concelho from cont_freg_v5 where distrito like 'PORTO'")
	concelho = cursor.fetchall()

	barra = "\d";

	#bar = cursor.fetchall()
	#cursor.execute("%s", barra)

#	pprint.pprint(barra[0])


if __name__ == "__main__":
	main()
