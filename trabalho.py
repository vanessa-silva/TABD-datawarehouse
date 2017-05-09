#!/usr/bin/python
import psycopg2
import sys
import pprint
import datetime

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
	#dw_local(cursor)
	#dw_tempo(cursor)
	dw_services(cursor)

	conn.commit()
	cursor.close()
	conn.close()
	
def cria_tabelas(cursor):
	cursor.execute("DROP TABLE IF EXISTS dw_stand")
	cursor.execute("DROP TABLE IF EXISTS dw_local")	
	cursor.execute("DROP TABLE IF EXISTS dw_tempo")
	cursor.execute("DROP TABLE IF EXISTS dw_taxi")
	cursor.execute("DROP TABLE IF EXISTS dw_taxi_services")

	cursor.execute("create table dw_tempo(id int primary key not null, hora text not null, dia text not null, mes text not null)")
	cursor.execute("create table dw_local(id int primary key not null, stand_id int not null, freguesia text not null, concelho text not null)")
	cursor.execute("create table dw_taxi(id int primary key not null, n_licenca int not null)")
	cursor.execute("create table dw_stand(id int primary key not null, nome text not null, lotacao int null, location text not null)")
	cursor.execute("create table dw_taxi_services(id int primary key not null, taxi_id int not null, tempo_id int not null, localI_id int not null, localF_id int not null, nViagens int not null, tempoTotal int not null)")


def transfereTaxiStands(cursor):

	cursor.execute("select count(*) from taxi_stands")
	nTuplos = cursor.fetchall()

	cursor.execute("select id from taxi_stands")
	ids = cursor.fetchall()

	cursor.execute("select name from taxi_stands")
	nomes = cursor.fetchall()
	
	cursor.execute("select location from taxi_stands")
	coordenadas = cursor.fetchall()

	for i in range(0,nTuplos[0][0]):
		cursor.execute("insert into dw_stand (id, nome, location) values (%s, %s, %s)", (ids[i][0], nomes[i][0],coordenadas[i][0],))
	

#id, stand_id, freguesia, concelho
def dw_local(cursor):

	#Freguesias existentes no Porto
	cursor.execute("select distinct freguesia from cont_freg_v5 where distrito like 'PORTO'")
	freguesia_r = cursor.fetchall()

	#Concelhos existentes no porto
	cursor.execute("select distinct concelho from cont_freg_v5 where distrito like 'PORTO'")
	concelho_r = cursor.fetchall()
	
	#numero de tupulos dos stands
	cursor.execute("select count(*) from dw_stand")
	nTuplos = cursor.fetchall()

	cursor.execute("select id from dw_stand")
	stand_id = cursor.fetchall()
	
	cursor.execute("select freguesia, concelho from cont_freg_v5 where distrito like 'PORTO' order by concelho")
	freg_con = cursor.fetchall()


	freg = [[]]
	for i in range(0, len(concelho_r)):
		for j in range(0, len(freg_con)):
			if j == len(freg_con)-1:
				freg[i].append(freg_con[j][0])
				break

			if freg_con[j][1] != freg_con[j+1][1]:
				freg[i].append(freg_con[j][0]) 
				break
			else:
				freg[i].append(freg_con[j][0])
			
		
				
	print len(freg[0])

	#freg = [[]]
	#for i in range(0,nTuplos[0][0]):
	#	cursor.execute("select freguesia from cont_freg_v5 where concelho like %s", (concelho_r[i][0],))
	#	freg[i].append(i)
		
	#print freg

	#freguesia
#	cursor.execute("select freguesia from cont_freg_v5 where distrito like 'PORTO' and geom like %s", (coordenada[i][0],))
#	freguesia = cursor.fetchall()

	#Concelhos 
#	for i in range(0,nTuplos[0][0]):
#		cursor.execute("select concelho from cont_freg_v5 where distrito like 'PORTO' and freguesia like %s", (freguesia[i][0],))
#	concelho = cursor.fetchall()

	#for i in range(0,nTuplos[0][0]):
	#	cursor.execute("insert into dw_local (id, stand_id, freguesia, concelho) values (%s, %s, %s, %s)", (i, stand_id[i][0], freguesia[i][0],concelho[i][0],))


#id, hora, dia, mes
def dw_tempo(cursor):
	cursor.execute("select TIMESTAMP 'epoch' + initial_ts * INTERVAL '1 second' from taxi_services")
	tempoI = cursor.fetchall()

	for i in range(len(tempoI)):
		hora = str(tempoI[i][0])[11] + str(tempoI[i][0])[12]
		dia = str(tempoI[i][0])[8] + str(tempoI[i][0])[9]
		mes = str(tempoI[i][0])[5] + str(tempoI[i][0])[6]

		cursor.execute("insert into dw_tempo (id, hora, dia, mes) values (%s, %s, %s, %s)", (i, hora, dia, mes))
	

def dw_services(cursor):

	cursor.execute("select final_ts - initial_ts from taxi_services")
	tempo = cursor.fetchall()
	
	for i in range(len(tempoI)):
		str(datetime.timedelta(seconds=tempo[0][0]))

	cursor.execute("select TIMESTAMP 'epoch' + initial_ts * INTERVAL '1 second' from taxi_services")
	print cursor.fetchall()[0][0]	
	cursor.execute("select TIMESTAMP 'epoch' + final_ts * INTERVAL '1 second' from taxi_services")
	print cursor.fetchall()[0][0]
	
	
if __name__ == "__main__":
	main()
