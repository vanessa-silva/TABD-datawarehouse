#!/usr/bin/python
import psycopg2
import sys
import pprint
import datetime
 



def cria_tabelas(cursor):
	cursor.execute("DROP TABLE IF EXISTS dw_stand CASCADE")
	cursor.execute("DROP TABLE IF EXISTS dw_local CASCADE")	
	cursor.execute("DROP TABLE IF EXISTS dw_tempo CASCADE")
	cursor.execute("DROP TABLE IF EXISTS dw_taxi CASCADE")
	cursor.execute("DROP TABLE IF EXISTS dw_taxi_services CASCADE")

	
	cursor.execute("create table dw_stand(stand_id int not null, nome text not null, PRIMARY KEY (stand_id))")

	cursor.execute("create table dw_local(local_id int not null, stand_id int not null, freguesia text not null, concelho text not null, PRIMARY KEY (local_id, stand_id))")

	cursor.execute("create table dw_tempo(tempo_id int not null, hora_I text not null, dia_I text not null, mes_I text not null, hora_F text not null, dia_F text not null, mes_F text not null, PRIMARY KEY (tempo_id))")

	cursor.execute("create table dw_taxi(taxi_id int not null, nViagens int not null, tempoTotal text not null, PRIMARY KEY (taxi_id))")

   	cursor.execute("create table dw_taxi_services(id int not null, taxi_id int not null, tempo_id int not null, local_I_id int not null, local_F_id int not null, tempoTotal text null, PRIMARY KEY (id, taxi_id, tempo_id, local_I_id, local_F_id))")




#	(id, nome, lotacao)
def dw_stand(cursor):

	cursor.execute("select count(*) from taxi_stands")
	nTuplos = cursor.fetchall()

	cursor.execute("select name from taxi_stands")
	nomes = cursor.fetchall()

	for i in range(0,nTuplos[0][0]):
		cursor.execute("insert into dw_stand (stand_id, nome) values (%s, %s)", (i+1, nomes[i][0]))
	



#	(id, stand_id, freguesia, concelho)
def dw_local(cursor):

	cursor.execute("DROP TABLE IF EXISTS dw_local CASCADE")	
	cursor.execute("create table dw_local(local_id int not null, stand_id int not null, freguesia text not null, concelho text not null, PRIMARY KEY (local_id, stand_id))")

	#stand_id
	cursor.execute("select stand_id from dw_stand")
	stand_id = cursor.fetchall()

	cursor.execute("select st_astext(location) from taxi_stands")
	location = cursor.fetchall()

	for i in range(len(stand_id)):
		cursor.execute("select freguesia from cont_freg_v5 where st_within(%s, st_astext(geom)) and distrito like 'PORTO'", (location[i][0],) )
		freguesia = cursor.fetchall()[0][0]

		cursor.execute("select concelho from cont_freg_v5 where st_within(%s, st_astext(geom)) and distrito like 'PORTO'", (location[i][0],) )
		concelho = cursor.fetchall()[0][0]
		print str(i+1) + ":" + str(concelho)
		
		cursor.execute("insert into dw_local (local_id, stand_id, freguesia, concelho) values (%s, %s, %s, %s)", (i+1, stand_id[i][0], freguesia, concelho,))	
	


#	(id, hora, dia, mes)
def dw_tempo(cursor):
	cursor.execute("select TIMESTAMP 'epoch' + initial_ts * INTERVAL '1 second', TIMESTAMP 'epoch' + final_ts * INTERVAL '1 second' from taxi_services")
	tempo = cursor.fetchall()

	for i in range(len(tempo)):
		horaI = str(tempo[i][0])[11] + str(tempo[i][0])[12] + str(tempo[i][0])[13] + str(tempo[i][0])[14] + str(tempo[i][0])[15]
		diaI = str(tempo[i][0])[8] + str(tempo[i][0])[9]
		mesI = str(tempo[i][0])[5] + str(tempo[i][0])[6]

		horaF = str(tempo[i][1])[11] + str(tempo[i][1])[12] + str(tempo[i][1])[13] + str(tempo[i][1])[14] + str(tempo[i][1])[15]
		diaF = str(tempo[i][1])[8] + str(tempo[i][1])[9]
		mesF = str(tempo[i][1])[5] + str(tempo[i][1])[6]

		cursor.execute("insert into dw_tempo (tempo_id, hora_I, dia_I, mes_I, hora_F, dia_F, mes_F) values (%s, %s, %s, %s, %s, %s, %s)", (i+1, horaI, diaI, mesI, horaF, diaF, mesF))




# (taxi_id, n_licenca)
def dw_taxi(cursor):

	cursor.execute("DROP TABLE IF EXISTS dw_taxi CASCADE")
	cursor.execute("create table dw_taxi(taxi_id int not null, nViagens int not null, tempoTotal text not null, PRIMARY KEY (taxi_id))")

	#taxi_id
	cursor.execute("select distinct taxi_id from taxi_services order by taxi_id")
	taxi_id = cursor.fetchall()

	for i in range(len(taxi_id)):
		#nViagens por taxi
		cursor.execute("select count(*) from taxi_services where taxi_id = %s", (str(taxi_id[i][0]),))
		nViagens = cursor.fetchall()[0][0]		

		#tempo total por taxi
		cursor.execute("select initial_ts from taxi_services where taxi_id = %s", (str(taxi_id[i][0]),))
		tempoI = cursor.fetchall()

		cursor.execute("select final_ts from taxi_services where taxi_id = %s", (str(taxi_id[i][0]),))
		tempoF = cursor.fetchall()

		tempoTotal=0;
		for j in range(nViagens):
			tempoTotal += tempoF[j][0]-tempoI[j][0]
		
		tempoTotal = datetime.timedelta(seconds=tempoTotal)

		cursor.execute("insert into dw_taxi (taxi_id, nViagens, tempoTotal) values (%s, %s, %s)", (taxi_id[i][0], nViagens, tempoTotal,))




#	(taxi_id, tempo_id, localI_id, localF_id, nViagens, tempoTotal)
def dw_taxi_services(cursor):

	cursor.execute("DROP TABLE IF EXISTS dw_taxi_services CASCADE")
   	cursor.execute("create table dw_taxi_services(id int not null, taxi_id int not null, tempo_id int not null, local_I_id int not null, local_F_id int not null, tempoTotal text null, PRIMARY KEY (id, taxi_id, tempo_id, local_I_id, local_F_id))")

	#local_I_id
	cursor.execute("select st_astext(initial_point) from taxi_services")
	pontoI = cursor.fetchall()

	#local_F_id
	cursor.execute("select st_astext(final_point) from taxi_services")
	pontoF = cursor.fetchall()

	for i in range(len(pontoI)):
		cursor.execute("select id, st_distance(st_astext(location), %s) from taxi_stands order by st_distance(st_astext(location), %s)", (pontoI[i][0],pontoI[i][0],))
		local_id_I = cursor.fetchall()[0][0]

		cursor.execute("select id, st_distance(st_astext(location), %s) from taxi_stands order by st_distance(st_astext(location), %s)", (pontoF[i][0],pontoF[i][0],))
		local_id_F = cursor.fetchall()[0][0]

		#taxi_id
		cursor.execute("select taxi_id from taxi_services where st_astext(initial_point) like %s and st_astext(final_point) like %s", (pontoI[i][0], pontoF[i][0],))
		taxi_id = cursor.fetchall()[0][0]

		#tempo_id
		cursor.execute("select TIMESTAMP 'epoch' + initial_ts * INTERVAL '1 second', TIMESTAMP 'epoch' + final_ts * INTERVAL '1 second' from taxi_services where  st_astext(initial_point) like %s and st_astext(final_point) like %s", (pontoI[i][0], pontoF[i][0],))
		tempo = cursor.fetchall()
		
		for j in range(len(tempo)):
			horaI = str(tempo[j][0])[11] + str(tempo[j][0])[12] + str(tempo[j][0])[13] + str(tempo[j][0])[14] + str(tempo[j][0])[15]
			diaI = str(tempo[j][0])[8] + str(tempo[j][0])[9]
			mesI = str(tempo[j][0])[5] + str(tempo[j][0])[6]

			horaF = str(tempo[j][1])[11] + str(tempo[j][1])[12] + str(tempo[j][1])[13] + str(tempo[j][1])[14] + str(tempo[j][1])[15]
			diaF = str(tempo[j][1])[8] + str(tempo[j][1])[9]
			mesF = str(tempo[j][1])[5] + str(tempo[j][1])[6]

		cursor.execute("select tempo_id from dw_tempo where dia_I like %s and mes_I like %s and hora_I like %s and dia_F like %s and mes_F like %s and hora_F like %s", (diaI, mesI, horaI, diaF, mesF, horaF,))
		tempo_id = cursor.fetchall()[0][0]
	
		#tempo total por taxi
		cursor.execute("select initial_ts from taxi_services where st_astext(initial_point) like %s and st_astext(final_point) like %s", (pontoI[i][0], pontoF[i][0],))
		tempoI = cursor.fetchall()[0][0]
		
		cursor.execute("select final_ts from taxi_services where st_astext(initial_point) like %s and st_astext(final_point) like %s", (pontoI[i][0], pontoF[i][0],))
		tempoF = cursor.fetchall()[0][0]
		
		tempoTotal = tempoF-tempoI
		tempoTotal = datetime.timedelta(seconds=tempoTotal)
		
		cursor.execute("insert into dw_taxi_services (id, taxi_id, tempo_id, local_I_id, local_F_id, tempoTotal) values (%s, %s, %s, %s, %s, %s)", (i+1, taxi_id, tempo_id, local_id_I, local_id_F, tempoTotal,))

		print i
		



if __name__ == "__main__":
	conn_string = "dbname='guest' user='guest' host = '127.0.0.1' password=''"
	conn = psycopg2.connect(conn_string)
	conn.set_client_encoding('LATIN9')
	cursor = conn.cursor()
 
	#cria_tabelas(cursor)
	#dw_stand(cursor)
	#dw_taxi(cursor)
	#dw_tempo(cursor)

	#dw_local(cursor)
	dw_taxi_services(cursor)

	conn.commit()
	cursor.close()
	conn.close()
