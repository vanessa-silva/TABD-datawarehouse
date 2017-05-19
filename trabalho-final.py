#!/usr/bin/python
import psycopg2
import sys
import pprint
import datetime
 

def dw_stand(cursor):

	cursor.execute("DROP TABLE IF EXISTS dw_stand CASCADE")
	cursor.execute("create table dw_stand(stand_id int not null, nome text not null, PRIMARY KEY (stand_id))")

	cursor.execute("select count(*) from taxi_stands")
	nTuplos = cursor.fetchall()

	cursor.execute("select name from taxi_stands")
	nomes = cursor.fetchall()

	for i in range(0,nTuplos[0][0]):
		cursor.execute("insert into dw_stand (stand_id, nome) values (%s, %s)", (i+1, nomes[i][0]))
	



def dw_local(cursor):

	cursor.execute("DROP TABLE IF EXISTS dw_local CASCADE")	
	cursor.execute("create table dw_local(local_id int not null, stand_id int null, freguesia text not null, concelho text not null, PRIMARY KEY (local_id), FOREIGN KEY(stand_id) REFERENCES dw_stand (stand_id))")

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
		
		cursor.execute("insert into dw_local (local_id, stand_id, freguesia, concelho) values (%s, %s, %s, %s)", (i+1, stand_id[i][0], freguesia, concelho,))	

	


def dw_tempo(cursor):

	cursor.execute("DROP TABLE IF EXISTS dw_tempo CASCADE")
	cursor.execute("create table dw_tempo(tempo_id int not null, hora_I text not null, dia_I text not null, mes_I text not null, hora_F text not null, dia_F text not null, mes_F text not null, PRIMARY KEY (tempo_id))")

	cursor.execute("select cast((TIME '00:00' + initial_ts * INTERVAL '1 second') as text), cast(date_part('day', (TIMESTAMP 'epoch' + initial_ts * INTERVAL '1 second')) as text), cast(date_part('month', (TIMESTAMP 'epoch' + initial_ts * INTERVAL '1 second')) as text), cast((TIME '00:00' + final_ts * INTERVAL '1 second') as text), cast(date_part('day', (TIMESTAMP 'epoch' + final_ts * INTERVAL '1 second')) as text), cast(date_part('month', (TIMESTAMP 'epoch' + final_ts * INTERVAL '1 second')) as text) from taxi_services")
	Res = cursor.fetchall()

	for i in range(len(Res)):
		cursor.execute("insert into dw_tempo (tempo_id, hora_I, dia_I, mes_I, hora_F, dia_F, mes_F) values (%s, %s, %s, %s, %s, %s, %s)", (i+1, Res[i][0], Res[i][1], Res[i][2], Res[i][3], Res[i][4], Res[i][5]))




def dw_taxi(cursor):

	cursor.execute("DROP TABLE IF EXISTS dw_taxi CASCADE")
	cursor.execute("create table dw_taxi(taxi_id int not null, nLicenca int not null, PRIMARY KEY (taxi_id))")

	#licenca
	cursor.execute("select distinct taxi_id from taxi_services")
	nLicenca = cursor.fetchall()

	for i in range(len(nLicenca)):
		cursor.execute("insert into dw_taxi (taxi_id, nLicenca) values (%s, %s)", (i+1, nLicenca[i][0],))




def dw_taxi_services(cursor):

	cursor.execute("DROP TABLE IF EXISTS dw_taxi_services CASCADE")
   	cursor.execute("create table dw_taxi_services(id int not null, taxi_id int not null, tempo_id int not null, local_I_id int null, local_F_id int null, tempoTotal text null, PRIMARY KEY (id), FOREIGN KEY(taxi_id) REFERENCES dw_taxi (taxi_id), FOREIGN KEY(tempo_id) REFERENCES dw_tempo (tempo_id), FOREIGN KEY(local_I_id) REFERENCES dw_local (local_id), FOREIGN KEY(local_F_id) REFERENCES dw_local (local_id))")


	cursor.execute("SELECT dw_taxi.taxi_id, T.tempo_id, (SELECT id FROM taxi_stands WHERE st_distance(initial_point, location)  = (SELECT MIN(st_distance(initial_point, location)) FROM taxi_stands)), (SELECT id FROM taxi_stands WHERE st_distance(final_point, location)  = (SELECT MIN(st_distance(final_point, location)) FROM taxi_stands)), SUM(final_ts - initial_ts) * INTERVAL '1 second' FROM taxi_services INNER JOIN dw_taxi ON taxi_services.taxi_id=dw_taxi.nLicenca INNER JOIN dw_tempo as T ON cast((TIME '00:00' + initial_ts * INTERVAL '1 second') as text) = T.hora_i AND cast(date_part('day', (TIMESTAMP 'epoch' + initial_ts * INTERVAL '1 second')) as text) = T.dia_i AND cast(date_part('month', (TIMESTAMP 'epoch' + initial_ts * INTERVAL '1 second')) as text) = T.mes_i AND cast((TIME '00:00' + final_ts * INTERVAL '1 second') as text) = T.hora_f AND cast(date_part('day', (TIMESTAMP 'epoch' + final_ts * INTERVAL '1 second')) as text) = T.dia_f AND cast(date_part('month', (TIMESTAMP 'epoch' + final_ts * INTERVAL '1 second')) as text) = T.mes_f  GROUP BY 1,2,3,4 ORDER BY 1,2 ASC")
	nTuplos = cursor.fetchall()

	for i in range(len(nTuplos)):
		cursor.execute("insert into dw_taxi_services (id, taxi_id, tempo_id, local_I_id, local_F_id, tempoTotal) values (%s, %s, %s, %s, %s, %s)", (i+1, nTuplos[i][0], nTuplos[i][1], nTuplos[i][2], nTuplos[i][3], str(nTuplos[i][4]),))	




if __name__ == "__main__":
	conn_string = "dbname='guest' user='guest' host = '127.0.0.1' password=''"
	conn = psycopg2.connect(conn_string)
	conn.set_client_encoding('LATIN9')
	cursor = conn.cursor()
 
	dw_tempo(cursor)	
	dw_taxi(cursor)		
	dw_stand(cursor)
	dw_local(cursor)			
	dw_taxi_services(cursor)	

	conn.commit()
	cursor.close()
	conn.close()
