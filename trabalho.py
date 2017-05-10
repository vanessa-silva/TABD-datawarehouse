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

	
	cursor.execute("create table dw_stand(stand_id bigserial, nome text not null, lotacao int null, location text not null, PRIMARY KEY (stand_id))")

	#cursor.execute("create table dw_local(local_id bigserial, stand_id int not null, freguesia text not null, concelho text not null, PRIMARY KEY (local_id, stand_id), FOREIGN KEY (stand_id) REFERENCES dw_stand (stand_id) ON DELETE CASCADE)")
        
        cursor.execute("create table dw_local(local_id bigserial, stand_id int not null, freguesia text not null, concelho text not null, PRIMARY KEY (local_id, stand_id))")

	cursor.execute("create table dw_tempo(tempo_id bigserial, hora text not null, dia text not null, mes text not null, PRIMARY KEY (tempo_id))")

	cursor.execute("create table dw_taxi(taxi_id bigserial, n_licenca int not null, PRIMARY KEY (taxi_id))")

	#cursor.execute("create table dw_taxi_services(id bigserial, taxi_id int not null, tempo_id int not null, localI_id int not null, nViagens int not null, tempoTotal text not null, PRIMARY KEY (id, taxi_id, tempo_id, localI_id), FOREIGN KEY (taxi_id) REFERENCES dw_taxi (taxi_id) ON DELETE CASCADE, FOREIGN KEY (tempo_id) REFERENCES dw_tempo (tempo_id) ON DELETE CASCADE, FOREIGN KEY (localI_id) REFERENCES dw_local (local_id) ON DELETE CASCADE)")


        cursor.execute("create table dw_taxi_services(id bigserial, taxi_id int not null, tempo_id int not null, localI_id int not null, nViagens int not null, tempoTotal text not null, PRIMARY KEY (id))")


def dw_stand(cursor):

	cursor.execute("select count(*) from taxi_stands")
	nTuplos = cursor.fetchall()

	cursor.execute("select name from taxi_stands")
	nomes = cursor.fetchall()
	
	cursor.execute("select location from taxi_stands")
	coordenadas = cursor.fetchall()

	for i in range(0,nTuplos[0][0]):
		cursor.execute("insert into dw_stand (nome, location) values (%s, %s)", (nomes[i][0],coordenadas[i][0],))
	

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
	#	cursor.execute("insert into dw_local (stand_id, freguesia, concelho) values (%s, %s, %s)", (stand_id[i][0], freguesia[i][0],concelho[i][0],))


#id, hora, dia, mes
def dw_tempo(cursor):
	cursor.execute("select TIMESTAMP 'epoch' + initial_ts * INTERVAL '1 second' from taxi_services")
	tempoI = cursor.fetchall()

	for i in range(len(tempoI)):
		hora = str(tempoI[i][0])[11] + str(tempoI[i][0])[12]
		dia = str(tempoI[i][0])[8] + str(tempoI[i][0])[9]
		mes = str(tempoI[i][0])[5] + str(tempoI[i][0])[6]

		cursor.execute("insert into dw_tempo (hora, dia, mes) values (%s, %s, %s)", (hora, dia, mes))
	

#taxi_id, tempo_id, localI_id, localF_id, nViagens, tempoTotal
def dw_services(cursor):

	#taxi_id
	cursor.execute("select distinct taxi_id from taxi_services")
	taxi_id = cursor.fetchall()

	#numero de linhas que vao existir na tabela
	nTuplos = len(taxi_id)
	
	for i in range(nTuplos):
		#nViagens por taxi
		cursor.execute("select count(*) from taxi_services where taxi_id = %s", (str(taxi_id[i][0]),))
		nViagens = cursor.fetchall()		
	
		#tempo total por taxi
		cursor.execute("select final_ts - initial_ts from taxi_services where taxi_id = %s", (str(taxi_id[i][0]),))
		tempo = cursor.fetchall()
		tempoTotal = str(datetime.timedelta(seconds=tempo[i][0]))

		#cursor.execute("insert into dw_taxi_services (taxi_id, nViagens, tempoTotal) values (%s, %s, %s)", (taxi_id[i][0], nViagens[i][0], tempoTotal,))
		break


if __name__ == "__main__":
	conn_string = "dbname='guest' user='guest' host = '127.0.0.1' password=''"
	conn = psycopg2.connect(conn_string)
	conn.set_client_encoding('LATIN9')
	cursor = conn.cursor()
 
	cria_tabelas(cursor)
	dw_stand(cursor)
	#dw_local(cursor)
	#dw_tempo(cursor)
	#dw_services(cursor)

	conn.commit()
	cursor.close()
	conn.close()
