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

	
	cursor.execute("create table dw_stand(stand_id int not null, nome text not null, lotacao int null, location text not null, PRIMARY KEY (stand_id))")

	cursor.execute("create table dw_local(local_id int not null, stand_id int not null, freguesia text not null, concelho text not null, PRIMARY KEY (local_id, stand_id))")

	#, FOREIGN KEY (stand_id) REFERENCES dw_stand (stand_id) ON DELETE CASCADE)")

	cursor.execute("create table dw_tempo(tempo_id int not null, hora_I text not null, dia_I text not null, mes_I text not null, hora_F text not null, dia_F text not null, mes_F text not null, PRIMARY KEY (tempo_id))")

	cursor.execute("create table dw_taxi(taxi_id int not null, n_licenca int not null, PRIMARY KEY (taxi_id))")

   	cursor.execute("create table dw_taxi_services(id int not null, taxi_id int not null, tempo_id int not null, local_I_id int not null, local_F_id int not null, nViagens int not null, tempoTotal text null, PRIMARY KEY (id, taxi_id, tempo_id, local_I_id, local_F_id))")
	#, FOREIGN KEY (taxi_id) REFERENCES dw_taxi (taxi_id) ON DELETE CASCADE, FOREIGN KEY (tempo_id) REFERENCES dw_tempo (tempo_id) ON DELETE CASCADE)")




#	(id, nome, lotacao, location)
def dw_stand(cursor):

	cursor.execute("select count(*) from taxi_stands")
	nTuplos = cursor.fetchall()

	cursor.execute("select name from taxi_stands")
	nomes = cursor.fetchall()
	
	cursor.execute("select location from taxi_stands")
	coordenadas = cursor.fetchall()

	for i in range(0,nTuplos[0][0]):
		cursor.execute("insert into dw_stand (stand_id, nome, location) values (%s, %s, %s)", (i+1, nomes[i][0], coordenadas[i][0]))
	



#	(id, stand_id, freguesia, concelho)
def dw_local(cursor):

	cursor.execute("DROP TABLE IF EXISTS dw_local CASCADE")	
	cursor.execute("create table dw_local(local_id int not null, stand_id int not null, freguesia text not null, concelho text not null, PRIMARY KEY (local_id, stand_id))")

	#stand_id
	cursor.execute("select stand_id from dw_stand")
	stand_id = cursor.fetchall()
	
	cursor.execute("select freguesia, concelho from cont_freg_v5 where distrito like 'PORTO' order by concelho")
	freg_con = cursor.fetchall()

	

	freguesia = 'ALFENA'
	
	#concelho
	cursor.execute("select distinct concelho from cont_freg_v5 where distrito like 'PORTO' and freguesia like %s", (freguesia,))
	concelho = cursor.fetchall()[0][0]

	for i in range(len(stand_id)):
		cursor.execute("insert into dw_local (local_id, stand_id, freguesia, concelho) values (%s, %s, %s, %s)", (i+1, stand_id[i][0], 0, concelho,))



'''
	cursor.execute("select distinct concelho from cont_freg_v5 where distrito like 'PORTO'")
	concelho_r = cursor.fetchall()

	for i in range(len(concelho_r)):
		for j in range(len(freg_con)):
			if j == len(freg_con)-1:
				freg[i].append(freg_con[j][0])
				break

			if freg_con[j][1] != freg_con[j+1][1]:
				freg[i].append(freg_con[j][0]) 
				break
			else:
				freg[i].append(freg_con[j][0])		
				
	print len(freg[0])
'''
	
	


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
	#taxi_id
	cursor.execute("select distinct taxi_id from taxi_services")
	taxi_id = cursor.fetchall()

	#numero de linhas que vao existir na tabela
	nTuplos = len(taxi_id)
	
	for i in range(nTuplos):
		cursor.execute("insert into dw_taxi (taxi_id, n_licenca) values (%s, %s)", (taxi_id[i][0], 0,))




#	(taxi_id, tempo_id, localI_id, localF_id, nViagens, tempoTotal)
def dw_services(cursor):

	cursor.execute("DROP TABLE IF EXISTS dw_taxi_services CASCADE")
   	cursor.execute("create table dw_taxi_services(id int not null, taxi_id int not null, tempo_id int not null, local_I_id int not null, local_F_id int not null, nViagens int not null, tempoTotal text null, PRIMARY KEY (id, taxi_id, tempo_id, local_I_id, local_F_id))")


	'''
	ESTAMOS A FAZER AS PESQUISAS EM RELACAO AO TAXI ID MAS VAI SER EM RELACAO AO LOCAL_I LOCAL_F E TAXI_ID
	'''

	#taxi_id
	cursor.execute("select taxi_id from dw_taxi")
	taxi_id = cursor.fetchall()

	#numero de linhas que vao existir na tabela
	nTuplos = len(taxi_id)
	
	for i in range(nTuplos):

		#tempo_id
		cursor.execute("select TIMESTAMP 'epoch' + initial_ts * INTERVAL '1 second', TIMESTAMP 'epoch' + final_ts * INTERVAL '1 second' from taxi_services where taxi_id = %s", (str(taxi_id[i][0]),))
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

		#nViagens por taxi
		cursor.execute("select count(*) from taxi_services where taxi_id = %s", (str(taxi_id[i][0]),))
		nViagens = cursor.fetchall()[0][0]		

		#tempo total por taxi
		cursor.execute("select initial_ts from taxi_services where taxi_id = %s", (str(taxi_id[i][0]),))
		tempoI = cursor.fetchall()

		cursor.execute("select final_ts from taxi_services where taxi_id = %s", (str(taxi_id[i][0]),))
		tempoF = cursor.fetchall()

		tempoTotal=0;
		for j in range(nViagens[0][0]):
			tempoTotal += tempoF[j][0]-tempoI[j][0]
			
		tempoTotal = datetime.timedelta(seconds=tempoTotal)
#		tempoTotal = (datetime.datetime.min + datetime.timedelta(seconds=tempoTotal)).time()		ESTA FORMA APRESENTA SO EM HORAS MAS DEPOIS FALTA OS DIAS
		
		if i==5:
			break
		
		cursor.execute("insert into dw_taxi_services (id, taxi_id, tempo_id, local_I_id, local_F_id, nViagens, tempoTotal) values (%s, %s, %s, %s, %s, %s, %s)", (i+1, taxi_id[i][0], tempo_id, 0, 0, nViagens, tempoTotal,))
		



if __name__ == "__main__":
	conn_string = "dbname='guest' user='guest' host = '127.0.0.1' password=''"
	conn = psycopg2.connect(conn_string)
	conn.set_client_encoding('LATIN9')
	cursor = conn.cursor()
 
	#cria_tabelas(cursor)
	#dw_stand(cursor)
	#dw_taxi(cursor)
	#dw_tempo(cursor)
	dw_local(cursor)
	dw_services(cursor)

	conn.commit()
	cursor.close()
	conn.close()
