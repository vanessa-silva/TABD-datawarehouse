#!/usr/bin/python
import psycopg2
import sys
import pprint
 
def main():
	#Define our connection string
	conn_string = "dbname='guest' user='guest' host = '127.0.0.1' password=''"
 
	# print the connection string we will use to connect
	#print "Connecting to database\n	->%s" % (conn_string)
 
	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
 
	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()
 
	local(cursor);

def local(cursor):
	# execute our Query
	cursor.execute("select location from taxi_stands limit 1")
 
	# retrieve the records from the database
	records = cursor.fetchall()
 
	pprint.pprint(records)


if __name__ == "__main__":
	main()
