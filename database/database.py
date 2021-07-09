#!/usr/bin/env python

''' Python class for communicating with a PostgreSQL data
This is a database wrapper class using the Psycopg2 Python library to perform SQL queries. This class
hold methods for SELECT/INSERT/UPDATE/DELETE and basic querying.
Date: 2020-02-05
Methods in the class:
	select: (self, table, fields, where = None, orderby = None, limit = None)
	insert(self, table, fields, values, upsert=False, conflict_on=None):
	
To do list:
1. pass values to update and delete 
2. we already have copy_stringio() in database, try to develop copy_string_iterator()
2. cur.close(), how many cur we can have
4. input format of the execute_values fucntion, array_tuple, not nescessary to be a list with tuple element. Two diemensional array works.
5. change this insert_execute_values_iterator method to true iterator one 
6. remove the idle connectios 
'''

__author__ = "Bobby Leary,Liming Gao"
__email__ = "winstonglm@gmail.com"
__status__ = "Developing"

import numpy as np
import psycopg2
import traceback
from psycopg2.extras import execute_values

class Database:

	def __init__(self, db_name):

		# Connect to the PostgreSQL database
		try:

			# self.conn = psycopg2.connect(database = db_name)
			self.conn = psycopg2.connect(user="ivsg_db_user",
                                  password="ivsg@DB320",
                                  host="130.203.223.234",
                                  port="5432",
                                  database=db_name)
			self.cur = self.conn.cursor()
			self.cur_dict = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
			print('Connect to '+db_name+' successfully!')
		except psycopg2.Error as e:
                          
			self.printErrors(error = e, message = "Unable to connect to the database")

	'''
		============================= Method select() ====================================
		#	Method Purpose:
		#		select is used to query the the data out
		#	Input Variable:
		#		self, table, fields, where ,orderby, limit
		#
		#	Output/Return:
		#		rows 			the rows of result
		#		status 			the query result
		#
		#	Algorithm:
				SELECT id,name,age, count(*) as num_age
				FROM students as s
				WHERE s.id =12
					AND s.age <18
			    ORDER BY s.id
				GROUP BY age
				LIMIT 4;
		#
		# 	Restrictions/Notes:
		# 		None
		#
		# 	The follow methods are called:
		# 		self.query()
		#		cur.fetchall()
		# 	Author: Liming Gao
		# 	Date: 02/05/2020
		#
		================================================================================
	'''

	def select(self, table, fields, where = None, orderby = None, limit = None):

		query =  "SELECT " + ", ".join(fields)

		if table is not None:

			query += " FROM " + table

		if where is not None:

			query += " WHERE " + " AND ".join(where)

		if orderby is not None:

			query += " ORDER BY " + orderby

		if limit is not None:

			query += " LIMIT " + str(limit)

		# for query, only pass the query SQL statement to the method, values are not required 
		cur, status = self.query(query = query, values = None, message = 'Unable to execute SELECT query')  

		try:

			rows = cur.fetchall()  #fetches all rows in the result set

		except psycopg2.ProgrammingError as e:

			rows = []

		if len(rows) == 0:

			return None, False

		if len(rows) == 1:

			return rows[0], status

		else:

			return rows, status

	'''
		============================= Method insert() ====================================
		#	Method Purpose:
		#		insert single row into table
		#
		#	Input Variable:
		#		self, table, fields, values ,
				upsert:			When you insert a new row into the table, PostgreSQL will update the row if it already exists, 
								otherwise, PostgreSQL inserts the new row. action is upsert (update or insert).
				conflict_on: 	If upsert is true and conflict_on is false, take action only when (id) conflicts
		#
		#	Output/Return:
		#		insert_id
		#		status 			the query result
		#
		#	Algorithm:
				INSERT INTO table(name,age)
				VALUES ('%s','%s')
				ON CONFLICT target action
				RETURNING id
				target can be: 1. (column_name) 2.ON CONSTRAINT constraint_name, the constraint name could be a name of the UNIQUE constraint.
				action can be:  1. DO NOTHING means do nothing if the row already exists in the table. 2. DO UPDATE SET column_1 = value_1, WHERE condition update some fields in the table.
		#
		# 	Restrictions/Notes:
		# 		None
		#
		# 	The follow methods are called:
		# 		self.query()
		#		cur.fetchone()  insert ID
		# 	Author: Liming Gao
		# 	Date: 02/05/2020
		#
		================================================================================
	'''

	def insert(self, table, fields, values, upsert=False, conflict_on=None):

		query =  "INSERT INTO " + table
		query += " (" + ", ".join(fields) + ") " 
		query += "VALUES (" + ", ".join(['%s' for x in fields]) + ") "

		if upsert is False:

			query += "RETURNING id;"

		else:

			query += "ON CONFLICT "
			if conflict_on is None:

				query += "(id) "

			else:

				query += "(" + ", ".join(conflict_on) + ") "
			query += "DO UPDATE "

			query += "SET "
			i = 0
			for field in fields:

				query += field + "='" + str(values[i]) + "'"
				''' SET field1 = 'values1', field2= 'values2',... '''
				if i != len(fields)-1:

					query += ", "

				i += 1

			# query += " DO NOTHING"

			query += " RETURNING id;"

		cur, status = self.query(query = query, values = values, message = 'Unable to execute INSERT query')

		insert_id = cur.fetchone()

		if insert_id is not None:
			insert_id = insert_id[0]   #return the insert id

		self.conn.commit()

		return insert_id, status


	'''
		============================= Method insert_CSVfile() ====================================
		#	Method Purpose:
		#		insert the rows in a CSV file into table
		#	
		#	Input Variable: 
		#		self, table, fields, file_name
		# 	
		#	Output/Return: 
				None
		#
		#	Algorithm:
			 copy_expert(sql, file, size=8192)
			 sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
		#	alternative: copy_from(file, table, sep='\t', null='\\N', size=8192, columns=None)
		#
		# 	Restrictions/Notes:
		# 		None
		#
		# 	The follow methods are called:
		# 	Author: Liming Gao
		# 	Date: 02/05/2020
		#
		================================================================================
	'''

	def insert_CSVfile(self, table, fields, file_name):

		# Create a new cursor for the query
		cur = self.conn.cursor()

		query =  table
		query += " (" + ", ".join(fields) + ") "

		q = "COPY {0} FROM STDIN WITH CSV".format(query)  #input file qithout header 
		# COPY table FROM STDIN WITH CSV  table(f1,f2,f3)
		# print ("Hello, I am {0} years {1} old {0}!".format(18,12)) 
		# {} contains either the numeric index of a positional argument, or the name of a keyword argument
		with open(file_name, 'r') as f:

			cur.copy_expert(sql = q, file = f)
			self.conn.commit()

	'''
		============================= Method bulk_insert() ====================================
		#	Method Purpose:
		#		insert the data in a csv file stored in disk into table
		#
		#	Input Variable:
		#		self, table, fields, file_name
		# 
		#	Output/Return:
				None
		#
		#	Algorithm:
			 copy_expert(sql, file, size=8192)
			COPY %s FROM STDIN WITH
                    CSV
                    HEADER
                    DELIMITER AS ','
		#	alternative: copy_from(file, table, sep='\t', null='\\N', size=8192, columns=None)
						 execute_many()
			reference: 
			1. https://www.psycopg.org/docs/cursor.html#cursor.copy_from
			2. https://hackersandslackers.com/psycopg2-postgres-python-the-old-fashioned-way/
		# 	Restrictions/Notes:
		# 		None
		#
		# 	The follow methods are called:
		# 	Author: Liming Gao
		# 	Date: 02/05/2020
		#
		================================================================================
	'''

	def bulk_insert(self, table, fields, file_name):

		# Create a new cursor for the query
		cur = self.conn.cursor()

		query =  table
		query += " (" + ", ".join(fields) + ") "

		q = "COPY {0} FROM STDIN WITH CSV".format(query)

		with open(file_name, 'r') as f:
			print(f.readline())
			try:
				cur.copy_expert(sql = q, file = f)
				self.conn.commit()
			except psycopg2.Error as e:
				print('Bulk inert error:',e) 
	'''
		============================= Method bulk_insertCopyfrom() ====================================
		#	Method Purpose:
		#		insert the data in a csv file stored in disk into table, similar with copy_expert()
		#
		#	Input Variable:
		#		self, table, fields, file_name
		#
		#	Output/Return:
				None
		#
		#	Algorithm:
		#	copy_from(file, table, sep='\t', null='\\N', size=8192, columns=None)
						 execute_many()
			reference: 
			1. https://www.psycopg.org/docs/cursor.html#cursor.copy_from
			2. https://hackersandslackers.com/psycopg2-postgres-python-the-old-fashioned-way/
		# 	Restrictions/Notes:
		# 		None
		#
		# 	The follow methods are called:
		# 	Author: Liming Gao
		# 	Date: 02/05/2020
		#
		================================================================================
	'''
	def bulk_insertCopyfrom(self, table, fields, file_name):

		# Create a new cursor for the query
		cur = self.conn.cursor()

		with open(file_name, 'r') as f:
			try:
				cur.copy_from(f, table,sep=',', columns=fields)
				self.conn.commit()
			except psycopg2.Error as e:
				print('Copyfrom Bulk inert error:',e)


	'''
	============================= Method bulk_insert_Copyfrom_MemoryIO() ====================================
		#	Method Purpose:
		#		insert the data in a file-like object(a file in memory rather than in disk) into table
		#
		#	Input Variable:
		#		self, table, fields, FileObject_name
		#
		#	Output/Return:
				None
		#
		#	Algorithm:
		#	copy_from(file, table, sep='\t', null='\\N', size=8192, columns=None)
						 execute_many()
			reference: 
			1. https://www.psycopg.org/docs/cursor.html#cursor.copy_from
			2. https://hackersandslackers.com/psycopg2-postgres-python-the-old-fashioned-way/
		# 	Restrictions/Notes:
		# 		None
		#
		# 	The follow methods are called:
		# 	Author: Liming Gao
		# 	Date: 02/05/2020
		#
		================================================================================
	'''
	def bulk_insert_Copyfrom_MemoryIO(self,FileObject_name,table, fields,size=8192):

		# Create a new cursor for the query
		cur = self.conn.cursor()
		try:
			cur.copy_from(FileObject_name,table,sep=',', columns=fields,size=size)
			#self.conn.commit()
		except psycopg2.Error as e:
			print('Copyfrom Bulk inert error:',e) 

	'''
		============================= Method insert_execute_values_iterator() ====================================
		#	Method Purpose:
		#		insert the many rows into table
		#
		#	Input Variable:
		#		self,
		# 		table,
		# 		fields,
				array_tuple, not nescessary to be a list with tuple element. Two diemensional array works.
		#
		#	Output/Return:
				None
		#
		#	Algorithm:
			  psycopg2.extras.execute_values(cur, sql, argslist, template=None, page_size=1000, fetch=False)
			reference: 
			1. https://www.psycopg.org/docs/extras.html#psycopg2.extras.execute_values
			2. https://hakibenita.com/fast-load-data-python-postgresql#execute-values
		# 	Restrictions/Notes:
		# 		for speed, it does not commit the insert soon 
		#
		# 	The follow methods are called:
		# 	Author: Liming Gao
		# 	Date: 03/05/2020
		#
		================================================================================
	'''

	def insert_execute_values_iterator(self,table, fields,array_tuple,page_size = 1000,commitNow = False):
		# with self.conn.cursor() as cur:
			query =  table
			query += "(" + ", ".join(fields) + ") "
			sql = "INSERT INTO {0} VALUES %s".format(query)
			#psycopg2.extras.execute_values(cur, sql, () for beer in beers), page_size=page_size)
			#cur = self.conn.cursor()
			try:
				#print(array for array in array_tuple)
				execute_values(self.cur,sql,array_tuple,page_size=1000)
				#execute_values(cur,sql,(array for array in array_tuple),page_size=1000)
				#if commitNow == True:
				#	self.conn.commit() #

			except psycopg2.Error as e:
				self.conn.rollback()
				print('execute_values inert error:',e)

	
	'''
		============================= Method update() ====================================
		#	Method Purpose:
		#		update the data in a table
		#
		#	Input Variable: 
		#		self, table, update_set, where,returning
		#	Output/Return: 
				None
				Status
		#
		#	Algorithm:
				UPDATE table 
				SET update_set1, update_set2
				WHERE where1 
					AND where2
				RETRUNING returning1, returning2
		#
		# 	Restrictions/Notes:
		# 		None
		#
		# 	The follow methods are called:
				self.query()
		# 	Author: Liming Gao
		# 	Date: 02/05/2020
		#
		================================================================================
	'''

	def update(self, table, update_set, where = None, returning = None):

		query =  "UPDATE " + table
		query += " SET " + ", ".join(update_set)

		if where is not None:

			query += " WHERE " + " AND ".join(where)

		if returning is not None:

			query += " RETURNING " + ", ".join(returning)
		query += ";"
		cur, status = self.query(query = query, values = None, message = 'Unable to execute UPDATE query')
		rows_updated = cur.rowcount
		print("{} rows are updated.".format(rows_updated))
		self.conn.commit()

		return None, status

	'''
		============================= Method delete() =================================
		#	Method Purpose:
		#		delete the rows in a table
		#
		#	Input Variable:
		#		self, table, using, where
		#	Output/Return:
				Status
		#
		#	Algorithm:
				DELETE FROM table
				USING another_table
				WHERE where1
					AND where2
		    		AND	table.id = another_table.id;
		#
		# 	Restrictions/Notes:
		# 		None
		#
		# 	The follow methods are called:
				self.query()
		# 	Author: Liming Gao
		# 	Date: 02/05/2020
		#
		================================================================================
	'''

	def delete(self, table, using, where):

		query =  "DELETE FROM " + table

		if using is not None:

			query += " USING " + using

		query += " WHERE " + " AND ".join(where)

		query += ";"

		cur, status = self.query(query = query, values = None, message = 'Unable to execute DELETE query')
		rows_deleted = cur.rowcount
		print("{} rows are deleted.".format(rows_updated))
		self.conn.commit()

		return status

	'''
		============================= Method query() ====================================
		#	Method Purpose:
		#		query is used to get the the cursor and execute the SQL statement
		#
		#	Input Variable:
		#		self.conn 		the instance of a connection class
		#		query 			the SQL statement
		#		vlaue 			the values the SQL statement need to handle
		#		message 		The error message
		#
		#	Output/Return:
		#		cur 			cursor object
		#		status 			the execution status of the SQL statement
		#
		#	Algorithm:
		#		1. use cur.execute(query,(value1,value2)) command
		#		2. print the error message if there is a error.
		#
		# 	Restrictions/Notes:
		# 		None
		#
		# 	The follow methods are called:
		# 		printErrors()
		#
		# 	Author: Liming Gao
		# 	Date: 02/05/2020
		#
		================================================================================
	'''
	def query(self, query, values = None, message = None):

		try:
			# Create a new cursor for the query
			cur = self.conn.cursor()

			# Print PostgreSQL Connection properties
		# print (self.conn.get_dsn_parameters(),"\n")
			print('new cursor is created successfully')

		except (Exception,psycopg2.Error) as error:

			print ("Error while connecting to PostgreSQL", error)

		# Attempt to execute the query
		try:

			# If we are just performing a select query, we won't have any values to submit to the database
			if values is None:

				cur.execute(query)

			# Otherwise, we are performing an insert/update/delete query and need to pass in the values with the query
			else:

				cur.execute(query,values)

		# If for whatever reason we have caught an error, let's display that to the user
		except psycopg2.Error as e:

			if message is None:

				self.printErrors(error = e)

			else:

				self.printErrors(error = e, message = message + ": " + query)

			return cur, False

		return cur, True

	'''close the communication with the PostgreSQL database server by calling the close()method of the cursor and connection objects.
			cur.close()
	'''
	def close_DB_connection(self):
		self.cur.close()
		self.conn.close()

	def connection_commit(self):
		self.conn.commit()

	# printErrors() Method is used to 
	def printErrors(self,error,message = None): 

		if message is not None:

			print(message)

		print(error)
		print(error.pgcode)
		print(error.pgerror)
		print(traceback.format_exc())