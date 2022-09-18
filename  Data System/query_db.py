#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 27 10:35:14 2022

@author: anthony
"""

import psycopg2

class queryData:
    
    def __init__(self):
        self.connection = None
        
        try:
            self.connection = psycopg2.connect(
                user='admin',
                password='quest',
                host='127.0.0.1',
                port='8812',
                database='qdb')
        
        except (Exception, psycopg2.Error) as error:
            print("Error while fetching data from PostgreSQL", error)
         
        self.cursor = self.connection.cursor() 
        
            
    def end_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("PostgreSQL connection is closed")
        
    def query_entire_table(self, table_name):
        print('Selecting all rows from ' + table_name)
        self.cursor.execute('SELECT * FROM ' + table_name + ';')
        query_results = self.cursor.fetchall()
        
        return query_results
    
    def query_last_date(self, table_name):
        print('Selecting last date from ' + table_name)
        self.cursor.execute('SELECT * FROM '+ table_name +' LIMIT -1')
        query_results = self.cursor.fetchall()
        
        return query_results
    
    def custom_query(self,sql_command):
        print('Selecting rows from test table using cursor.fetchall')
        self.cursor.execute(sql_command)
        query_results = self.cursor.fetchall()
        return query_results