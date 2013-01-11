
#  Membertools, a basic set of tools for administering lists, by Martin Keegan

#  Copyright (C) 2013  Martin Keegan
#
#  This programme is free software; you may redistribute and/or modify
#  it under the terms of the Apache Software Licence v2.0

import psycopg2

DB_NAME = 'rewired_state'

def db_handle():
    db = psycopg2.connect(database=DB_NAME)
    return db

def query(sql):
    db = db_handle()

    c = db.cursor(name="mycursor")
    c.itersize = 2000
    c.execute(sql)
    for row in c:
        yield row
                        
