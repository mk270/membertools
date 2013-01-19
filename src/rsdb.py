
#  Membertools, a basic set of tools for administering lists, by Martin Keegan

#  Copyright (C) 2013  Martin Keegan
#
#  This programme is free software; you may redistribute and/or modify
#  it under the terms of the Apache Software Licence v2.0

import psycopg2

def db_handle(database_name):
    db = psycopg2.connect(database=database_name)
    return db

def query(database_name, sql):
    db = db_handle(database_name)

    c = db.cursor(name="mycursor")
    c.itersize = 2000
    c.execute(sql)
    for row in c:
        yield row
                        
