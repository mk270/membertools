#  Membertools, a basic set of tools for administering lists, by Martin Keegan

#  Copyright (C) 2013  Martin Keegan
#
#  This programme is free software; you may redistribute and/or modify
#  it under the terms of the Apache Software Licence v2.0

import oursql

def _transfer_entries(upstream, downstream, 
                     src_tbl, dst_tbl, 
                     src_cols, dst_cols, 
                     src_key_col, dst_key_col):

    def decant():
        assert len(src_cols) == len(dst_cols)
        assert src_key_col in src_cols
        assert dst_key_col in dst_cols

        src_fields = ", ".join(src_cols)
        dst_fields = ", ".join(dst_cols)

        # gather rows from upstream
        sql = '''select %s from %s''' % (src_fields, src_tbl)

        with upstream.cursor(oursql.DictCursor) as c:
            c.execute(sql)
            rows = c.fetchall()

        # get the key column from downstream
        sql = '''select %s from %s''' % (dst_key_col, dst_tbl)

        c = downstream.cursor()
        c.execute(sql)
        dst_keys = [ i[0] for i in c.fetchall() ]

        # remove rows which are already present downstream
        new_rows = filter(lambda x: x[src_key_col] not in dst_keys, rows)

        dst_placeholders = ", ".join(["%s"] * len(dst_cols))
        sql = '''insert into %s (%s) values (%s);''' % (dst_tbl, 
                                                        dst_fields, 
                                                        dst_placeholders)
        lookup = dict(zip(src_cols, dst_cols))

        #print "LOOKUP", lookup
        if len(new_rows) > 1:
            pass # print "NEW_ROWS[0]", new_rows[0]
        #print "SRC_COLS", src_cols

        # insert remaining rows into downstream
        for row in new_rows:
            #print "ROW", row
            dst_values = map(lambda i: row[i], src_cols)
            c = downstream.cursor()
            rv = c.execute(sql, dst_values)
        downstream.commit()

    decant()

def transfer_entries(upstream, downstream, 
                     src_tbl, dst_tbl, 
                     column_mapping,
                     src_key_col, dst_key_col):

    pairs = [ (k, v) for k, v in column_mapping.iteritems() ]
    src_cols = [ i[0] for i in pairs ]
    dst_cols = [ i[1] for i in pairs ]

    return _transfer_entries(upstream, downstream, 
                     src_tbl, dst_tbl, 
                     src_cols, dst_cols, 
                     src_key_col, dst_key_col)
