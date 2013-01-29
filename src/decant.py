#  Membertools, a basic set of tools for administering lists, by Martin Keegan

#  Copyright (C) 2013  Martin Keegan
#
#  This programme is free software; you may redistribute and/or modify
#  it under the terms of the Apache Software Licence v2.0

import oursql
import sys

def _transfer_entries(upstream, downstream, 
                     src_tbl, dst_tbl, 
                     src_cols, dst_cols, 
                     src_key_cols, dst_key_cols):

    def decant():
        assert len(src_cols) == len(dst_cols)
        for col in src_key_cols:
            if col not in src_key_cols:
                print "Source key column '%s' not mentioned in mapping" % col
                sys.exit(1)
        for col in dst_key_cols:
            if col not in dst_key_cols:
                print "Destination key column '%s' not mentioned in mapping" % col
                sys.exit(1)

        src_fields = ", ".join(src_cols)
        dst_fields = ", ".join(dst_cols)

        # gather rows from upstream
        sql = '''select %s from %s''' % (src_fields, src_tbl)

        with upstream.cursor(oursql.DictCursor) as c:
            c.execute(sql)
            rows = c.fetchall()

        # get the key column from downstream
        dst_key_col_names = ", ".join(dst_key_cols)
        sql = '''select %s from %s''' % (dst_key_col_names, dst_tbl)

        c = downstream.cursor()
        c.execute(sql)
        dst_keys = [ i for i in c.fetchall() ]

        # remove rows which are already present downstream
        def slice(row, columns):
            return tuple([ row[c] for c in columns ])
        new_rows = filter(lambda x: slice(x, src_key_cols) not in dst_keys, rows)

        dst_placeholders = ", ".join(["%s"] * len(dst_cols))
        sql = '''insert into %s (%s) values (%s);''' % (dst_tbl, 
                                                        dst_fields, 
                                                        dst_placeholders)
        lookup = dict(zip(dst_cols, src_cols))

        unique_keys = {}
        # insert remaining rows into downstream
        for row in new_rows:
            # hack on a stick
            def lookup_col(col_id):
                delim = " as "
                force_bool = False
                col_id = col_id.lower()
                if delim in col_id:
                    parts = col_id.split(delim)
                    col_id = parts[-1]
                    force_bool = True
                val = row[col_id]
                if isinstance(val, unicode):
                    val = val.encode('raw_unicode_escape').decode('utf8')
                if not force_bool: return val
                if val in [0, 1]: return bool(val)
                return val
            dst_values = map(lookup_col, src_cols)

            key = slice(row, [ lookup[dkc] for dkc in dst_key_cols ])
            if key in unique_keys:
                continue # aaaargh
            unique_keys[key] = True

            c = downstream.cursor()
            rv = c.execute(sql, dst_values)
        downstream.commit()

    decant()

def transfer_entries(upstream, downstream, 
                     src_tbl, dst_tbl, 
                     column_mapping,
                     src_key_cols, dst_key_cols):

    pairs = [ (k, v) for k, v in column_mapping.iteritems() ]
    src_cols = [ i[0] for i in pairs ]
    dst_cols = [ i[1] for i in pairs ]

    return _transfer_entries(upstream, downstream, 
                     src_tbl, dst_tbl, 
                     src_cols, dst_cols, 
                     src_key_cols, dst_key_cols)
