#  Membertools, a basic set of tools for administering lists, by Martin Keegan

#  Copyright (C) 2013  Martin Keegan
#
#  This programme is free software; you may redistribute and/or modify
#  it under the terms of the Apache Software Licence v2.0

import itertools

def make_fake_member():
    counter = itertools.count(22)

    limited_counter = itertools.takewhile(lambda i: i < 41, counter)

    for i in limited_counter:
        yield (
            'mk270-%d@no.ucant.org' % i,
            'User %d' % i,
            'Keegan',
            )
