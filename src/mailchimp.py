#!/usr/bin/env python

#  Membertools, a basic set of tools for administering lists, by Martin Keegan

#  Copyright (C) 2013  Martin Keegan
#
#  This programme is free software; you may redistribute and/or modify
#  it under the terms of the Apache Software Licence v2.0

import sys
import itertools
from postmonkey import PostMonkey
from batch_generator import batch_generator
from mock_data import make_fake_member

def mailchimp_dict_of_tuple(details):
    # emailaddress, firstname, lastname -> dictionary
    tmp = {
        'EMAIL': details[0],
        'EMAIL_TYPE': 'html'
        }

    if details[1] is not None:
        tmp['FNAME'] = details[1]
    if details[2] is not None:
        tmp['LNAME'] = details[2]
    return tmp

def address_of_member(details): return details[0]

def batch_subscribe(pm, list_id, members):
    return pm.listBatchSubscribe(id=list_id, 
                          batch=members, 
                          double_optin=False, 
                          update_existing=True)

def batch_unsubscribe(pm, list_id, email_addresses):
    return pm.listBatchUnsubscribe(id=list_id,
                                   emails=email_addresses,
                                   delete_member=True,
                                   send_goodbye=False,
                                   send_notify=False)

# conks out at 15000 - need to use Export API to work around
# this
def get_subscriber_addresses(pm, list_id):
    return [ i["email"] for i in pm.listMembers(id=list_id, 
                                                status="subscribed", 
                                                limit=15000)["data"] ]

def get_list_by_name(pm, list_name):
    lists = pm.lists(filters={'list_name':list_name})
    assert lists['total'] == 1
    return lists['data'][0]['id']

def update_list(api_key, list_name, batch_size, list_source, verbose=False):
    pm = PostMonkey(api_key, timeout=20)

    list_id = get_list_by_name(pm, list_name)

    email_addresses_present = []
    email_addresses_desired = []

    email_addresses_present = get_subscriber_addresses(pm, list_id)

    member_source = itertools.imap(mailchimp_dict_of_tuple, list_source)
    member_stream = batch_generator(batch_size, member_source)

    for batch in member_stream:
        email_addresses_desired += [ i["EMAIL"] for i in batch ]
        batch = filter(lambda i: i["EMAIL"] not in email_addresses_present, batch)
        results = batch_subscribe(pm, list_id, batch)
        if verbose:
            print results

    def email_addresses_to_remove():
        for address in filter(lambda i: i not in email_addresses_desired, 
                              email_addresses_present):
            yield address

    removal_stream = batch_generator(batch_size, email_addresses_to_remove())

    for batch in removal_stream:
        results = batch_unsubscribe(pm, list_id, batch)
        if verbose:
            print results
