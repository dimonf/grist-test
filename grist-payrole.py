#!/usr/bin/env python3
from grist_api import GristDocAPI
import os
from datetime import datetime as dt

SERVER = "https://tbl.paradoc.net"
DOC_ID = "wQZx3NfSt859ZpZpaCHvfe"

# Get api key from your Profile Settings, and run with GRIST_API_KEY=<key>
api = GristDocAPI(DOC_ID, server=SERVER)

# add some rows to a table
rows = api.add_records('Transactions', [
    {'timestamp': dt.now(), 'session': 1, 'amount': 25},
])

# fetch all the rows
data = api.fetch_table('Transactions')
print(data)
