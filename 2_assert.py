#!/usr/bin/python3

import psycopg2
import psycopg2.extras
import sys
from collections import defaultdict

conn = psycopg2.connect("dbname=openerp")
cr = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

last_code = None

def show_row(log):
    print('%-8d %s    %-15s %10.2f %10.2f'% (log['id'], log['create_date'], log['event_type'], log['amount_signed'] or 0, log['recurring_monthly']))

def show_table(logs, error='All right'):
    global last_code
    if (last_code is None) or (last_code != logs[0]['ooid']):
        last_code = logs[0]['ooid']
        print() # new line at each contract
    print('Order %-8s %-16s %s %s:'% (logs[0]['order_id'], logs[0]['ooid'], logs[0]['currency_id'], error))
    for log in logs:
        show_row(log)


orders = defaultdict(list)
code = defaultdict(list)

print('SQL Queries')
cr.execute('''
    select *, coalesce(origin_order_id, order_id) as ooid
    from sale_order_log 
    order by coalesce(origin_order_id, order_id), id''')

logs = cr.fetchall()

cr.execute(''' select id, state, subscription_state from sale_order order by id''')
so = {x['id']: x for x in cr.fetchall()}

print('Reorder')
prec_order = {}
for log in logs:
    order_id = log['order_id']
    if not len(orders[order_id]) and len(code[log['ooid']]):
        prec_order[order_id] = code[log['ooid']][-1]
    orders[order_id].append(log)
    code[log['ooid']].append(order_id)


print('Check Orders')
nbr = 0
for order_id,logs in orders.items():
    print(nbr, ' / ', len(orders)-1)
    nbr += 1

    if so[logs[0]['order_id']]['subscription_state'] == '7_upsell':
        cr.execute('delete from sale_order_log where order_id=%s', (logs[0]['order_id'],))
        del logs[:]
        continue

    before = prec_order.get(logs[0]['order_id'], None)
    if ((len(logs)==1) or logs[0]['event_type'] not in ('0_creation', '3_transfer')) and logs[0]['amount_signed']==0 and logs[0]['recurring_monthly']==0:
        cr.execute('delete from sale_order_log where id=%s', (logs[0]['id'],))
        del logs[0]
        continue

    if logs[0]['event_type'] not in ('0_creation','3_transfer'):
        if len(logs)>1 and logs[1]['event_type'] == '0_creation':
            cr.execute('delete from sale_order_log where id=%s', (logs[0]['id'],))
            del logs[0]
        else:
            show_table(logs, 'Beginning failed')
            sys.exit(1)

    if so[logs[-1]['order_id']]['subscription_state'] not in ('3_progress', '4_paused', None) and logs[-1]['event_type'] not in ('2_churn','3_transfer'):
        if logs[-1]['recurring_monthly'] == 0:
            logs[-1]['event_type'] = '2_churn'
            cr.execute('update sale_order_log set event_type=%s where id=%s', ('2_churn', logs[-1]['id']))
        elif so[logs[-1]['order_id']]['subscription_state'] == '5_renewed':
            logs[-1]['event_type'] = '3_transfer'
            logs[-1]['amount_signed'] = logs[-1]['amount_signed'] - logs[-1]['recurring_monthly']
            logs[-1]['recurring_monthly'] = 0.0
            cr.execute('update sale_order_log set event_type=%s, amount_signed=%s, recurring_monthly=0.0 where id=%s', (logs[-1]['event_type'], logs[-1]['amount_signed'], logs[-1]['id']))
        else:
            show_table(logs, 'End failed')
            sys.exit(1)

conn.rollback()
