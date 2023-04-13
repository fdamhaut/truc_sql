#!/usr/bin/python3

import psycopg2
import psycopg2.extras
import sys
from collections import defaultdict


conn = psycopg2.connect("dbname=trunk")
cr = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

last_code = None

def show_row(log):
    print('%-8d %s    %-15s %10.2f %10.2f'% (log['id'], log['create_date'], log['event_type'], log['amount_signed'] or 0, log['recurring_monthly']))

def show_table(logs, error='All right'):
    global last_code
    if (last_code is None) or (last_code != logs[0]['subscription_code']):
        last_code = logs[0]['subscription_code']
        print() # new line at each contract
    print('Order %-8s %-16s %s %s:'% (logs[0]['order_id'], logs[0]['subscription_code'], logs[0]['currency_id'], error))
    for log in logs:
        show_row(log)


orders = defaultdict(list)
code = defaultdict(list)

# ('M21043025842286','M22011035087550', 'M22070141784236', 'M21101831769180', 'M21043025842286', 'M1701132758517')
codes = ('M21043025842286','M22011035087550', 'M22070141784236', 'M21101831769180', 'M21043025842286', 'M1701132758517')

# After log_1.sql


cr.execute('''
    select * 
    from sale_order_log 
    where subscription_code in %s 
    order by subscription_code, id''', 
(codes,))

logs = cr.fetchall()

prec_order = {}
for log in logs:
    order_id = log['order_id']
    if not len(orders[order_id]) and len(code[log['subscription_code']]):
        prec_order[order_id] = code[log['subscription_code']][-1]
    orders[order_id].append(log)
    code[log['subscription_code']].append(order_id)


for order_id,logs in orders.items():

    # If there is a transfer and expansion in same transaction: we might need to merge them
    for i in range(1, len(logs)-1):
        if logs[i]['create_date'] == logs[i+1]['create_date'] and \
            logs[i]['event_type'] == '3_transfer' and \
            logs[i+1]['event_type'] in ('1_expansion', '15_contraction') and \
            logs[i+1]['recurring_monthly'] != (logs[i]['recurring_monthly'] + logs[i+1]['amount_signed']):

            cr.execute('''update sale_order_log
                   set id = case id
                                 when %s then %s
                                 when %s then %s
                                  end
                    where id in %s
                ''', (logs[i]['id'], logs[i+1]['id'], logs[i+1]['id'], logs[i]['id'], (logs[i]['id'], logs[i+1]['id'])))
            (logs[i]['id'], logs[i+1]['id']) = (logs[i+1]['id'], logs[i]['id'])

            logs[i], logs[i+1] = logs[i+1], logs[i]

            print('Transfer and expansion to switch %s' % (logs[i]['id']))

    # Try to match transfers of new orders
    before = prec_order.get(logs[0]['order_id'], None)
    if before and orders[before][-1]['event_type'] != '3_transfer':
        # last line before is not a transfer
        logs0 = orders[before]

        for i in range(len(logs0)):
            if logs0[i]['id'] > logs[0]['id']:
                cr.execute('select sum(new_enterprise_user) from sale_order_log where id > %s and order_id = %s', (logs0[i]['id'], before))
                ent_user = cr.fetchall()[0]
                cr.execute('delete from sale_order_log where id > %s and order_id = %s', (logs0[i]['id'], before))
                while len(logs0) > i+1:
                    del logs0[i+1]
                break

        # Add condition to avoid useless execute
        logs0[i]['create_date'] = logs[0]['create_date']
        logs0[i]['amount_signed'] = -logs0[i-1]['recurring_monthly']
        logs0[i]['recurring_monthly'] = 0.00
        logs0[i]['new_enterprise_user'] = logs0[i]['new_enterprise_user'] + ent_user
        logs0[i]['event_type'] = '3_transfer'

        cr.execute('''
            update sale_order_log_bcp 
            set amount_signed=%s, 
                create_date=%s, 
                recurring_monthly=%s, 
                event_type=%s ,
                new_enterprise_user=%s
            where id=%s'''
            , (-logs0[i-1]['recurring_monthly'], logs0[i]['create_date'], 0.00, '3_transfer', logs0[i]['new_enterprise_user'], logs0[i]['id']))
        print('forcing last transfer')


    if before and orders[before][-1]['event_type'] == '3_transfer':
        if (len(logs)>1) and (logs[0]['event_type'] == '3_transfer') and (logs[0]['create_date'] == logs[1]['create_date']):
            value = orders[before][-1]['amount_signed']
            diff = logs[0]['recurring_monthly'] + value
            logs[0]['amount_signed'] = logs[0]['recurring_monthly'] = -value
            logs[1]['amount_signed'] += diff
            event_type = logs[1]['amount_signed'] > 0 and '1_expansion' or '15_contraction'
            logs[1]['event_type'] = event_type
            cr.execute('update sale_order_log_bcp set amount_signed=%s, recurring_monthly=%s, event_type=%s where id=%s', (-value, -value, event_type, logs[0]['id']))
            cr.execute('update sale_order_log_bcp set amount_signed=amount_signed+%s where id=%s', (diff, logs[1]['id']))
            print('fixing new transfer')
        else:
            # show_table(orders[before])
            # show_table(logs)
            print('Something to implement: insert an expansion, that will be ixed afrer')
            # raise "Do something..."

for order_id,logs in orders.items():
    show_table(logs)



conn.rollback()