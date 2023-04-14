#!/usr/bin/python3

import psycopg2
import psycopg2.extras
import sys
from collections import defaultdict


conn = psycopg2.connect("dbname=openerp port=5431")
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

# ('M21043025842286','M22011035087550', 'M22070141784236', 'M21101831769180', 'M21043025842286', 'M1701132758517')
codes = ('M21043025842286','M22011035087550', 'M22070141784236', 'M21101831769180', 'M21043025842286', 'M1701132758517')

# After log_1.sql


cr.execute('''
    select *, coalesce(origin_order_id, order_id) as ooid
    from sale_order_log 
    order by coalesce(origin_order_id, order_id), id''')

logs = cr.fetchall()

prec_order = {}
for log in logs:
    order_id = log['order_id']
    if not len(orders[order_id]) and len(code[log['ooid']]):
        prec_order[order_id] = code[log['ooid']][-1]
    orders[order_id].append(log)
    code[log['ooid']].append(order_id)


for order_id,logs in orders.items():

    # If there is a transfer and expansion in same transaction: we might need to merge them
    for i in range(1, len(logs)-1):
        if logs[i]['create_date'] == logs[i+1]['create_date'] and \
            logs[i]['event_type'] == '3_transfer' and \
            logs[i]['recurring_monthly'] == 0 and\
            logs[i+1]['event_type'] in ('1_expansion', '15_contraction') and \
            logs[i+1]['recurring_monthly'] != (logs[i]['recurring_monthly'] + logs[i+1]['amount_signed']):

            cr.execute('''update sale_order_log
                set id = %s
                where id = %s
                ''', (-1, logs[i]['id']))
            cr.execute('''update sale_order_log
                set id = %s
                where id = %s
                ''', (logs[i]['id'], logs[i+1]['id']))
            cr.execute('''update sale_order_log
                set id = %s
                where id = %s
                ''', (logs[i+1]['id'], -1))
            
            logs[i]['id'], logs[i+1]['id'] = logs[i+1]['id'], logs[i]['id']
            logs[i], logs[i+1] = logs[i+1], logs[i]

            print('Transfer and expansion to switch %s' % (logs[i]['id']))

    # Try to match transfers of new orders
    before = prec_order.get(logs[0]['order_id'], None)
    if before:
        # last line before is not a transfer
        logs0 = orders[before]

        ent_user = 0
        for i in range(len(logs0)):
            if logs0[i]['id'] > logs[0]['id']:
                cr.execute('select sum(coalesce(new_enterprise_user, 0)) as sum from sale_order_log where id > %s and order_id = %s', (logs0[i]['id'], before))
                ent_user = cr.fetchall()[0]['sum'] or 0
                cr.execute('delete from sale_order_log where id > %s and order_id = %s', (logs[0]['id'], before))
                del logs0[i:]
                break
        i = i-1

        # Add condition to avoid useless execute
        logs0[i]['create_date'] = logs[0]['create_date']
        logs0[i]['amount_signed'] = -logs0[i-1]['recurring_monthly'] if i > 0 else 0
        logs0[i]['recurring_monthly'] = 0.00
        logs0[i]['new_enterprise_user'] = (logs0[i]['new_enterprise_user'] or 0) + ent_user
        logs0[i]['event_type'] = '2_churn' if logs[0][0]['event_type']=='0_creation' else '3_transfer'

        cr.execute('''
            UPDATE sale_order_log 
            SET amount_signed = %s, 
                create_date = %s, 
                recurring_monthly = %s, 
                event_type = %s,
                new_enterprise_user = %s
            WHERE id = %s'''
            , (logs0[i]['amount_signed'], 
                logs0[i]['create_date'], 
                0.00, 
                logs0[i]['event_type'],
                logs0[i]['new_enterprise_user'], 
                logs0[i]['id'])
            )

        # Reconcile Transfer
        if len(logs) > 1 and logs[0]['event_type'] == '3_transfer' and\
            logs[0]['currency_id'] == orders[before][-1]['currency_id']:

            old_mrr = -orders[before][-1]['amount_signed']
            diff = logs[0]['recurring_monthly'] - old_mrr
            logs[0]['amount_signed'] = logs[0]['recurring_monthly'] = old_mrr
            logs[1]['amount_signed'] = logs[1]['recurring_monthly'] - old_mrr
            logs[1]['event_type'] = '1_expansion' if logs[1]['amount_signed'] >= 0 else '15_contraction'
            orders[before][-1]['event_date'] = logs[0]['event_date']

            cr.execute('''
                UPDATE sale_order_log 
                    set amount_signed = %s, 
                        recurring_monthly = %s
                    where id = %s''', 
                (old_mrr, old_mrr, logs[0]['id']))
            cr.execute('''
                UPDATE sale_order_log 
                    set amount_signed = %s,
                        event_type = %s
                    where id = %s''', 
                (logs[1]['amount_signed'], logs[1]['event_type'], logs[1]['id']))
            cr.execute('''
                UPDATE sale_order_log 
                    set event_date = %s
                    where id = %s''', 
                (logs[0]['event_date'], orders[before][-1]['id']))

            print('fixing new transfer')

        elif len(logs) and logs[0]['event_type'] == '3_transfer' and\
            logs[0]['currency_id'] == orders[before][-1]['currency_id']:

            old_mrr = - orders[before][-1]['amount_signed']
            new_mrr = logs[0]['recurring_monthly']

            cr.execute('''UPDATE sale_order_log 
                    set amount_signed = %s, 
                        recurring_monthly = %s
                    where id = %s''', 
                (old_mrr, old_mrr, logs[0]['id']))
            orders[before][-1]['event_date'] = logs[0]['event_date']
            cr.execute('''
                UPDATE sale_order_log 
                    set event_date = %s
                    where id = %s''', 
                (logs[0]['event_date'], orders[before][-1]['id']))

            if new_mrr != old_mrr:
                # TODO insert MRR Change
                l = logs[0]
                cr.execute('''
                INSERT INTO sale_order_log(
                    order_id,
                    origin_order_id,
                    subscription_code,
                    event_date,
                    currency_id,
                    subscription_state,
                    recurring_monthly,
                    amount_signed,
                    amount_expansion,
                    amount_contraction,
                    event_type
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (l['order_id'], l['origin_order_id'], l['subscription_code'], l['event_date'],
                 l['currency_id'], '5_renewed', new_mrr, new_mrr - old_mrr, 0, new_mrr - old_mrr, '1_expansion'))


# for order_id,logs in orders.items():
#     show_table(logs)


conn.commit()
