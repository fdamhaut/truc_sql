#!/usr/bin/python3

import psycopg2
import psycopg2.extras
import sys
from collections import defaultdict

to_show = 2231129

conn = psycopg2.connect("dbname=openerp port=5431")
cr = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

last_code = None

def show_row(log):
    print('%-8d %s %s   %-15s %10.2f %10.2f'% 
        (log['id'], log['create_date'], log['event_date'], log['event_type'], log['amount_signed'] or 0, log['recurring_monthly']))

def show_table(logs, error='All right'):
    print('Order %-8s %-16s %s %s:'% (logs[0]['order_id'], logs[0]['ooid'], logs[0]['currency_id'], error))
    for log in logs:
        show_row(log)

def show_all():
    for order, logs in orders.items():
        if logs and logs[0]['ooid'] == to_show:
            show_table(logs)
    print()

def show(origin, msg):
    if origin == 0 or orders[origin] and orders[origin][0]['ooid'] == to_show:
        print(msg)
        show_all()

# After pre.sql

orders = defaultdict(list)
has_next = set()
prec_order = {}

cr.execute('''
    SELECT *, coalesce(origin_order_id, order_id) as ooid
    FROM sale_order_log
    ORDER BY coalesce(origin_order_id, order_id), order_id, create_date, id
    ''')

logs = cr.fetchall()


for log in logs:
    orders[log['order_id']].append(log)

cr.execute('''
    SELECT id, subscription_id
    FROM sale_order
    WHERE subscription_id IS NOT NULL
    AND subscription_state IN ('3_progress', '4_paused', '5_renewed', '6_churn')
    ''')

sos = cr.fetchall()
for so in sos:
    has_next.add(so['subscription_id'])
    prec_order[so['id']] = so['subscription_id']


show(0, 'begin')

# Fix Transfer
for order_id, logs in [(o, l) for o, l in orders.items()]:

    if not logs:
        continue

    if logs and logs[0]['ooid'] == to_show:
        print(order_id)

    show(order_id, 'b')

    # Try to match transfers of new orders
    before = prec_order.get(logs[0]['order_id'], None)
    if before and orders[before]:
        # last line before is not a transfer
        logs0 = orders[before]

        # If the first event is not a 'beginning' event
        if logs[0]['event_type'] not in ('3_transfer', '0_creation'):
            l = logs[0]
            event_type = '0_creation' if logs0[-1]['event_type'] == '2_churn' else '3_transfer'
            mrr = -logs0[-1]['amount_signed']
            cr.execute('''
                INSERT INTO sale_order_log(
                    company_id,
                    user_id,
                    team_id,
                    create_date,
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
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id''',
                (l['company_id'], l['user_id'], l['team_id'], l['create_date'], l['order_id'], l['ooid'], 
                l['subscription_code'], l['event_date'], l['currency_id'], '3_progress', mrr, mrr, 0, mrr, event_type))
            ids = cr.fetchall()[0]['id']

            logs = [{ 'id': ids,
                    'company_id': l['company_id'],
                    'user_id': l['user_id'],
                    'team_id': l['team_id'],
                    'create_date': l['create_date'],
                    'order_id': l['order_id'],
                    'ooid': l['ooid'],
                    'subscription_code': l['subscription_code'],
                    'event_date': l['event_date'],
                    'currency_id': l['currency_id'],
                    'subscription_state': '3_progress',
                    'recurring_monthly': mrr,
                    'amount_signed': mrr,
                    'event_type': event_type,
                    'new_enterprise_user': 0,}] + logs
            orders[order_id] = logs

            show(order_id, 'first')

        # We ensure that the 'ending' event of the parent correspond to the 'begin' of this one 
        # Add condition to avoid useless execute
        logs0[-1]['create_date'] = logs[0]['create_date']
        logs0[-1]['event_date'] = logs[0]['event_date']
        logs0[-1]['amount_signed'] = -logs0[-2]['recurring_monthly'] if len(logs0) > 1 else 0
        logs0[-1]['recurring_monthly'] = 0.00
        logs0[-1]['new_enterprise_user'] = (logs0[-1]['new_enterprise_user'] or 0)
        logs0[-1]['event_type'] = '2_churn' if logs[0]['event_type'] == '0_creation' else '3_transfer'

        cr.execute('''
            UPDATE sale_order_log 
            SET amount_signed = %s, 
                create_date = %s,
                event_date = %s,
                recurring_monthly = %s, 
                event_type = %s,
                new_enterprise_user = %s
            WHERE id = %s'''
            , (logs0[-1]['amount_signed'], 
                logs0[-1]['create_date'],
                logs0[-1]['event_date'], 
                0.00, 
                logs0[-1]['event_type'],
                logs0[-1]['new_enterprise_user'], 
                logs0[-1]['id'])
            )

        show(order_id, 'old')

        if len(logs0) > 2 and\
            logs0[-2]['event_type'] in ('1_expansion', '15_contraction') and\
            logs0[-2]['create_date'] == logs0[-1]['create_date'] and\
            logs0[-2]['create_date'] == logs0[0]['create_date'] and\
            logs0[-2]['id'] > logs0[-1]['id']:

            cr.execute('''update sale_order_log
                        set id = %s
                        where id = %s
                        ''', (-1, logs0[-1]['id']))
            cr.execute('''update sale_order_log
                        set id = %s
                        where id = %s
                        ''', (logs0[-1]['id'], logs0[-2]['id']))
            cr.execute('''update sale_order_log
                        set id = %s
                        where id = %s
                        ''', (logs0[-2]['id'], -1))

            logs0[-1]['id'], logs0[-2]['id'] = logs0[-2]['id'], logs0[-1]['id']

            show(order_id, 'swap')


        # Reconcile Transfer
        if len(logs) and logs[0]['event_type'] == '3_transfer' and\
            logs[0]['currency_id'] == orders[before][-1]['currency_id']:

            old_mrr = -orders[before][-1]['amount_signed']
            new_mrr = logs[0]['recurring_monthly']

            orders[before][-1]['event_date'] = logs[0]['event_date']

            cr.execute('''
                UPDATE sale_order_log 
                    set event_date = %s
                    where id = %s''', 
                (logs[0]['event_date'], orders[before][-1]['id']))

            if new_mrr != old_mrr:
                l = logs[0]
                cr.execute('''UPDATE sale_order_log 
                    set amount_signed = %s, 
                        recurring_monthly = %s
                    where id = %s''', 
                (old_mrr, old_mrr, l['id']))

                l['amount_signed'] = l['recurring_monthly'] = old_mrr
                # TODO insert MRR Change
                cr.execute('''
                INSERT INTO sale_order_log(
                    company_id,
                    user_id,
                    team_id,
                    order_id,
                    origin_order_id,
                    subscription_code,
                    event_date,
                    create_date,
                    currency_id,
                    subscription_state,
                    recurring_monthly,
                    amount_signed,
                    amount_expansion,
                    amount_contraction,
                    event_type
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id''',
                (l['company_id'], l['user_id'], l['team_id'], l['order_id'], l['ooid'], l['subscription_code'], 
                l['event_date'], l['create_date'], l['currency_id'], '3_progress', new_mrr, new_mrr - old_mrr, 0, new_mrr - old_mrr, '1_expansion'))
                ids = cr.fetchall()[0]['id']
                orders[order_id] = [logs[0]] + [
                {   'id': ids,
                    'company_id': l['company_id'],
                    'user_id': l['user_id'],
                    'team_id': l['team_id'],
                    'create_date': l['create_date'],
                    'order_id': l['order_id'],
                    'ooid': l['ooid'],
                    'subscription_code': l['subscription_code'],
                    'event_date': l['event_date'],
                    'currency_id': l['currency_id'],
                    'subscription_state': '3_progress',
                    'recurring_monthly': new_mrr,
                    'amount_signed': new_mrr-old_mrr,
                    'event_type': '1_expansion',
                    'new_enterprise_user': 0,}] + logs[1:]
                logs = orders[order_id]

                if len(logs) > 2 and\
                    logs[1]['create_date'] == logs[2]['create_date'] and\
                    logs[2]['event_type'] in ('3_transfer', '2_churn'):

                    cr.execute('''update sale_order_log
                        set id = %s
                        where id = %s
                        ''', (-1, logs[1]['id']))
                    cr.execute('''update sale_order_log
                        set id = %s
                        where id = %s
                        ''', (logs[1]['id'], logs[2]['id']))
                    cr.execute('''update sale_order_log
                        set id = %s
                        where id = %s
                        ''', (logs[2]['id'], -1))
            
                    logs[1]['id'], logs[2]['id'] = logs[2]['id'], logs[1]['id']

                show(order_id, 'recon')

print('Between Done')

# Fix SO 
for order_id,logs in orders.items():
    # Fix number of transfer
    cre = sum(map(lambda s:s['event_type'] == '0_creation', logs))
    ch = sum(map(lambda s:s['event_type'] == '2_churn', logs))
    ltr = []
    ltr_id = []
    for n, l in enumerate(logs):
        if l['event_type'] == '3_transfer':
            ltr += [n]
            ltr_id += [l['id']]

    t = cre + ch + len(ltr)
    # Remove all logs if we could not reconcile
    if order_id in has_next and t < 2:
        end = 0
        ltr = []
        cr.execute('DELETE from sale_order_log where order_id = %s', (order_id,))
        del logs[:]
    # Do no remove first transfer if we need one
    if not cre:
        ltr = ltr[1:]
        ltr_id = ltr_id[1:]
    # Do no remove last transfer if we need one
    if order_id in has_next and not ch:
        ltr = ltr[:-1]
        ltr_id = ltr_id[:-1]
    # Remove other transfer
    if ltr:
        cr.execute('DELETE from sale_order_log where id in %s', (tuple(ltr_id),))
        for n in reversed(ltr):
            del logs[n]

show(0, 'end')

print('IN Done')

# After pre.sql

orders = defaultdict(list)
has_next = set()
prec_order = {}

cr.execute('''
    SELECT *, coalesce(origin_order_id, order_id) as ooid
    FROM sale_order_log
    ORDER BY coalesce(origin_order_id, order_id), order_id, create_date, id
    ''')
logs = cr.fetchall()

for log in logs:
    orders[log['order_id']].append(log)

cr.execute('''
    SELECT id, subscription_id
    FROM sale_order
    WHERE subscription_id IS NOT NULL
    AND subscription_state IN ('3_progress', '4_paused', '5_renewed', '6_churn')
    ''')
sos = cr.fetchall()

for so in sos:
    has_next.add(so['subscription_id'])
    prec_order[so['id']] = so['subscription_id']


show(0, 'truth')

for order_id,logs in orders.items():
    new = sum(map(lambda s:s['event_type'] == '0_creation', logs))
    chu = sum(map(lambda s:s['event_type'] == '2_churn', logs))
    tr = sum(map(lambda s:s['event_type'] == '3_transfer', logs))

    if new > 1 or chu > 1 or new+chu+tr > 2 or (order_id in has_next and new+chu+tr != 2):
        show_table(logs)
        print(f'Error in {order_id} : Wrong number of special {new}, {chu}, {tr}, expected : {2 if order_id in has_next else 1}, got : {new+chu+tr}')
        assert 0 == 1


conn.commit()
