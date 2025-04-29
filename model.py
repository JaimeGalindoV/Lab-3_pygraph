#!/usr/bin/env python3
import json
import csv
import ast

import pydgraph

def set_schema(client):
    schema = """
    type Group {
        name
        description
        HAS
    }

    type User {
        name
        description
        phone
        location
        SEND
        POST
    }

    type Message {
        content
        reactions
        sendAt
        RECEIVE
        BELONGS_TO
    }

    type Status {
        content
        text
        postedAt
        SEEN
    }

    name: string @index(exact) .
    description: string .
    phone: int @index(int) .
    location: geo @index(geo) .
    content: string @index(fulltext) .
    reactions: [string] .
    sendAt: datetime @index(day) .
    text: string .
    postedAt: datetime @index(day) .

    HAS: [uid] @reverse .
    SEND: [uid] .
    POST: [uid] @reverse .
    RECEIVE: uid .
    BELONGS_TO: uid .
    SEEN: [uid] .

    """
    return client.alter(pydgraph.Operation(schema=schema))

def normalize_id(id_str):
    # Normaliza un ID eliminando el prefijo '_:' si existe.
    return id_str.replace('_:', '') if id_str.startswith('_:') else id_str


def create_data(client):
    file_path_users = './data/users.csv'
    file_path_groups = './data/groups.csv'
    file_path_messages = './data/messages.csv'
    file_path_statuses = './data/statuses.csv'

    # Crear usuarios y grupos
    usersUids = load_users(client, file_path_users)
    groupsUids = load_groups(client, file_path_groups)

    # agregar los usuarios a sus grupos
    load_groups_users_edges(client, file_path_groups, usersUids, groupsUids)  

    # crear mensajes
    messagesUids = load_messages(client, file_path_messages)

    # crear relaciones de mensjaes con usuarios y grupos
    load_messages_users_groups_edges(client, file_path_messages, file_path_users, messagesUids, usersUids, groupsUids)

    statusesUids = load_statuses(client, file_path_statuses)

    # crear relaciones de usuarios con status
    load_user_status_edges(client, file_path_users, file_path_statuses, usersUids, statusesUids)


def load_users(client, file_path):
    # Import users.
    txn = client.txn()
    response = None
    try:
        users = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            for row in reader:
                location = ast.literal_eval(row['location'])
                users.append({
                    'uid': row['uid'],
                    'dgraph.type': 'User',
                    'name': row['name'],
                    'description': row['description'],
                    'phone': row['phone'],
                    'location': {
                        "type": "Point",
                        "coordinates": location
                    }
                })

            print(f"Loading products: ")
            for p in users:
                print(p)

            response = txn.mutate(set_obj=users)

        # Commit transaction.
        txn.commit()
        
        print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()
    return response.uids

def load_groups(client, file_path):
    # Import users.
    txn = client.txn()
    response = None
    try:
        groups = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            for row in reader:
                groups.append({
                    'uid': row['uid'],
                    'dgraph.type': 'Group',
                    'name': row['name'],
                    'description': row['description'],
                })

            print(f"Loading groups: ")
            for p in groups:
                print(p)

            response = txn.mutate(set_obj=groups)

        # Commit transaction.
        txn.commit()
        
        print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()
    return response.uids

def load_messages(client, file_path):
    # Import users.
    txn = client.txn()
    response = None
    try:
        messages = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            for row in reader:
                messages.append({
                    'uid': row['uid'],
                    'dgraph.type': 'Message',
                    'content': row['content'],
                    'reactions': ast.literal_eval(row['reactions']),
                    'sendAt': row['sendAt']
                })

            print(f"Loading messages: ")
            for p in messages:
                print(p)

            response = txn.mutate(set_obj=messages)

        # Commit transaction.
        txn.commit()
        
        print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()
    return response.uids

def load_statuses(client, file_path):
    # Import users.
    txn = client.txn()
    response = None
    try:
        statuses = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            for row in reader:
                statuses.append({
                    'uid': row['uid'],
                    'dgraph.type': 'Status',
                    'content': row['content'],
                    'text': row['text'],
                    'postedAt': row['postedAt']
                })

            print(f"Loading statuses: ")
            for p in statuses:
                print(p)

            response = txn.mutate(set_obj=statuses)

        # Commit transaction.
        txn.commit()
        
        print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()
    return response.uids


def load_groups_users_edges(client, file_path, users_uids, goups_uids):
    txn = client.txn()
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                group = normalize_id(row['uid'])

                users = [normalize_id(u) for u in ast.literal_eval(row['HAS'])]
                users_dic = [{'uid': users_uids[u]} for u in users]
                # print(f"users: {users}\nDic: {users_dic}")

                print(f"Generating relationship {goups_uids[group]} -HAS-> {users_dic}\n")
                mutation = {
                    'uid': goups_uids[group],
                    'HAS': users_dic
                }
                txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()

def load_messages_users_groups_edges(client, file_path_messages, file_path_users, messages_uids, users_uids, groups_uids):
    txn = client.txn()
    try:
        # Agregar la relación user -> SEND -> message
        with open(file_path_users, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                user = normalize_id(row['uid'])

                messages = [normalize_id(m) for m in ast.literal_eval(row['SEND'])]

                messages_dic = [{'uid': messages_uids[m]} for m in messages]
                # print(f"messages: {messages}\nDic: {messages_dic}")

                print(f"Generating relationship {users_uids[user]} -SEND-> {messages_dic}")
                mutation = {
                    'uid': users_uids[user],
                    'SEND': messages_dic
                }
                txn.mutate(set_obj=mutation)
        
        # Agregar la relación message -> RECEIVE -> user
        with open(file_path_messages, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Obtengo ids normalizados
                message = normalize_id(row['uid'])
                group = normalize_id(row['BELONGS_TO'])
                user = normalize_id(row['RECEIVE'])

                # print(f"group: {group}, user: {user}")

                # Elegir qué relación será, si hacia grupo o hacia persona
                if user != "":
                    relation = 'RECEIVE'
                    uid = users_uids[user]
                else:
                    relation = 'BELONGS_TO'
                    uid = groups_uids[group]

                print(f"Generating relationship {messages_uids[message]} -{relation}-> {uid}")
                mutation = {
                    'uid': messages_uids[message],
                    relation: {
                        'uid': uid
                    }
                }

                txn.mutate(set_obj=mutation)

        txn.commit()
    finally:
        txn.discard()
    
def load_user_status_edges(client, file_path_users, file_path_statuses, users_uids, statuses_uids):
    txn = client.txn()
    try:
        # Agregar la relación user -> POST -> status
        with open(file_path_users, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:

                user = normalize_id(row['uid'])

                statuses = [normalize_id(s) for s in ast.literal_eval(row['POST'])]

                users_dic = [{'uid': statuses_uids[s]} for s in statuses]
                # print(f"users: {users}\nDic: {users_dic}")

                print(f"Generating relationship {user} -POST-> {statuses}")
                mutation = {
                    'uid': users_uids[user],
                    'POST': users_dic
                }
                txn.mutate(set_obj=mutation)

        # Agregar la relación status -> SEEN -> user
        with open(file_path_statuses, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:

                status = normalize_id(row['uid'])

                users = [normalize_id(u) for u in ast.literal_eval(row['SEEN'])]

                users_dic = [{'uid': users_uids[u]} for u in users]
                # print(f"users: {users}\nDic: {users_dic}")

                print(f"Generating relationship {status} -SEEN-> {users}")
                mutation = {
                    'uid': statuses_uids[status],
                    'SEEN': users_dic
                }
                txn.mutate(set_obj=mutation)

        txn.commit()
    finally:
        txn.discard()


# 4 queries:
#/ o	Query the indexed text field.
#/ o	Query the indexed numeric field.
#/ o	A query that extracts graph data from the graph which involves at least 2 node types.
#/ o	A query which allows to exercised the reversed relationship.
#/ o	A query which allows to order data.
#/ o	A query which uses the count method.
#/ o	A query which uses pagination.



# Groups by specific user:
# - Query the indexed text field.
# - A query which allows to exercised the reversed relationship.
def groups_by_user(client, name):
    query = """
    query search_groups_by_user($name: string){
        groups_by_user(func: type(User)) @filter(eq(name, $name)) {
            name
                ~HAS {
                    name
            }
        }
    }
    """

    variables = {'$name': str(name)}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print(f"Number of groups by user {name}: {len(ppl['groups_by_user'][0]['~HAS'])}")
    print(f"Groups associated with user {name}:\n{json.dumps(ppl['groups_by_user'][0]['~HAS'], indent=2)}")

# Get messages sent to a specific number
# o	Query the indexed numeric field.
# o	A query that extracts graph data from the graph which involves at least 2 node types.
def messages_from_number_to_number(client, n, fromPhone, toPhone):
    query = """
    query messages_to_user($n: int, $fromPhone: int, $toPhone: int){
        messages_to_user(func: eq(phone, $fromPhone)) {
            SEND (first: $n) @cascade {
                content
                reactions
                sendAt
                RECEIVE @filter(eq(phone, $toPhone)) {
                }
            }
        }
    }
    """

    variables = {'$n': str(n), '$fromPhone': str(fromPhone), '$toPhone': str(toPhone)}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    # print(ppl)
    print(f"Messages associated from phone: {fromPhone} to phone: {toPhone}:\n{json.dumps(ppl['messages_to_user'][0]['SEND'], indent=2)}")


# Get all statuses in order (like seeing all statuses of my contacts)
# o	A query which uses pagination.
# o	A query which allows to order data.
# o	A query which allows to exercised the reversed relationship.
def status_in_order_in_range(client, first, offset):
    query = """
    query all_statuses_in_order_by_date($first: int, $offset: int){
        all_statuses(func: type(Status), orderasc: postedAt, first: $first, offset: $offset) {
            content
            text
            postedAt
            ~POST{
                name
            }
        }
    }
    """
    variables = {'$first': str(first), '$offset': str(offset)}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    # print(ppl)
    print(f"Number of statuses: {len(ppl['all_statuses'])}")
    print(f"Statuses:\n{json.dumps(ppl['all_statuses'], indent=2)}")


# Number of users and their information
# o	A query which uses the count method.
# o	A query which allows to order data.
def users_and_amount(client):
    query = """
    query get_users_and_users_amount(){
        users_amount(func: type(User), orderasc: name){
            totalUsers: count(uid)
            name
            phone
            location 
        }
    }
    """

    res = client.txn(read_only=True).query(query)
    ppl = json.loads(res.json)

    # Print results.
    # print(ppl)
    print(f"Number of users: {(ppl['users_amount'][0]["totalUsers"])}")
    if ppl['users_amount'][0]['totalUsers'] != 0:
        print(f"Info of users:\n{json.dumps(ppl['users_amount'][1:], indent=2)}")

# Delete status older than a date
def delete_status(client, date):
    # Create a new transaction.
    txn = client.txn()
    try:
        query = """
        query search_status($date: string) {
            order_status(func: type(Status)) @filter(le(postedAt, $date)) {
                uid
                content
                text
                postedAt
            }
        }"""
        variables = {'$date': str(date)}
        res1 = client.txn(read_only=True).query(query, variables=variables)
        ppl1 = json.loads(res1.json)
        for status in ppl1['order_status']:
            print("UID: " + status['uid'])
            txn.mutate(del_obj=status)
            print(f"{status} deleted")
        commit_response = txn.commit()
        print(commit_response)
    finally:
        txn.discard()


def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))
