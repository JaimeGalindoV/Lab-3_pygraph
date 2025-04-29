#!/usr/bin/env python3
import os

import pydgraph

import model

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

def print_menu():
    mm_options = {
        1: "Create data",
        2: "Search groups by user name",
        3: "Search first n messages from number to number",
        4: "Range of status in date order",
        5: "Number of users and their information",
        6: "Delete status older than X date",
        7: "Drop All",
        8: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


# Conexión directa con el servidor Dgraph usando gRPC.
def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)

# Crea el cliente principal de Dgraph: realizar operaciones de alto nivel como queries, mutaciones y transacciones.
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def close_client_stub(client_stub):
    client_stub.close()

def main():
    # Inicializar Client Stub y Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Crear schema
    model.set_schema(client)

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 1:
            model.create_data(client)
        if option == 2:
            person = input("Name: ")
            model.groups_by_user(client, person)
        if option == 3:
            n = input("limit messages: ")
            fromNumber = input("from number: ")
            toNumber = input("to number: ")
            model.messages_from_number_to_number(client, n, fromNumber, toNumber)
        if option == 4:
            first = input("Insert amount of status to show: ")
            offset = input("Insert amount of status to offset: ")
            model.status_in_order_in_range(client, first, offset)
        if option == 5:
            model.users_and_amount(client)
        if option == 6:
            date = input("inserta la fecha, los estados anteriores a ella serán eliminados: ")
            model.delete_status(client, date)
        if option == 7:
            model.drop_all(client)
        if option == 8:
            model.drop_all(client)
            close_client_stub(client_stub)
            exit(0)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))