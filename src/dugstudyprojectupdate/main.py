import argparse
import json

import requests


def main():
    parser = argparse.ArgumentParser(description='Util that updates fields in ES dug database')
    parser.add_argument('-n', help='<Required> Path to json file with updated values', required=True)
    parser.add_argument('-i', help='<Required> Index name in ElasticSearch', required=True)
    parser.add_argument('-e', help='<Required> ElasticSearch server full url. For example: https://USERNAME:PASSWORD@localhost:9200', required=True)
    args = parser.parse_args()

    data = {}

    with open(args.n) as json_file:
        data = json.load(json_file)
        print(f"Found {len(data)} items to update in {args.n}")

    num_elements = len(data) - 1
    i = 0
    for id, v in data.items():
        print(f"{i}/{num_elements} Updating {id}")
        update_es_index(args.i, args.e, id, v)
        i += 1

def update_es_index(index_name, url, id, data):
    print(f"Updating {index_name} {id}")
    headers = { 'Content-Type': 'application/json' }
    update_req = { "doc": data}
    response = requests.post(url + f"/{index_name}/_update/{id}", headers=headers, json=update_req, verify=False)
    res = response.json()

    if 'error' in res:
        print(f"Error: {res}")
        raise Exception(f"Error while updating {index_name} {id}")
    elif res['result'] == "noop":
        print("NOT UPDATED (noop)")
    elif res['result'] == "updated":
        print("UPDATED OK")
    print(res)





if __name__ == '__main__':
    main()
