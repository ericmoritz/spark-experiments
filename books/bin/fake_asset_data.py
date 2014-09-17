import sys
import json
import random

for line in sys.stdin:
    book_data = json.loads(line)
    if "base_metadata" in book_data and "print_isbn" in book_data['base_metadata']:
        asset_record = {"isbn": book_data['base_metadata']['print_isbn'],
                        "asset_id": random.randint(1, 1000000)}
        print json.dumps(asset_record)
