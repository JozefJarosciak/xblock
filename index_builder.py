import sqlite3
import argparse
import requests
from passlib.hash import argon2
import hashlib
import json
import time

parser = argparse.ArgumentParser(description="Your script description")
parser.add_argument("ethereum_address", type=str, help="Your Ethereum address")
args = parser.parse_args()

my_ethereum_address = args.ethereum_address


def format_time(seconds):
    """Formats time in seconds to a string of the form hours:minutes"""
    hours, remainder = divmod(seconds, 3600)
    minutes = remainder // 60
    return f"{int(hours)}h {int(minutes)}m"

def hash_value(value):
    return hashlib.sha256(value.encode()).hexdigest()


def build_merkle_tree(elements):
    merkle_tree = {}
    while len(elements) > 1:
        new_elements = []
        for i in range(0, len(elements), 2):
            left = elements[i]
            right = elements[i + 1] if i + 1 < len(elements) else left
            new_hash = hash_value(left + right)
            merkle_tree[new_hash] = {'left': left, 'right': right}
            new_elements.append(new_hash)
        elements = new_elements
    return elements[0], merkle_tree


def validate(eth_address):
    with sqlite3.connect('blockchain.db') as conn:
        c = conn.cursor()
        c.execute('SELECT id, id, block_hash FROM blockchain order by id desc limit 1')
        row = c.fetchone()
        if row:
            total_count, last_block_id, last_block_hash = row
            validation_data = {
                "total_count": total_count,
                "my_ethereum_address": eth_address,
                "last_block_id": last_block_id,
                "last_block_hash": last_block_hash
            }
            print(validation_data)
            requests.post("http://xenminer.mooo.com/validate", json=validation_data)


def get_total_blocks():
    response = requests.get("http://xenminer.mooo.com/total_blocks")
    if response.status_code == 200:
        try:
            data = response.json()
            return (data.get("total_blocks_top100", 0) - 100) // 100
        except Exception as e:
            print(f"Error parsing JSON: {e}")
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")


def process_blocks():
    with sqlite3.connect('blockchain.db') as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS blockchain (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        prev_hash TEXT,
                        merkle_root TEXT,
                        records_json TEXT,
                        block_hash TEXT)''')

        c.execute('SELECT MAX(id), block_hash FROM blockchain')
        row = c.fetchone()
        last_block_id = row[0] if row and row[0] is not None else 0
        prev_hash = row[1] if row else 'genesis'
        print("Last fetched block ID from blockchain:", last_block_id)

        total_blocks = get_total_blocks()
        if total_blocks is None:
            print("Failed to retrieve total_blocks. Exiting process_blocks.")
            return  # Return early if total_blocks is None

        num_to_fetch = max(1, total_blocks - (last_block_id or 0))  # Use 0 if last_block_id is None
        end_block_id = last_block_id + num_to_fetch
        print("Number of records to fetch:", num_to_fetch)
        print("End block ID:", end_block_id)

        start_time = time.time()
        for i, block_id in enumerate(range(last_block_id + 1, end_block_id + 1), 1):
            current_time = time.time()
            elapsed_time = current_time - start_time
            avg_time_per_block = elapsed_time / i
            remaining_blocks = num_to_fetch - i
            estimated_remaining_time = avg_time_per_block * remaining_blocks

            formatted_time = format_time(estimated_remaining_time)

            print(f"Processing block {block_id} out of {end_block_id} ({((block_id - last_block_id) / num_to_fetch) * 100:.2f}%)  | Estimated time remaining: {formatted_time}")

            url = f"http://xenminer.mooo.com:4445/getblocks/{block_id}"
            response = requests.get(url)
            if response.status_code != 200:
                continue

            records = response.json()
            if len(records) < 100:
                print("All sealed blocks are current")
                break

            verified_hashes = [hash_value(str(block_id) + record.get("hash_to_verify") + record.get("key") + record.get("account"))
                               for record in records if argon2.verify(record.get("key"), record.get("hash_to_verify"))]

            if verified_hashes:
                merkle_root, _ = build_merkle_tree(verified_hashes)
                block_contents = str(prev_hash) + merkle_root
                block_hash = hash_value(block_contents)
                c.execute('REPLACE INTO blockchain (id, prev_hash, merkle_root, records_json, block_hash) VALUES (?,?,?,?,?)',
                          (block_id, prev_hash, merkle_root, json.dumps(records), block_hash))
                conn.commit()
                prev_hash = block_hash


def verify_block_hashes():
    with sqlite3.connect('blockchain.db') as conn:
        c = conn.cursor()
        c.execute('SELECT id, prev_hash, merkle_root, block_hash, records_json FROM blockchain ORDER BY id')
        prev_hash = 'genesis'
        for row in c.fetchall():
            id, prev_hash_db, merkle_root, block_hash, records_json = row
            block_contents = str(prev_hash) + merkle_root
            computed_block_hash = hash_value(block_contents)
            if computed_block_hash != block_hash:
                print(f"Block {id} is invalid. Computed hash doesn't match the stored hash.")
                return False

            records = json.loads(records_json)
            verified_hashes = [hash_value(str(id) + record.get("hash_to_verify") + record.get("key") + record.get("account"))
                               for record in records if argon2.verify(record.get("key"), record.get("hash_to_verify"))]

            computed_merkle_root, _ = build_merkle_tree(verified_hashes)
            if computed_merkle_root != merkle_root:
                print(f"Block {id} is invalid. Computed Merkle root doesn't match the stored Merkle root.")
                return False
            prev_hash = block_hash

        print("All blocks are valid.")
        return True


process_blocks()
validate(my_ethereum_address)
