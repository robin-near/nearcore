import json
import requests

USE_COLD_STORAGE = False

class EntityAPI:
    def __init__(self, host):
        self.endpoint = "http://{}/debug/api/entity".format(host)

    def query(self, query_name, cold_storage=False, **kwargs):
        args = kwargs
        if len(args) == 0:
            args = None
        if cold_storage:
            query = {
                query_name: args,
                "use_cold_storage": cold_storage
            }
        else:
            query = {
                query_name: args,
            }
        result = requests.post(self.endpoint, json=query)
        return EntityDataValue.of(result.json())
    

class EntityDataValue:
    @staticmethod
    def of(value):
        if isinstance(value, str):
            return value
        return EntityDataValue(value)

    def __init__(self, value):
        self.value = value

    def __getitem__(self, key):
        if isinstance(key, int):
            key = str(key)
        entries = self.value['entries']

        return EntityDataValue.of([entry['value'] for entry in entries if entry['name'] == key][0])
    
    def __contains__(self, key):
        entries = self.value['entries']

        return any(entry['name'] == key for entry in entries)
    
    def array(self):
        return [EntityDataValue.of(entry['value']) for entry in self.value['entries']]
    
    def __iter__(self):
        return iter(self.array())
    
    def __str__(self):
        return json.dumps(self.value)

api = EntityAPI("34.78.183.8:3030")

BLOCK = '3sHKWjyFZA6kV7oRAtJtBFUD1TtJ3ePjft8a11xJ9rz8'
SHARD = 0

block = api.query('BlockByHash', block_hash=BLOCK, cold_storage=USE_COLD_STORAGE)
epoch_id = block['header']['epoch_id']
chunk_hashes = [chunk['chunk_hash'] for chunk in block['chunks']]
chunk_bodies = [api.query('ChunkByHash', chunk_hash=chunk, cold_storage=USE_COLD_STORAGE) for chunk in chunk_hashes]
receipts = [receipt for chunk in chunk_bodies for receipt in chunk['receipts']]
transactions_for_shard = chunk_bodies[SHARD]['transactions']
receipts_for_shard = [receipt for receipt in receipts if api.query('ShardIdByAccountId', epoch_id=epoch_id, account_id=receipt['receiver_id']) == str(SHARD)]
print("Found {} receipts from block {}, {} of them targeting shard {}".format(len(receipts), BLOCK, len(receipts_for_shard), SHARD))

# outcomes_for_receipts = [api.query('OutcomeByReceiptIdAndBlockHash', receipt_id=receipt['receipt_id'], block_hash=BLOCK, cold_storage=USE_COLD_STORAGE) for receipt in receipts_for_shard]
# outcomes_for_transactions = [api.query('OutcomeByTransactionHashAndBlockHash', transaction_hash=transaction['hash'], block_hash=BLOCK, cold_storage=USE_COLD_STORAGE) for transaction in transactions_for_shard]

total_gas_cost = 0
total_balance_burnt = 0

def ellipsify(s, max_length=32):
    if len(s) > max_length:
        return s[:max_length - 3] + "..."
    return s.ljust(max_length)

def format_gas_cost(gas_cost):
    return "{} ({:.5f} Tgas)".format(gas_cost, int(gas_cost) / 10**12)

def format_balance_burnt(balance_burnt):
    return "{} ({:.5f} mNEAR)".format(balance_burnt, int(balance_burnt) / 10**21)

for receipt in receipts_for_shard:
    try:
        outcome = api.query('OutcomeByReceiptIdAndBlockHash', receipt_id=receipt['receipt_id'], block_hash=BLOCK, cold_storage=USE_COLD_STORAGE)
    except:
        outcome = None
    if outcome:
        total_gas_cost += int(outcome['gas_burnt'])
        total_balance_burnt += int(outcome['tokens_burnt'])
        print("Receipt {} from {} to {} gas cost {} burnt balance {}".format(
            receipt['receipt_id'], ellipsify(receipt['predecessor_id']), ellipsify(receipt['receiver_id']),
             format_gas_cost(outcome['gas_burnt']),
              format_balance_burnt(outcome['tokens_burnt'])))
    else:
        print("Receipt {} from {} to {} outcome not found".format(
            receipt['receipt_id'], ellipsify(receipt['predecessor_id']), ellipsify(receipt['receiver_id'])))

for transaction in transactions_for_shard:
    try:
        outcome = api.query('OutcomeByTransactionHashAndBlockHash', transaction_hash=transaction['hash'], block_hash=BLOCK, cold_storage=USE_COLD_STORAGE)
    except:
        outcome = None
    if outcome:
        total_gas_cost += int(outcome['gas_burnt'])
        total_balance_burnt += int(outcome['tokens_burnt'])
        print("Transaction {} from {} to {} gas cost {} burnt balance {}".format(
            transaction['hash'], ellipsify(transaction['signer_id']), ellipsify(transaction['receiver_id']),
             format_gas_cost(outcome['gas_burnt']),
              format_balance_burnt(outcome['tokens_burnt'])))
        
        if transaction['signer_id'] == transaction['receiver_id']:
            receipt_id = outcome['receipt_ids'][0]
            try:
                
                receipt_outcome = api.query('OutcomeByReceiptIdAndBlockHash', receipt_id=receipt_id, block_hash=BLOCK, cold_storage=USE_COLD_STORAGE)
            except:
                receipt_outcome = None
            if receipt_outcome:
                total_gas_cost += int(receipt_outcome['gas_burnt'])
                total_balance_burnt += int(receipt_outcome['tokens_burnt'])
                print("  Local Receipt {} gas cost {} burnt balance {}".format(
                    receipt_id, format_gas_cost(receipt_outcome['gas_burnt']), format_balance_burnt(receipt_outcome['tokens_burnt'])))
            else:
                print("  Local Receipt {} outcome not found".format(receipt_id))

    else:
        print("Transaction {} from {} to {} outcome not found".format(
            transaction['hash'], ellipsify(transaction['signer_id']), ellipsify(transaction['receiver_id'])))

print("Total gas cost {} burnt balance {}".format(format_gas_cost(total_gas_cost), format_balance_burnt(total_balance_burnt)))