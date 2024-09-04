import json
import requests
from multiprocessing.pool import ThreadPool

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
    
    def get_node_in_trie_at_path(self, shard_uid, state_root, path):
        trie_path = '{}/{}/{}'.format(shard_uid, state_root, path)
        try:
            return self.query('TrieNode', trie_path=trie_path)
        except KeyboardInterrupt:
            raise
        except:
            return None

    def get_leaf_in_trie(self, shard_uid, state_root, key):
        path = ''
        while len(path) <= len(key):
            if path != key[:len(path)]:
                return None
            node = self.get_node_in_trie_at_path(shard_uid, state_root, path)
            if node is None:
                return None
            if 'extension' in node:
                path = node['extension'].split('/')[2]
                continue
            if 'leaf_path' in node:
                if node['leaf_path'] == key:
                    return node['value']
            if path == key:
                return None
            next_nibble = key[len(path)]
            if next_nibble in node:
                path += next_nibble
                continue
            return None
        return None


class EntityDataValue:
    @staticmethod
    def of(value):
        if isinstance(value, str) or value is None:
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
    
stuck_node = EntityAPI('34.141.156.240:3030')
good_node = EntityAPI('34.78.183.8:3030')

NEXT_BLOCK = '69ng57zzdDLGQU8WjsNXF5joL4hY7THKcCa1LnMJvDLi'
BLOCK = '3sHKWjyFZA6kV7oRAtJtBFUD1TtJ3ePjft8a11xJ9rz8'
SHARD = 0
BAD_STATE_ROOT = 'AFSEPGR69SLi54xtFmMEtaQHAEJwVWkcARx6VJ8Fe2W7'  # can be None

block = good_node.query('BlockByHash', block_hash=BLOCK)
next_block = good_node.query('BlockByHash', block_hash=NEXT_BLOCK)
epoch_id = block['header']['epoch_id']
shard_uid = good_node.query('ShardUIdByShardId', shard_id=SHARD, epoch_id=epoch_id)

bad_flat_state_changes = stuck_node.query('FlatStateChangesByBlockHash', block_hash=BLOCK, shard_uid=shard_uid)

good_trie_root = next_block['chunks'][SHARD]['prev_state_root']
bad_state_root = BAD_STATE_ROOT

def check_key(change):
    print("Checking key: {}".format(change['key']))
    good_value = good_node.get_leaf_in_trie(shard_uid, good_trie_root, change['key'])
    if bad_state_root:
        bad_value = stuck_node.get_leaf_in_trie(shard_uid, bad_state_root, change['key'])
    else:
        bad_value = good_value

    bad_value_from_flat = change['value']
    if not (good_value == bad_value_from_flat == bad_value):
        print('Mismatch at key: {}'.format(change['key']))
        print('Good value: {}'.format(good_value))
        print('Bad value from flat changes: {}'.format(bad_value_from_flat))
        if bad_state_root:
            print('Bad value from bad trie: {}'.format(bad_value))
        print()

pool = ThreadPool(10)
pool.map(check_key, bad_flat_state_changes)