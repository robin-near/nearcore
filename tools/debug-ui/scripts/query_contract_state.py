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
    
node = EntityAPI('34.141.156.240:3030')

leaf = node.get_leaf_in_trie('0', '9tgotkJ42LbZNM6pjLWfP4B4Hxvcaazt1QCEqLzaUb1n', '00' + b'0x86b14e5375d01286f8ff85fec5c3f114e46eed19'.hex())
print(leaf)
