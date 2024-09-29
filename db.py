
from collections import defaultdict
import json
import time

class SimpleRedis:
    #For single user and server-client architecture
    def __init__(self) -> None:
        self.store = {}
        self.subscribers = defaultdict(list)  # Maps channels to subscribers
        self.transaction_queue = []  # Stores commands for transaction
        self.transaction_failed = False  # Track if any command fails in a transaction



    def set(self,key,value,expiration=None):
        # Remove expired keys first
        self._remove_expired_keys()

        expiration_time = time.time() + expiration if expiration else None
        if isinstance(value, list):
            self.store[key] = ('list', value, expiration_time)
        elif isinstance(value, set):
            self.store[key] = ('set', value, expiration_time)
        elif isinstance(value, dict):
            self.store[key] = ('hash', value, expiration_time)
        else:
            self.store[key] = ('string', value, expiration_time)


    def get(self,key):
        self._remove_expired_keys()
        if key in self.store:
            data_type, value, expiration_time = self.store[key]
            if expiration_time is None or time.time() < expiration_time:
                return value
            else:
                self.delete(key)
        return None
    
    def delete(self,key):
        if key in self.store:
            del self.store[key]

    #List methods
    def append_to_list(self,key,value):
        if key in self.store and self.store[key][0]=='list':
            self.store[key][1].append(value)
        else:
            raise KeyError(f"Key '{key}' does not exist or is not a list.")
    
    def get_list(self, key):
        if key in self.store and self.store[key][0] == 'list':
            return self.store[key][1]
        raise KeyError(f"Key '{key}' does not exist or is not a list.")
    
    #Set methods
    def add_to_set(self, key, value):
        if key in self.store and self.store[key][0] == 'set':
            self.store[key][1].add(value)
        else:
            raise KeyError(f"Key '{key}' does not exist or is not a set.")


    def remove_from_set(self, key, value):
        if key in self.store and self.store[key][0] == 'set':
            self.store[key][1].discard(value)
        else:
            raise KeyError(f"Key '{key}' does not exist or is not a set.")

    def get_set(self, key):
        if key in self.store and self.store[key][0] == 'set':
            return self.store[key][1]
        raise KeyError(f"Key '{key}' does not exist or is not a set.")
    
    # Hash methods
    def hset(self, key, field, value):
        if key in self.store and self.store[key][0] == 'hash':
            self.store[key][1][field] = value
        elif key not in self.store:
            self.store[key] = ('hash', {field: value}, None)
        else:
            raise KeyError(f"Key '{key}' does not exist or is not a hash.")

    def hget(self, key, field):
        if key in self.store and self.store[key][0] == 'hash':
            return self.store[key][1].get(field)
        raise KeyError(f"Key '{key}' does not exist or is not a hash.")
    
     #Transaction methods
    def multi(self):
        self.transaction_queue =[]
        self.transaction_failed = False  # Reset the failure flag

    
    def exec(self):
        if not self.transaction_failed:
            for command in self.transaction_queue:
                command()  # Execute each queued command
            self.transaction_queue = []
        else:
            print("Transaction failed. Rolling back changes.")


    def queue_command(self, command):
        self.transaction_queue.append(command)

     # Pub/Sub Methods
    def subscribe(self, channel):
        # Simulated subscriber for the channel
        self.subscribers[channel].append(channel)

    def publish(self, channel, message):
        # Notify all subscribers of the channel
        for subscriber in self.subscribers[channel]:
            print(f"Message to {subscriber}: {message}")

    # Handling of removing expired keys
    def _remove_expired_keys(self):
        current_time = time.time()
        keys_to_delete = [key for key, (_, _, expiration) in self.store.items()
                          if expiration is not None and current_time >= expiration]
        for key in keys_to_delete:
            del self.store[key]

    
    
    def save(self,filename):
        #saving our data
        with open(filename,'w') as f:
            json.dump(self.store,f)

    
    def load(self,filename):
        with open(filename,'r') as f:
            self.store = json.load(f)











def test_simple_redis():
    # Create an instance of SimpleRedis
    redis = SimpleRedis()
    
    # Test setting values
    print("Testing SET operation:")
    redis.set('key1', 'value1')
    print(redis.get('key1'))  # Should print 'value1'

    # Test getting a value
    print("Testing GET operation:")
    print(redis.get('key1'))  # Should print 'value1'
    print(redis.get('key2'))  # Should print 'None' (or 'nil')

    # Test deleting a value
    print("Testing DELETE operation:")
    redis.delete('key1')
    print(redis.get('key1'))  # Should print 'None' (or 'nil')

    # Test setting with expiration
    print("Testing SET with expiration:")
    redis.set('key2', 'value2', expiration=2)  # Expires after 2 seconds
    print(redis.get('key2'))  # Should print 'value2'
    time.sleep(3)  # Wait for the key to expire
    print(redis.get('key2'))  # Should print 'None' (or 'nil')

    # Test list operations
    print("Testing LIST operations:")
    redis.set('list_key', [], expiration=None)
    redis.append_to_list('list_key', 'item1')
    redis.append_to_list('list_key', 'item2')
    print(redis.get_list('list_key'))  # Should print ['item1', 'item2']

    # Test set operations
    print("Testing SET operations:")
    redis.set('set_key', set(), expiration=None)
    redis.add_to_set('set_key', 'member1')
    redis.add_to_set('set_key', 'member2')
    print(redis.get_set('set_key'))  # Should print {'member1', 'member2'}

    # Test hash operations
    print("Testing HASH operations:")
    redis.hset('hash_key', 'field1', 'value1')
    print(redis.hget('hash_key', 'field1'))  # Should print 'value1'
    print(redis.hget('hash_key', 'field2'))  # Should print 'None' (or 'nil')

    # Test transaction methods
    print("Testing Transactions:")
    redis.multi()
    redis.queue_command(lambda: redis.set('trans_key1', 'trans_value1'))
    redis.queue_command(lambda: redis.set('trans_key2', 'trans_value2'))
    redis.exec()
    print(redis.get('trans_key1'))  # Should print 'trans_value1'
    print(redis.get('trans_key2'))  # Should print 'trans_value2'

if __name__ == "__main__":
    test_simple_redis()
