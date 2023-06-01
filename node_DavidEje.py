from argparse import ArgumentParser
from threading import Thread
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

M = 5
PORT = 1234
RING = [2, 7, 11, 17, 22, 27]


class Node:
    def __init__(self, node_id):
        """Initializes the node properties and constructs the finger table according to the Chord formula"""
        self.finger_table = []
        self.node_id = node_id
        self.finger_table = [Node.finger_table_successor((node_id + (2**i))% (2**M)) for i in range(M)]
        self.data = {}
        self.successor = RING[(RING.index(node_id) + 1) % len(RING)]
        print(f"Node created! Finger table = {self.finger_table}")

    @classmethod
    def finger_table_successor(cls, id):
        ans = -1
        for val in RING:
            if val >= id:
                ans = val
                break
        if ans == -1:
            return min(RING)
        return ans
    


    def closest_preceding_node(self, id):
        """Returns node_id of the closest preceeding node (from n.finger_table) for a given id"""
        for val in reversed(self.finger_table):
            if self.node_id < id:
                for i in range(self.node_id + 1, id):
                    if i == val:
                        return val
            else:
                for i in range(self.node_id + 1, 2 ** M):
                    if i == val:
                        return val
                for i in range(1, id):
                    if i == val:
                        return val
        return self.node_id

    def find_successor(self, id):
        """Recursive function returning the identifier of the node responsible for a given id"""
        if id == self.node_id:
            return id
     
        if id > self.node_id and id <= self.successor or (id < self.node_id and id <= self.successor):
            return self.successor

        n = self.closest_preceding_node(id)

        if n == self.node_id:
            return self.node_id
        
        nextNode = ServerProxy(f'http://node_{n}:{PORT}')
        print(f"Forwarding request (key={id}) to node {n}")
        return nextNode.find_successor(id)

    def put(self, key, value):
        """Stores the given key-value pair in the node responsible for it"""
        print(f"put({key}, {value})")
        if key >= 2 ** M or key < 0:
            return False
        nextNode = self.find_successor(key)
        if nextNode == self.node_id: 
            return self.store_item(key, value)
        else:
            proxy = ServerProxy(f'http://node_{nextNode}:{PORT}')
            return proxy.store_item(key, value)

    def get(self, key):
        """Gets the value for a given key from the node responsible for it"""
        print(f"get({key})")
        n = self.find_successor(key)
        if n == self.node_id:
            return self.retrieve_item(key)
        node = ServerProxy(f'http://node_{n}:{PORT}')
        return node.retrieve_item(key)

    def store_item(self, key, value):
        """Stores a key-value pair into the data store of this node"""
        self.data[key] = value
        return True

    def retrieve_item(self, key):
        """Retrieves a value for a given key from the data store of this node"""
        ans = self.data.get(key)
        if ans is None:
            return -1
        return ans


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('node_id', type = int)
    args = parser.parse_args()
    node = Node(args.node_id)
    server = SimpleXMLRPCServer((f"node_{args.node_id}", PORT), logRequests = False)
    server.register_instance(node)
    server.serve_forever()
