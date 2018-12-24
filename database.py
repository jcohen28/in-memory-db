

class Database:
    def __init__(self):
        self.db = {}
        self.rollback_queue = []
        self.value_index = {}
    
    def validate_args(expected_args=[]):
        def real_decorator(func):
            def wrapper(self, *args):
                if len(args) == len(expected_args):
                    func(self, *args)
                else:
                    msg = '{} expects {} argument(s)'.format(action, len(expected_args))
                    if len(expected_args) > 0:
                        msg += ': {}'.format(expected_args)
                    print(msg)
            return wrapper
        return real_decorator

    def log_transaction_rollback(func):
        def wrapper(self, name, *args):
            # If Rollback Queue is empty that means there are no active transactions
            # If name is already in the current transaction then do not overwrite the original rollback state
            # Otherwise, store the original value for the given name as the rollback state
            if len(self.rollback_queue) > 0 and name not in self.rollback_queue[-1].keys():
                # Note: if the name does not exist yet in the db that means it is getting added new
                # The rollback_queue will store a value of None, which will then be handled in the rollback function
                self.rollback_queue[-1][name] = self.db.get(name)
            
            # Once the original value has been stored to be able to rollback, then process the command
            func(self, name, *args)
            
        return wrapper

    @validate_args(['name', 'value'])
    @log_transaction_rollback
    def set(self, name, value):        
        # before updating value, decrement count of original value
        # then check if new value is already in value_index, if so increment, otherwise add with initial value of 1
        self.db[name] = value

    @validate_args(['name'])
    def get(self, name):
        print(self.db.get(name) or 'NULL')
    
    @validate_args(['name'])
    @log_transaction_rollback
    def delete(self, name):        
        # use pop in case name doesn't exist
        # before deleting record, decrement count of original value
        self.db.pop(name, None)
    
    @validate_args(['value'])
    def count(self, value):        
        print(list(self.db.values()).count(value))
    
    @validate_args()
    def begin(self):
        self.rollback_queue.append({})

    @validate_args()
    def rollback(self):
        try:
            rollback_state = self.rollback_queue.pop()
            # could have used self.db.update(rollback_state) but in order to replicate exact original state
            # wanted to delete keys where the original value was None
            for name, value in rollback_state.items():
                if value:
                    self.db[name] = value
                else:
                    # Still use pop in case the name was added and deleted within the same transaction
                    self.db.pop(name, None)
        except IndexError:
            print('TRANSACTION NOT FOUND')
    
    @validate_args()
    def commit(self):
        self.rollback_queue = []

import sys

db = Database()

COMMANDS = {
    'SET': db.set,
    'GET': db.get,
    'DELETE': db.delete,
    'COUNT': db.count,
    'BEGIN': db.begin,
    'ROLLBACK': db.rollback,
    'COMMIT': db.commit,
    'END': sys.exit
}

while True:
    line = input('>> ').strip().split(' ')
    action = line[0].upper()
    args = line[1:]
    command = COMMANDS.get(action, None)

    if command:
        command(*args)
    else:
        print('{} is not a recognized action'.format(action))
