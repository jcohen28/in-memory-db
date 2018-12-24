

class Database:
    def __init__(self):
        self.db = {}
        self.rollback_queue = []
        self.value_count = {}
    
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
    
    def _decrement_value_count(self, value):
        count = self.value_count.get(value, 0) if value else 0
        if count > 0:
            self.value_count[value] = count - 1
    
    def _increment_value_count(self, value):
        self.value_count[value] = self.value_count.get(value, 0) + 1
    
    # This _set function contains the actual set logic but can also be called internally to bypass the decorators
    def _set(self, name, value):
        # before updating value, decrement count of original value
        orig_value = self.db.get(name, None)
        self._decrement_value_count(orig_value)
        
        # then check if new value is already in value_count, if so increment, otherwise add with initial value of 1
        self._increment_value_count(value)
        self.db[name] = value

    # This _delete function contains the actual delete logic but can also be called internally to bypass the decorators
    def _delete(self, name):
        # before deleting record, decrement count of original value
        value = self.db.get(name, None)
        self._decrement_value_count(value)
        
        # use pop in case name doesn't exist
        self.db.pop(name, None)


    @validate_args(['name', 'value'])
    @log_transaction_rollback
    def set(self, name, value):
        # This set function is called by the command line actions to run the validation and rollback logic
        self._set(name, value)

    @validate_args(['name'])
    def get(self, name):
        print(self.db.get(name) or 'NULL')
    
    @validate_args(['name'])
    @log_transaction_rollback
    def delete(self, name):
        # This delete function is called by the command line actions to run the validation and rollback logic
        self._delete(name)
    
    @validate_args(['value'])
    def count(self, value):        
        print(self.value_count.get(value, 0))
    
    @validate_args()
    def begin(self):
        self.rollback_queue.append({})

    @validate_args()
    def rollback(self):
        try:
            rollback_state = self.rollback_queue.pop()            
            for name, value in rollback_state.items():
                # call the private _set or _delete functions to skip the rollback logic since we are in the middle of a rollback itself
                if value:
                    self._set(name, value)                    
                else:
                    self._delete(name)                    
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
