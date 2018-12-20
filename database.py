class Database:
    def __init__(self):
        self.db = {}
        self.rollback_queue = []
    
    def log_transaction_rollback(func):
        def wrapper(self, name, *args):
            # If no name is provided the function will print an error message so skip this logic
            # If Rollback Queue is empty that means there are no active transactions
            # If name is already in the current transaction then do not overwrite the original rollback state
            # Otherwise, store the original value for the given name as the rollback state
            if name and len(self.rollback_queue) > 0 and name not in self.rollback_queue[-1].keys():
                # Note: if the name does not exist yet in the db that means it is getting added new
                # The rollback_queue will store a value of None, which will then be handled in the rollback function
                self.rollback_queue[-1][name] = self.db.get(name)
            
            # Once the original value has been stored to be able to rollback, then process the command
            func(self, name, *args)
            
        return wrapper

    @log_transaction_rollback
    def set(self, name, value):        
        self.db[name] = value

    def get(self, name):
        print(self.db.get(name) or 'NULL')
    
    @log_transaction_rollback
    def delete(self, name):        
        # use pop in case name doesn't exist
        self.db.pop(name, None)
    
    def count(self, value):        
        print(list(self.db.values()).count(value))
    
    def begin(self):
        self.rollback_queue.append({})

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
    
    def commit(self):
        self.rollback_queue = []



import sys

db = Database()

class Command:
    def __init__(self, func, expected_args=[]):
        self.execute = func
        self.expected_args = expected_args
    
    def validate(self, args):
        return len(args) == len(self.expected_args)        

# Started with a simple dict of {'SET': db.set, ...} but created the class to include validation
COMMANDS = {
    'SET': Command(db.set, ['name', 'value']),
    'GET': Command(db.get, ['name']),
    'DELETE': Command(db.delete, ['name']),
    'COUNT': Command(db.count, ['value']),
    'BEGIN': Command(db.begin),
    'ROLLBACK': Command(db.rollback),
    'COMMIT': Command(db.commit),
    'END': Command(sys.exit)
}

while True:
    line = input('>> ').split(' ')
    action = line[0].upper()
    args = line[1:]
    command = COMMANDS.get(action, None)

    if not command:
        print('{} is not a recognized action'.format(action))
    elif not command.validate(args):
        print('{} expects {} argument(s) but received {}'.format(action, command.expected_args, args))
    else:
        command.execute(*args)        
