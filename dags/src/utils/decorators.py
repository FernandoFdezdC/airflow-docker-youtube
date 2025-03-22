from datetime import datetime
from functools import wraps

def time_it(func : callable) -> callable:
    '''
    Decorator that takes a function and logs the time it takes to execute it
    Parameters:
        func : callable : function to be timed
        log : Logger : logger object to log the time 
    '''
    def wrapper(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        end = datetime.now()
        print(f"Function {func.__name__} executed in {end-start}")
        return result
    return wrapper
    
