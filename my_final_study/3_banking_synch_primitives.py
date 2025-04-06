import threading
import time 
import concurrent.futures 

# This is the current snapshot of the account of a particular depositor of a bank. They have 0 monetary units in their account. 
balance = 0

# I want to replicate a case that is relevant to what happens at the end of the month: 
# They get particular incoming streams of money (e.g. pension or wage, dividends), which are combined with particular outflows (e.g. paying 
# the bills, paying the rent, shopping expenses). 
# So there is a particular maximum number of transactions that can be submitted to the bank's ledger to handle. 

BUFSIZE = 10
mutex = threading.Lock()  # We have a single resource requiring exclusive access: the account's balance. So we need to implement a single lock. 

ledger = [-1] * BUFSIZE  # the buffer keeping track of pending transactions - the bank considers a particular number of transactions as being reasonable to be submitted at a time
# else they could (e.g.) be falling victim of a cyber-attack


empty = threading.Semaphore(BUFSIZE)  # keeps track of empty slots in the buffer; should there be BUFSIZE empty slots, no more transactions should be allowed to take place


# A deposit transaction need not consider the current balance of the user's account. It is always considered valid. 
# The same is not true when it comes to withdrawals; in case there are insufficient funds in the user's account, such a transaction should wait 
# until enough deposits take place so as to make it valid. 
insufficient_funds = threading.Condition(lock=mutex)  # Condition variable used to delay the execution of a withdrawal transaction (!!! should add a retry time buffer !!!)
# in case there are not sufficient funds in an account to facilitate it. 

# If we let the various transactions take place concurrently in an uncontrollable manner (i.e. without refining how the transactions/threads/lines of execution 
# interact with each other and with our exclusive resource), then we could come across various race conditions:
    # (a) Concurrent ledger updates could lead to a corrupt ledger/buffer state - we could have jobs submitted at a rate higher than what 
    # is considered safe to handle by the bank 
    # (b) Maybe withdrawal transactions could try to access the ledger before deposits finish adding money to the balance => this could lead to 
    # invalid/incomplete data consumption 

# 2 types of transactions may take place: 
def deposit(transaction_id, deposit_amount):
    global nextin

    #mutex.acquire()
    empty.acquire()  
    with insufficient_funds:  # we are locking mutex automatically when working in the context of the condition variable
        do_transaction(transaction_id, "D", deposit_amount)  # this is always valid - never .wait()
        insufficient_funds.notify_all()  # let withdrawers left waiting know that they can try to withdraw again

    empty.release()      
    #mutex.release() 

    print(f"Transaction {transaction_id}: Successfully deposited {deposit_amount}, increasing global balance to {balance} monetary units.")

    return f"Successfully completed deposit with ID {transaction_id}"


def withdraw(transaction_id, withdrawal_amount):
    global balance
    global nextout

    # Even if there are 0 filled-in slots, we should be able to make a further check to see if a withdrawal is valid; 
    # In particular, there could precede very big deposits allowing for this withdrawal to take place:
    # So i first check for the deposit case and then for the full semaphore thing 
    retries = 2
    with insufficient_funds:
        while balance < withdrawal_amount and retries > 0: 
            print(f"Transaction {transaction_id}: Waiting to withdraw {withdrawal_amount} (retries left: {retries})...")
            insufficient_funds.wait(timeout=1)
            retries -= 1
            
        if balance >= withdrawal_amount:
            #mutex.acquire()  # condition variables come coupled with explicitly defined locks from their initialization - no need to acquire and release a lock
            empty.acquire()

            do_transaction(transaction_id, "W", withdrawal_amount)
            
            empty.release() 

            #mutex.release()
            print(f"Transaction {transaction_id}: Successfully withdrew {withdrawal_amount}, reducing global balance to {balance} monetary units.")
            return f"Successfully completed withdrawal with ID {transaction_id}"

        else:
            return f"Failed to complete withdrawal with ID {transaction_id}"

        



def do_transaction(transaction_id, type, amount):
    global balance 

    if type == "W":
        balance -= amount 
    elif type == "D":
        balance += amount 
    else: 
        raise Exception("Invalid type of transaction submitted.")


def main():
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        amounts = [100,200,100,200,1000]
        # Ensure the range doesn't exceed the list length
        results = [executor.submit(deposit, _, amounts[_ % len(amounts)]) if _%2==0 else executor.submit(withdraw, _, amounts[_ % len(amounts)]) for _ in range(10)]
        # Fetching the multiple results in the order as_completed
        #for f in concurrent.futures.as_completed(results):
        #    print(f.result())
        

if __name__ == "__main__":
    main()