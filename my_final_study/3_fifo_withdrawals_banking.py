import threading
import queue
import time

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

transaction_queue = queue.Queue()  # Queue used to ensure FIFO submission of transactions

condition = threading.Condition()  # Used to control access and waiting on insufficient funds

# 2 types of transactions may take place: 
def deposit(transaction_id, deposit_amount):
    return ("D", transaction_id, deposit_amount)

def withdraw(transaction_id, withdrawal_amount):
    return ("W", transaction_id, withdrawal_amount)


# Executes one transaction at a time — in FIFO order
def transaction_worker():
    global balance
    while True:
        tx = transaction_queue.get()
        if tx is None:
            break  # signal to stop the worker

        tx_type, tx_id, amount = tx

        with condition:
            if tx_type == "D":
                balance += amount
                print(f"Transaction {tx_id}: Successfully deposited {amount}, increasing global balance to {balance} monetary units.")
                condition.notify_all()  # wake up any withdrawal that was blocked
            elif tx_type == "W":
                retries = 5
                while balance < amount and retries > 0:
                    print(f"Transaction {tx_id}: Waiting to withdraw {amount} (retries left: {retries})...")
                    condition.wait(timeout=1)
                    retries -= 1
                if balance >= amount:
                    balance -= amount
                    print(f"Transaction {tx_id}: Successfully withdrew {amount}, reducing global balance to {balance} monetary units.")
                else:
                    print(f"Transaction {tx_id}: Failed to complete withdrawal of {amount} — insufficient funds after retries.")

        transaction_queue.task_done()


def main():
    # Start a single transaction-processing thread
    worker = threading.Thread(target=transaction_worker, daemon=True)
    worker.start()

    # Schedule 100 transactions in strict alternating order
    amounts = [100, 200, 100, 200, 1000]
    for i in range(10):
        if i % 2 == 0:
            tx = deposit(i, amounts[i % len(amounts)])
        else:
            tx = withdraw(i, amounts[i % len(amounts)])
        transaction_queue.put(tx)

    transaction_queue.join()  # Wait until all are processed
    transaction_queue.put(None)  # Signal to stop the worker
    worker.join()

if __name__ == "__main__":
    main()
