# -------------------- Imports --------------------
import threading
import queue
import time
from enum import Enum
from collections import deque


# -------------------- Transaction Type --------------------
class TransactionType(Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


# -------------------- Transaction Model --------------------
class Transaction:
    def __init__(self, timestamp, transaction_id, amount, tx_type, account_no):
        self.timestamp = timestamp
        self.transaction_id = transaction_id
        self.amount = amount
        self.tx_type = tx_type
        self.account_no = account_no

    def __repr__(self):
        return f"Transaction({self.timestamp}, {self.transaction_id}, {self.amount}, {self.tx_type.value}, Acc:{self.account_no})"


# -------------------- Account Model --------------------
class Account:
    account_counter = 1000

    def __init__(self, user_id):
        Account.account_counter += 1
        self.account_no = Account.account_counter
        self.user_id = user_id
        self.balance = 0

    def apply_transaction(self, transaction):
        if transaction.tx_type == TransactionType.CREDIT:
            self.balance += transaction.amount
        else:
            self.balance -= transaction.amount

    def __repr__(self):
        return f"AccountNo:{self.account_no} | User:{self.user_id} | Balance:{self.balance}"


# -------------------- Sliding Window Processor --------------------
class SlidingWindowProcessor:
    def __init__(self, window_size=600):
        self.window = deque()
        self.total_amount = 0
        self.window_size = window_size
        self.lock = threading.Lock()
        self.seen_tx_ids = set()
        self.accounts = {}
        self.tx_counter = 0

    def create_account(self, user_id):
        with self.lock:
            acc = Account(user_id)
            self.accounts[acc.account_no] = acc
            print("🏦 Account Created:", acc)
            return acc.account_no

    def process(self, transaction):
        with self.lock:

            if transaction.transaction_id in self.seen_tx_ids:
                print("❌ Duplicate transaction:", transaction.transaction_id)
                return

            if transaction.account_no not in self.accounts:
                print("❌ Account not found:", transaction.account_no)
                return

            self.seen_tx_ids.add(transaction.transaction_id)

            account = self.accounts[transaction.account_no]
            account.apply_transaction(transaction)

            # Add to sliding window
            self.window.append(transaction)

            # Add/subtract properly for analytics
            if transaction.tx_type == TransactionType.CREDIT:
                self.total_amount += transaction.amount
            else:
                self.total_amount -= transaction.amount

            # Remove expired transactions
            while self.window and \
                transaction.timestamp - self.window[0].timestamp > self.window_size:

                old = self.window.popleft()

                if old.tx_type == TransactionType.CREDIT:
                    self.total_amount -= old.amount
                else:
                    self.total_amount += old.amount

            print("✅ Processed:", transaction)
            print("💰 Account Balance:", account.balance)
            print("📊 10-min Net Total:", self.total_amount)
            print("-" * 40)


# -------------------- Producer Thread --------------------
class Producer(threading.Thread):
    def __init__(self, q, processor):
        super().__init__(daemon=True)
        self.q = q
        self.processor = processor

    def run(self):
        while True:
            try:
                line = input(
                    "Enter tx (account_no,amount,DEBIT/CREDIT): "
                ).strip()

                if not line:
                    continue

                parts = [p.strip() for p in line.split(",")]

                if len(parts) != 3:
                    print("⚠️ Format: account_no,amount,DEBIT")
                    continue

                account_no = int(parts[0])
                amount = float(parts[1])
                tx_type = TransactionType(parts[2])

                # 🔥 Auto timestamp
                timestamp = int(time.time())

                # 🔥 Auto transaction ID
                with self.processor.lock:
                    self.processor.tx_counter += 1
                    tx_id = f"TXN{self.processor.tx_counter}"

                tx = Transaction(
                    timestamp,
                    tx_id,
                    amount,
                    tx_type,
                    account_no
                )

                self.q.put(tx)

            except Exception as e:
                print("❌ Invalid Input:", e)

# -------------------- Consumer Thread --------------------
class Consumer(threading.Thread):
    def __init__(self, q, processor):
        super().__init__(daemon=True)
        self.q = q
        self.processor = processor

    def run(self):
        while True:
            tx = self.q.get()
            self.processor.process(tx)
            self.q.task_done()


# -------------------- Main --------------------
def main():
    q = queue.Queue()
    processor = SlidingWindowProcessor()

    # Create accounts
    acc1 = processor.create_account(user_id=1)
    acc2 = processor.create_account(user_id=1)
    acc3 = processor.create_account(user_id=2)

    print(f"📋 Available accounts: {acc1}, {acc2}, {acc3}")

    producer = Producer(q, processor)

    consumer1 = Consumer(q, processor)
    consumer2 = Consumer(q, processor)

    producer.start()
    consumer1.start()
    consumer2.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")


if __name__ == "__main__":
    main()
