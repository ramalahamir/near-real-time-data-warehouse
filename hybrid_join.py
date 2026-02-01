import threading
import queue
import time
import csv
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import getpass

# Asking for the database credentials
print("\n-------- Enter Warehouse Database Credentials --------")
host = input("MySQL host: ") 
user = input("MySQL username: ")
password = getpass.getpass("MySQL password: ")

DW_DATABASE_CONFIG = {
    'host': host,
    'database': 'ramalah',
    'user': user,
    'password': password
}

# DW_DATABASE_CONFIG = {
#     'host': 'localhost',
#     'database': 'dw_project_dummy',
#     'user': 'root',
#     'password': 'wuhaib2113122'
# }

HASH_TOTAL_SLOTS = 10000  # 10,000 slots to accommodate the stream tuples
PARTITION_SIZE = 500      # Each disk partition size will be 500 tuples
CUSTOMER_KEY = 'Customer_ID'
PRODUCT_KEY = 'Product_ID'

# Trying to cast primary keys to int for correct sorting otherwise keep as string
def change_to_int(v):
    try:
        return int(v)
    except Exception:
        return v

# Loading Master Data. sorting by primary key
# and dividing into partitions
def Loading_master_data(file_path, key_field, partition_size):
    rows = []
    with open(file_path, mode='r', newline='') as f:
        reader = csv.DictReader(f)
        for r in reader:
            # Normalizing the primary key for ease in sorting
            normalized = r.copy()
            normalized[key_field] = change_to_int(r[key_field])
            rows.append(normalized)

    # Sorting
    rows.sort(key=lambda x: x[key_field])
    partitions = [rows[i:i + partition_size] for i in range(0, len(rows), partition_size)]
    return partitions

CUSTOMER_MD = Loading_master_data('customer_master_data.csv', CUSTOMER_KEY, PARTITION_SIZE)
PRODUCT_MD = Loading_master_data('product_master_data.csv', PRODUCT_KEY, PARTITION_SIZE)

# Stream buffer (thread-safe)
STREAM_BUFFER = queue.Queue()

# creating our doubly linked queue
class Node:
    def __init__(self, key, stream_tuple):
        self.key = key
        self.stream_tuple = stream_tuple
        self.prev = None
        self.next = None

class DoublyLinkedQueue:
    def __init__(self):
        self.head = None
        self.tail = None
        self.size = 0

    def append(self, node: Node):
        if not self.tail:
            self.head = self.tail = node
            node.prev = node.next = None
        else:
            node.prev = self.tail
            node.next = None
            self.tail.next = node
            self.tail = node
        self.size += 1

    # popping the oldest
    def pop(self):
        if not self.head:
            return None
        node = self.head
        if node.next:
            self.head = node.next
            self.head.prev = None
        else:
            self.head = self.tail = None
        node.prev = node.next = None
        self.size -= 1
        return node

    # removing a specific node
    def remove_node(self, node: Node):
        if node.prev:
            node.prev.next = node.next
        else:
            # node is head
            self.head = node.next
        if node.next:
            node.next.prev = node.prev
        else:
            # node is tail
            self.tail = node.prev
        node.prev = node.next = None
        self.size -= 1

class HybridJoin_SharedMem_StateTracker:
    def __init__(self, hash_slots):
        # hash_table is an array of buckets
        # each entry is a dict: { 'key': , 'tuple': , 'queue_node': }
        self.hash_slots = hash_slots
        self.hash_table = [[] for _ in range(hash_slots)]
        self.queue = DoublyLinkedQueue()

        # Disk buffer holds the current partition
        self.disk_buffer = []

        # keeping track of available slots 
        self.w = hash_slots

        # helps prevent race conditions
        self.lock = threading.Lock()

    def hash_function(self, key):
        if key is None:
            raise ValueError("Key cannot be None for hashing")
        return abs(hash(str(key))) % self.hash_slots

# continuously gets data from the transactional_data
class TransactionalDataLoadingThread(threading.Thread):
    def __init__(self, data_file, stream_buffer):
        super().__init__()
        self.data_file = data_file
        self.stream_buffer = stream_buffer
        self.running = True

    def run(self):
        print("Transactional Data Loading Thread started...")
        with open(self.data_file, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if not self.running:
                    break
                # Normalizing keys to same types as partitions if possible
                if CUSTOMER_KEY in row:
                    row[CUSTOMER_KEY] = change_to_int(row[CUSTOMER_KEY])
                if PRODUCT_KEY in row:
                    row[PRODUCT_KEY] = change_to_int(row[PRODUCT_KEY])
                self.stream_buffer.put(row)
        print("Transactional Data Loading Thread finished reading file")
        self.running = False

    def stop(self):
        self.running = False

class HybridJoinThread(threading.Thread):
    def __init__(self, state: HybridJoin_SharedMem_StateTracker, stream_buffer: queue.Queue,
                 customer_partitions, product_partitions):
        super().__init__()
        self.state = state
        self.stream_buffer = stream_buffer
        self.customer_partitions = customer_partitions
        self.product_partitions = product_partitions
        self.running = True

    def run(self):
        print("Hybrid Join Thread started...")
        while self.running or not self.stream_buffer.empty() or self.check_hash_table_not_empty():
            # Loading up to w stream data into the hash table and queue
            with self.state.lock:
                taken_slots = min(self.state.w, self.stream_buffer.qsize())

            for _ in range(taken_slots):
                s_tuple = self.stream_buffer.get()
                key = s_tuple.get(CUSTOMER_KEY)
                if key is None:
                    # skipping rows without primary key 
                    continue

                # creating queue node and hash-table entry + storing pointer between them
                node = Node(key=key, stream_tuple=s_tuple)
                entry = {'key': key, 'tuple': s_tuple, 'queue_node': node}

                with self.state.lock:
                    # inserting into bucket
                    bucket_idx = self.state.hash_function(key)
                    self.state.hash_table[bucket_idx].append(entry)

                    # appending node to doubly-linked queue
                    self.state.queue.append(node)

                    # decrementing available slots 
                    self.state.w -= 1

            if taken_slots > 0:
                print(f"Loaded {taken_slots} stream tuples; w={self.state.w}; queue_size={self.state.queue.size}")
            if self.stream_buffer.empty() and self.state.queue.size == 0 and not self.check_hash_table_not_empty():
                print("No more stream data and queue/hash table empty. Stopping thread.")
                break

            # If queue is empty then wait for more stream tuples
            if self.state.queue.size == 0:
                time.sleep(0.05)
                continue

            # Getting the oldest key to use its key value to load the customer master data partition
            with self.state.lock:
                oldest_node = self.state.queue.head  
                if oldest_node is None:
                    continue
                oldest_key = oldest_node.key

            # Finding the partition containing the oldest_key
            partition_index = self.finding_data_partition(self.customer_partitions, CUSTOMER_KEY, oldest_key)
            if partition_index is None:
                # we remove entries for which master doesn't exist.
                print(f"Customer key {oldest_key} not found in any customer partition; evicting corresponding entries.")
                self.remove_matched_key_entries(oldest_key)
                continue

            # Loading the partition into the disk buffer
            with self.state.lock:
                self.state.disk_buffer = self.customer_partitions[partition_index]
            print(f"Loaded Customer Partition {partition_index} for key {oldest_key} ({len(self.state.disk_buffer)} tuples)")

            # Probing the hash-table for matches using each tuple in disk buffer
            newly_freed = 0  # keeping track of freed buckets to update w
            outputs = []  # partial: stream + customer

            # For each tuple in disk buffer compute the hash bucket and probe that bucket
            for r_tuple in self.state.disk_buffer:
                r_key = r_tuple.get(CUSTOMER_KEY)
                if r_key is None:
                    continue
                bucket_idx = self.state.hash_function(r_key)

                # checking for matches in the hash table
                with self.state.lock:
                    bucket = self.state.hash_table[bucket_idx]
                    # We will iterate over bucket and find entries with same key
                    idx = 0
                    while idx < len(bucket):
                        entry = bucket[idx]
                        if entry['key'] == r_key:
                            # Match found then joining the data
                            joined = {**entry['tuple'], **r_tuple}
                            outputs.append(joined)

                            node = entry['queue_node']
                            # removing the hash bucket entry
                            bucket.pop(idx)
                            # removing from queue using the pointer
                            self.state.queue.remove_node(node)
                            newly_freed += 1
                           
                        else:
                            idx += 1

            # updating w by newly freed slots
            with self.state.lock:
                self.state.w += newly_freed
            if newly_freed:
                print(f"Freed {newly_freed} slots after customer join; w={self.state.w}; queue_size={self.state.queue.size}")

            # For each partially customer enriched output
            # performing product enrichment
            if outputs:
                fully_enriched = []
                # joining each output with the product partition that contains its Product_ID
                for partial in outputs:
                    p_key = partial.get(PRODUCT_KEY)
                    if p_key is None:
                        # if no product key keep the partial only
                        fully_enriched.append(partial)
                        continue

                    p_partition_index = self.finding_data_partition(self.product_partitions, PRODUCT_KEY, p_key)
                    if p_partition_index is None:
                        # if product not found still keep partial
                        fully_enriched.append(partial)
                        continue

                    # Loading product partition and finding matches
                    product_buffer = self.product_partitions[p_partition_index]
                    product_dict = {p[PRODUCT_KEY]: p for p in product_buffer}

                    if p_key in product_dict:
                        final = {**partial, **product_dict[p_key]}
                        fully_enriched.append(final)
                    else:
                        fully_enriched.append(partial)

                # 6) Load fully_enriched into the DW
                self.load_into_data_warehouse(fully_enriched)
                for i, t in enumerate(fully_enriched[:10], 1):
                    print(f"{i}: {t}")


        print("HybridJoinThread finished.")

    # Helper functions
    def check_hash_table_not_empty(self):
        with self.state.lock:
            for bucket in self.state.hash_table:
                if bucket:
                    return True
            return False

    def finding_data_partition(self, partitions, key_field, key_value):
        for idx, part in enumerate(partitions):
            if not part:
                continue
            min_k = part[0][key_field]
            max_k = part[-1][key_field]
        
            if min_k <= key_value <= max_k:
                return idx
        return None

    def remove_matched_key_entries(self, key_value):
        removed = 0
        with self.state.lock:
            # scanning all buckets and removing entries that match key value
            for bucket in self.state.hash_table:
                idx = 0
                while idx < len(bucket):
                    entry = bucket[idx]
                    if entry['key'] == key_value:
                        node = entry['queue_node']
                        bucket.pop(idx)
                        # Removing node from queue
                        try:
                            self.state.queue.remove_node(node)
                        except Exception:
                            pass
                        removed += 1
                        
                    else:
                        idx += 1
            self.state.w += removed
        if removed:
            print(f"Evicted {removed} stream tuples for key {key_value}; w={self.state.w}; queue_size={self.state.queue.size}")

    def load_into_data_warehouse(self, data):
        if not data:
            return

        try:
            conn = mysql.connector.connect(**DW_DATABASE_CONFIG)
            cursor = conn.cursor()

            # customer insert
            insert_customer = """
                INSERT INTO customer_dim (customer_id, gender, age, occupation,
                                        city_category, stay_in_current_city_years, marital_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE customer_id = customer_id;
            """

            # product insert
            insert_product = """
                INSERT INTO product_dim (product_id, product_category, price,
                                        store_id, store_name, supplier_id, supplier_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE product_id = product_id;
            """

            # seeing if time exists
            select_time = """
                SELECT date_id FROM time_dim WHERE full_date = %s;
            """

            # time insert
            insert_time = """
                INSERT INTO time_dim (full_date, day_of_week, month, quarter, season, year)
                VALUES (%s, %s, %s, %s, %s, %s);
            """

            # fact insert
            insert_fact = """
                INSERT INTO saleFact (order_id, customer_id, product_id,
                                    date_id, quantity, purchase_amount)
                VALUES (%s, %s, %s, %s, %s, %s);
            """

            for row in data:
                customer_values = (
                    row["Customer_ID"],
                    row["Gender"],
                    int(str(row["Age"]).replace('+', '').split('-')[0].strip()),  # converting to int
                    row["Occupation"],
                    row["City_Category"],
                    row["Stay_In_Current_City_Years"],
                    row["Marital_Status"]
                )
                cursor.execute(insert_customer, customer_values)

                product_values = (
                    row["Product_ID"],
                    row["Product_Category"],
                    float(row["price$"]),
                    int(row["storeID"]),
                    row["storeName"],
                    int(row["supplierID"]),
                    row["supplierName"]
                )
                cursor.execute(insert_product, product_values)

                # seeing if time exists
                full_date = datetime.strptime(row["date"], "%m/%d/%Y").date()
                cursor.execute(select_time, (full_date,))
                result = cursor.fetchone()

                if result:
                    date_id = result[0]
                else:
                    # derive attributes
                    day_of_week = full_date.strftime("%A")
                    month = full_date.strftime("%B")
                    quarter = (full_date.month - 1)//3 + 1
                    year = full_date.year

                    # determine season 
                    mm = full_date.month
                    if mm in (12, 1, 2):
                        season = "Winter"
                    elif mm in (3, 4, 5):
                        season = "Spring"
                    elif mm in (6, 7, 8):
                        season = "Summer"
                    else:
                        season = "Autumn"

                    cursor.execute(insert_time,
                        (full_date, day_of_week, month, quarter, season, year))
                    conn.commit() 
                    date_id = cursor.lastrowid

                quantity = int(row["quantity"])
                price = float(row["price$"])
                purchase_amount = round(quantity * price, 2)

                fact_values = (
                    int(row["orderID"]),
                    row["Customer_ID"],
                    row["Product_ID"],
                    date_id,
                    quantity,
                    purchase_amount
                )
                cursor.execute(insert_fact, fact_values)

            conn.commit()
            print(f"Loaded {len(data)} records into DW.")

        except Exception as e:
            print("DW Load Error:", e)
            if conn:
                conn.rollback()

        finally:
            if cursor:
                cursor.close()
            if conn and conn.is_connected():
                conn.close()

        def stop(self):
            self.running = False

def main():
    join_state = HybridJoin_SharedMem_StateTracker(HASH_TOTAL_SLOTS)

    stream_loader = TransactionalDataLoadingThread('transactional_data.csv', STREAM_BUFFER)
    join_core = HybridJoinThread(
        join_state,
        STREAM_BUFFER,
        CUSTOMER_MD,   
        PRODUCT_MD
    )

    stream_loader.start()
    join_core.start()

    try:
        stream_loader.join()
        join_core.join()
    except KeyboardInterrupt:
        print("\nStopping threads...")
        stream_loader.stop()
        join_core.stop()
        stream_loader.join()
        join_core.join()
        print("Threads gracefully terminated.")

if __name__ == "__main__":
    main()
