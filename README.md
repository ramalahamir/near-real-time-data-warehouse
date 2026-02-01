## Hybrid Stream-Master Data Join Project

#### 1. Requirements

-   Python 3.8+
-   MySQL Server
-   mysql-connector-python
-   CSV files:
    -   customer_master_data.csv
    -   product_master_data.csv
    -   transactional_data.csv

#### 2. How to Run

-  Download the provided folder
-  Ensure the MySQL database dw_project has been run
-  Run the script:
        python hybrid_join.py
-  Enter MySQL host, username, and password when prompted.

#### 3. What the Program Does

-   Loads master data into memory in sorted partitions.
-   Streams transactional data in real-time using a separate thread.
-   Performs a hybrid join using another thread on:
    -   Customer master data
    -   Product master data
-   Enriches the stream tuples and loads them into the Data Warehouse
    tables:
    -   customer_dim
    -   product_dim
    -   time_dim
    -   saleFact

#### 4. Stopping the Program

Press CTRL + C to stop.
Threads will shut down gracefully.
