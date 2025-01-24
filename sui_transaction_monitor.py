import requests
import time
import datetime
from typing import Dict, Any
from requests.exceptions import RequestException
from sqlalchemy import create_engine, Table, Column, String, DateTime, Integer, ARRAY, JSON, MetaData, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Database configuration
DB_CONFIG = {
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'port': '5432',
    'database': 'sui_db'
}

# API configuration
RPC_URL = "your_sui_rpc_endpoint"

def setup_database():
    """Initialize database connection and schema"""
    engine = create_engine(f'postgresql://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}')
    metadata = MetaData()

    # Define transactions table
    transactions = Table('transactions', metadata,
        Column('digest', String(44), nullable=False, primary_key=True),
        Column('sender', String(66), nullable=False),
        Column('timestamp', DateTime, nullable=False),
        Column('checkpoint', Integer, nullable=False),
        Column('transaction_type', String(255), nullable=False),
        Column('status', String(7), nullable=False),
        Column('package_id', String(66)),
        Column('function', String(255)),
        Column('total_gas_used', Integer, nullable=False),
        Column('created_objects', ARRAY(String(66))),
        Column('deleted_objects', ARRAY(String(66))),
        Column('modified_objects', ARRAY(String(66))),
        Column('events', JSON)
    )

    # Create tables if they don't exist
    metadata.create_all(engine)
    return engine, transactions

def make_api_request(payload: Dict[str, Any], max_retries: int = 3, delay: int = 2) -> Dict[str, Any]:
    """Make API request with retry logic"""
    headers = {'Content-Type': 'application/json'}
    
    for attempt in range(max_retries):
        try:
            response = requests.post(RPC_URL, headers=headers, json=payload)
            response_json = response.json()
            
            if 'error' in response_json:
                print(f"API Error: {response_json['error']}")
                if attempt < max_retries - 1:
                    time.sleep(delay * (attempt + 1))
                    continue
                raise Exception(f"API error after {max_retries} attempts")
                
            return response_json['result']
            
        except RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request failed, retrying... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(delay * (attempt + 1))
                continue
            raise Exception(f"Request failed after {max_retries} attempts: {str(e)}")

def get_latest_transaction():
    """Get the latest transaction from the blockchain"""
    # Get latest block
    latest_block = make_api_request({
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'sui_getLatestCheckpointSequenceNumber',
        'params': []
    })
    
    time.sleep(1)  # Increased delay between requests
    
    # Get block details
    block_data = make_api_request({
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'sui_getCheckpoint',
        'params': [latest_block]
    })
    
    return block_data['transactions'][-1] if block_data['transactions'] else None

def get_transaction_details(digest: str):
    """Get detailed transaction information"""
    return make_api_request({
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'sui_getTransactionBlock',
        'params': [
            digest,
            {
                'showInput': True,
                'showEffects': True,
                'showEvents': True,
                'showObjectChanges': True,
                'showBalanceChanges': True
            }
        ]
    })

def cleanse_transaction(tx_data):
    """Clean and structure transaction data"""
    print("Starting data cleansing...")
    print(f"Raw transaction data keys: {tx_data.keys()}")
    
    # Extract basic transaction data
    transaction = tx_data.get('transaction', {})
    effects = tx_data.get('effects', {})
    
    # Get transaction data
    tx_data_details = transaction.get('data', {})
    
    # Extract sender from transaction data
    sender = tx_data_details.get('sender', '')
    
    # Get transaction type and package info
    transaction_type = 'unknown'
    package_id = ''
    function_name = ''
    
    # Process events first to get package_id and function
    events = tx_data.get('events', [])
    processed_events = []
    for event in events:
        processed_event = {
            'type': event.get('type', ''),
            'sender': event.get('sender', ''),
            'packageId': event.get('packageId', ''),
            'transactionModule': event.get('transactionModule', ''),
            'data': event.get('parsedJson', {})
        }
        processed_events.append(processed_event)
        
        # Extract package_id and function from the first event if not already set
        if not package_id and event.get('packageId'):
            package_id = event.get('packageId')
        if not function_name and event.get('transactionModule'):
            function_name = event.get('transactionModule')
    
    # Extract from transaction data if not found in events
    if 'transaction' in tx_data_details:
        tx_info = tx_data_details['transaction']
        transaction_type = tx_info.get('kind', 'unknown')
        
        # Extract package and function info from MoveCall if not already set
        if not package_id and transaction_type == 'ProgrammableTransaction' and 'commands' in tx_info:
            for cmd in tx_info['commands']:
                if isinstance(cmd, dict) and cmd.get('type') == 'MoveCall':
                    package_id = cmd.get('package', '')
                    module = cmd.get('module', '')
                    func = cmd.get('function', '')
                    if module and func:
                        function_name = f"{module}::{func}"
                    break
    
    # Get status
    status = effects.get('status', {}).get('status', 'unknown')
    
    # Calculate total gas used
    gas_used = 0
    if 'gasUsed' in effects:
        gas_info = effects['gasUsed']
        computation_cost = int(gas_info.get('computationCost', 0))
        storage_cost = int(gas_info.get('storageCost', 0))
        storage_rebate = int(gas_info.get('storageRebate', 0))
        gas_used = computation_cost + storage_cost - storage_rebate
    
    # Get object changes
    created_objects = []
    deleted_objects = []
    modified_objects = []
    
    # Extract from effects
    if 'objectChanges' in effects:
        for change in effects['objectChanges']:
            change_type = change.get('type', '').lower()
            object_id = change.get('objectId', '')
            if object_id:
                if change_type == 'created':
                    created_objects.append(object_id)
                elif change_type == 'deleted':
                    deleted_objects.append(object_id)
                elif change_type == 'modified':
                    modified_objects.append(object_id)
    
    # Additional check for deleted objects in effects
    if 'deleted' in effects:
        deleted_refs = effects['deleted']
        for ref in deleted_refs:
            obj_id = ref.get('objectId', '')
            if obj_id and obj_id not in deleted_objects:
                deleted_objects.append(obj_id)
    
    # Get timestamp
    timestamp_ms = tx_data.get('timestampMs', None)
    if timestamp_ms:
        timestamp = datetime.datetime.fromtimestamp(int(timestamp_ms) / 1000)
    else:
        timestamp = datetime.datetime.now()
    
    cleaned_data = {
        'digest': tx_data.get('digest', ''),
        'sender': sender,
        'timestamp': timestamp,
        'checkpoint': int(tx_data.get('checkpoint', 0)),
        'transaction_type': transaction_type,
        'status': status,
        'package_id': package_id,
        'function': function_name,
        'total_gas_used': gas_used,
        'created_objects': created_objects,
        'deleted_objects': deleted_objects,
        'modified_objects': modified_objects,
        'events': processed_events
    }
    
    print(f"Cleaned transaction data: {cleaned_data}")
    return cleaned_data

def store_transaction(engine, transactions_table, tx_data):
    """Store transaction data in database"""
    try:
        print(f"\nAttempting to store transaction: {tx_data['digest']}")
        print(f"Transaction data to be stored: {tx_data}")
        
        with engine.begin() as conn:
            # First check if transaction already exists
            check_stmt = select(transactions_table).where(
                transactions_table.c.digest == tx_data['digest']
            )
            existing = conn.execute(check_stmt).fetchone()
            if existing:
                print(f"Transaction {tx_data['digest']} already exists in database")
            
            stmt = pg_insert(transactions_table).values(tx_data)
            stmt = stmt.on_conflict_do_update(
                constraint='transactions_pkey',
                set_=tx_data
            )
            result = conn.execute(stmt)
            print(f"Database operation completed. Rows affected: {result.rowcount}")
            
            # Verify the insertion
            verify_stmt = transactions_table.select().where(
                transactions_table.c.digest == tx_data['digest']
            )
            verification = conn.execute(verify_stmt).fetchone()
            if verification:
                print(f"Verified: Transaction {tx_data['digest']} is in database")
            else:
                print(f"WARNING: Transaction {tx_data['digest']} not found after insertion!")
                
    except Exception as e:
        print(f"Database error: {str(e)}")
        print(f"Attempted to insert/update transaction: {tx_data['digest']}")
        print(f"Transaction data: {tx_data}")
        raise

def insert_transaction(tx_data):
    """
    Insert transaction data into PostgreSQL with proper error handling
    """
    try:
        # Insert with upsert (update on conflict)
        with engine.begin() as conn:
            stmt = pg_insert(transactions).values(tx_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['digest'],
                set_=tx_data
            )
            result = conn.execute(stmt)
            print(f"Successfully inserted/updated transaction {tx_data['digest']}")
            return result
            
    except Exception as e:
        print(f"Error inserting transaction: {str(e)}")
        print(f"Problematic data: {tx_data}")
        raise

def main():
    """Main execution loop"""
    print("Starting Sui transaction monitor...")
    try:
        engine, transactions_table = setup_database()
        print("Successfully connected to database")
        
        with engine.connect() as conn:
            count_query = select(func.count()).select_from(transactions_table)
            count = conn.execute(count_query).scalar()
            print(f"Current number of rows in transactions table: {count}")
            
    except Exception as e:
        print(f"Failed to connect to database: {str(e)}")
        return

    last_processed_tx = None

    while True:
        try:
            # Get latest transaction
            latest_tx_digest = get_latest_transaction()
            print(f"Latest transaction digest: {latest_tx_digest}")
            
            if latest_tx_digest and latest_tx_digest != last_processed_tx:
                print(f"\nProcessing new transaction: {latest_tx_digest}")
                
                time.sleep(1)
                
                # Get and process transaction details
                tx_details = get_transaction_details(latest_tx_digest)
                print(f"Got transaction details: {tx_details.keys()}")
                
                cleaned_tx = cleanse_transaction(tx_details)
                print(f"Cleaned transaction data: {cleaned_tx}")
                
                print(f"Attempting to store transaction {cleaned_tx['digest']}")
                store_transaction(engine, transactions_table, cleaned_tx)
                
                print(f"Successfully processed and stored transaction: {latest_tx_digest}")
                last_processed_tx = latest_tx_digest
            else:
                print("No new transactions found")
            
            time.sleep(1)
            
        except Exception as e:
            print(f"Error in main loop: {str(e)}")
            time.sleep(1)

if __name__ == "__main__":
    main()  