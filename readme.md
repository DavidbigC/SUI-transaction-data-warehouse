
The monitor will:
1. Connect to the specified PostgreSQL database
2. Create necessary tables if they don't exist
3. Start monitoring new transactions
4. Process and store transaction details
5. Continue monitoring in an infinite loop

## Database Schema

The transactions table includes the following columns:
- `digest` (Primary Key): Transaction identifier
- `sender`: Transaction sender address
- `timestamp`: Transaction timestamp
- `checkpoint`: Checkpoint number
- `transaction_type`: Type of transaction
- `status`: Transaction status
- `package_id`: Smart contract package ID
- `function`: Called function name
- `total_gas_used`: Total gas consumed
- `created_objects`: Array of created object IDs
- `deleted_objects`: Array of deleted object IDs
- `modified_objects`: Array of modified object IDs
- `events`: JSON array of transaction events

## Error Handling

The script includes comprehensive error handling for:
- API request failures with automatic retries
- Database connection issues
- Transaction processing errors
- Data validation
