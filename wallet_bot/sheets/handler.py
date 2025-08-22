"""
High-level database manager for the Messenger Wallet Bot.

This module provides a clean interface for all database operations, abstracting away
the complexities of the two-sheet system. It manages both the Data_Log sheet (raw data)
and the Formatted_Report sheet (human-readable format).

This is the application's single point of contact for any Google Sheets operation.
It should be called exclusively by the messenger.handler module.
"""

import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import pandas as pd

from wallet_bot.sheets import api
from wallet_bot.config.settings import (
    get_data_log_sheet_name,
    get_formatted_report_sheet_name
)
from wallet_bot.utils.timezone import (
    now_manila,
    format_manila_timestamp,
    get_week_start_manila,
    get_month_start_manila,
    parse_manila_timestamp,
    MANILA_TIMEZONE
)

# Set up logging
logger = logging.getLogger(__name__)

# Data_Log sheet column structure
DATA_LOG_COLUMNS = [
    'timestamp',
    'transaction_type',
    'category_or_source',
    'description',
    'amount',
    'user_id'
]


def log_transaction(transaction_type: str, category_or_source: str, 
                   description: str, amount: float, user_id: str) -> bool:
    """
    Log a single transaction to the Data_Log sheet.
    
    Args:
        transaction_type (str): 'income' or 'expense'
        category_or_source (str): Category for expenses or source for income
        description (str): Transaction description
        amount (float): Transaction amount (always positive)
        user_id (str): Facebook user ID
        
    Returns:
        bool: True if successful, False otherwise
        
    Raises:
        Exception: If logging operation fails
    """
    try:
        # Validate inputs
        if transaction_type not in ['income', 'expense']:
            raise ValueError(f"Invalid transaction_type: {transaction_type}")
        
        if amount <= 0:
            raise ValueError(f"Amount must be positive: {amount}")
        
        if not category_or_source or not description:
            raise ValueError("Category/source and description cannot be empty")
        
        # Create timestamp in Manila timezone
        timestamp = format_manila_timestamp()
        
        # Prepare row data
        row_data = [
            timestamp,
            transaction_type,
            category_or_source,
            description,
            amount,
            user_id
        ]
        
        # Append to Data_Log sheet
        sheet_name = get_data_log_sheet_name()
        success = api.append_row(sheet_name, row_data)
        
        if success:
            logger.info(f"Successfully logged {transaction_type} transaction: ₱{amount:.2f} - {description}")
        
        return success
        
    except Exception as e:
        logger.error(f"Failed to log transaction: {str(e)}")
        raise Exception(f"Failed to log transaction: {str(e)}")


def get_transactions_for_period(period: str = "This Week", user_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Retrieve transactions from Data_Log sheet for analysis.
    
    Args:
        period (str): "This Week" or "This Month"
        user_id (Optional[str]): Filter by specific user ID, None for all users
        
    Returns:
        List[Dict[str, Any]]: List of transaction dictionaries
        
    Raises:
        Exception: If reading operation fails
    """
    try:
        sheet_name = get_data_log_sheet_name()
        
        # Get all records from Data_Log
        all_records = api.get_all_records(sheet_name)
        
        # Debug logging
        logger.info(f"DEBUG: Retrieved {len(all_records)} total records from sheet")
        logger.info(f"DEBUG: Looking for period='{period}', user_id='{user_id}'")
        
        if not all_records:
            logger.info("No transactions found in Data_Log sheet")
            return []
        
        # Debug: Show sample record
        if all_records:
            logger.info(f"DEBUG: Sample record: {all_records[0]}")
        
        # Filter by user_id if specified - FIXED: Convert both to strings for comparison
        if user_id:
            original_count = len(all_records)
            # Convert both user_id values to strings for reliable comparison
            all_records = [record for record in all_records if str(record.get('user_id', '')) == str(user_id)]
            logger.info(f"DEBUG: Filtered from {original_count} to {len(all_records)} records for user_id '{user_id}'")
        
        # Convert to DataFrame for easier date filtering
        df = pd.DataFrame(all_records)
        
        if df.empty:
            logger.info(f"No transactions found for user: {user_id}")
            return []
        
        # Debug: Show DataFrame info
        logger.info(f"DEBUG: DataFrame shape: {df.shape}")
        logger.info(f"DEBUG: DataFrame columns: {list(df.columns)}")
        
        # Ensure timestamp column exists and convert to datetime
        if 'timestamp' not in df.columns:
            logger.warning("No timestamp column found in Data_Log")
            return all_records
        
        try:
            # Debug: Show sample timestamps before conversion
            logger.info(f"DEBUG: Sample timestamps before conversion: {df['timestamp'].head().tolist()}")
            
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            
            # Check for any failed conversions
            failed_conversions = df['timestamp'].isna().sum()
            if failed_conversions > 0:
                logger.warning(f"DEBUG: {failed_conversions} timestamps failed to convert")
            
            # Debug: Show sample timestamps after conversion
            logger.info(f"DEBUG: Sample timestamps after conversion: {df['timestamp'].head().tolist()}")
            
        except Exception as e:
            logger.warning(f"Could not parse timestamps: {str(e)}")
            return all_records
        
        # Remove rows with invalid timestamps
        df = df.dropna(subset=['timestamp'])
        
        if df.empty:
            logger.info("No valid timestamps found after conversion")
            return []
        
        # Filter by period - FIXED: Better date calculation logic
        filtered_df = _filter_transactions_by_period_fixed(df, period)
        
        # Convert back to list of dictionaries
        transactions = filtered_df.to_dict('records')
        
        logger.info(f"Retrieved {len(transactions)} transactions for period '{period}'")
        return transactions
        
    except Exception as e:
        logger.error(f"Failed to get transactions for period '{period}': {str(e)}")
        raise Exception(f"Failed to retrieve transactions: {str(e)}")


# In wallet_bot/sheets/handler.py

def _filter_transactions_by_period_fixed(df: pd.DataFrame, period: str) -> pd.DataFrame:
    """
    FIXED: Filter transactions DataFrame by the specified time period using Manila timezone.
    
    Args:
        df (pd.DataFrame): DataFrame with transaction data
        period (str): "Today", "This Week", or "This Month"
        
    Returns:
        pd.DataFrame: Filtered DataFrame
    """
    # Use Manila timezone for all date calculations
    now_manila_time = now_manila()
    
    # Debug: Show current time in Manila
    logger.info(f"DEBUG: Current Manila time: {now_manila_time}")
    logger.info(f"DEBUG: Filtering for period: '{period}'")
    
    
    if period == "Today":
        # Get the start of today (midnight) in Manila timezone
        cutoff = now_manila_time.replace(hour=0, minute=0, second=0, microsecond=0)
    
    elif period == "This Week":
        # Get start of week in Manila timezone
        cutoff = get_week_start_manila(now_manila_time)
        
    elif period == "This Month":
        # Get start of month in Manila timezone
        cutoff = get_month_start_manila(now_manila_time)
        
    else:
        logger.warning(f"Unknown period '{period}', returning all transactions")
        return df
        
    
    # Debug: Show cutoff date
    logger.info(f"DEBUG: Cutoff date (Manila): {cutoff}")
    
    # Convert cutoff to naive datetime for comparison with parsed timestamps
    # (since the timestamps from sheets are parsed as naive datetime)
    cutoff_naive = cutoff.replace(tzinfo=None)
    cutoff_timestamp = pd.Timestamp(cutoff_naive)
    
    # Filter transactions after cutoff date
    filtered_df = df[df['timestamp'] >= cutoff_timestamp]
    
    logger.info(f"DEBUG: Filtered {len(df)} transactions to {len(filtered_df)} for period '{period}'")
    
    # Debug: Show some sample dates from filtered results
    if not filtered_df.empty:
        sample_dates = filtered_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').head(3).tolist()
        logger.info(f"DEBUG: Sample filtered dates: {sample_dates}")
    
    return filtered_df


# Additional debug function you can add to test manually:
def debug_date_filtering(user_id: str):
    """
    Debug function to help troubleshoot date filtering issues.
    Call this function manually to see what's happening.
    """
    try:
        logger.info("=== DEBUG DATE FILTERING ===")
        
        # Get raw data
        sheet_name = get_data_log_sheet_name()
        all_records = api.get_all_records(sheet_name)
        
        logger.info(f"Total records: {len(all_records)}")
        
        if all_records:
            logger.info(f"Sample record: {all_records[0]}")
        
        # Filter by user
        user_records = [r for r in all_records if str(r.get('user_id', '')) == str(user_id)]
        logger.info(f"User records: {len(user_records)}")
        
        if user_records:
            logger.info(f"Sample user record: {user_records[0]}")
            
            # Test timestamp parsing
            sample_timestamp = user_records[0].get('timestamp')
            logger.info(f"Sample timestamp: '{sample_timestamp}' (type: {type(sample_timestamp)})")
            
            try:
                parsed_time = pd.to_datetime(sample_timestamp, format='%Y-%m-%d %H:%M:%S')
                logger.info(f"Parsed timestamp: {parsed_time}")
                
                # Test date calculations
                now = datetime.now()
                logger.info(f"Current time: {now}")
                
                # This week calculation
                days_since_monday = now.weekday()
                start_of_week = now - timedelta(days=days_since_monday)
                start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
                logger.info(f"Start of week: {start_of_week}")
                
                # Check if transaction is this week
                is_this_week = parsed_time >= pd.Timestamp(start_of_week)
                logger.info(f"Is transaction from this week? {is_this_week}")
                
                # Time difference
                diff = parsed_time - pd.Timestamp(start_of_week)
                logger.info(f"Time difference: {diff}")
                
            except Exception as e:
                logger.error(f"Error parsing timestamp: {str(e)}")
        
        logger.info("=== END DEBUG ===")
        
    except Exception as e:
        logger.error(f"Debug function failed: {str(e)}")

def _filter_transactions_by_period(df: pd.DataFrame, period: str) -> pd.DataFrame:
    """
    Filter transactions DataFrame by the specified time period.
    
    Args:
        df (pd.DataFrame): DataFrame with transaction data
        period (str): "This Week" or "This Month"
        
    Returns:
        pd.DataFrame: Filtered DataFrame
    """
    now = datetime.now()
    
    if period == "This Week":
        # Get Monday of current week
        days_since_monday = now.weekday()
        start_of_week = now.date() - pd.Timedelta(days=days_since_monday)
        cutoff = pd.Timestamp(start_of_week)
    elif period == "This Month":
        # Get first day of current month
        start_of_month = now.replace(day=1).date()
        cutoff = pd.Timestamp(start_of_month)
    else:
        logger.warning(f"Unknown period '{period}', returning all transactions")
        return df
    
    # Filter transactions after cutoff date
    filtered_df = df[df['timestamp'] >= cutoff]
    
    logger.debug(f"Filtered {len(df)} transactions to {len(filtered_df)} for period '{period}'")
    return filtered_df


def regenerate_formatted_report() -> bool:
    """
    Regenerate the Formatted_Report sheet from Data_Log data.
    
    This function reads all data from Data_Log, clears the Formatted_Report sheet,
    and rebuilds it with proper daily grouping and formatting. It will also
    ensure the Data_Log sheet has the correct headers if they are missing.
    
    Returns:
        bool: True if successful, False otherwise
        
    Raises:
        Exception: If regeneration operation fails
    """
    try:
        data_sheet_name = get_data_log_sheet_name()
        report_sheet_name = get_formatted_report_sheet_name()
        
        logger.info("Starting formatted report regeneration...")
        
        # Get all values from the sheet to inspect the structure
        all_values = api.get_all_values(data_sheet_name)
        
        # Check if sheet is completely empty
        if not all_values:
            logger.info("Data_Log sheet is completely empty. Initializing with headers...")
            api.append_row(data_sheet_name, DATA_LOG_COLUMNS)
            return _create_empty_report(report_sheet_name)
        
        # Check if the first row matches our expected headers exactly
        if all_values[0] != DATA_LOG_COLUMNS:
            logger.warning(f"Headers mismatch. Expected: {DATA_LOG_COLUMNS}")
            logger.warning(f"Found: {all_values[0] if all_values else 'None'}")
            
            # Clear the sheet and set correct headers
            logger.info("Clearing sheet and setting correct headers...")
            api.clear_worksheet(data_sheet_name)
            api.append_row(data_sheet_name, DATA_LOG_COLUMNS)
            return _create_empty_report(report_sheet_name)
        
        # Get records using the API
        all_records = api.get_all_records(data_sheet_name)
        
        if not all_records:
            logger.info("No data records found (only headers exist), creating empty report")
            return _create_empty_report(report_sheet_name)
        
        # Check if we got the header row as data (this happens when get_all_records fails)
        if len(all_records) == 1 and all_records[0].get('timestamp') == 'timestamp':
            logger.warning("Detected header row returned as data. No actual transactions exist.")
            return _create_empty_report(report_sheet_name)
        
        # Convert to DataFrame
        df = pd.DataFrame(all_records)
        
        # Log DataFrame info for debugging
        logger.debug(f"DataFrame shape: {df.shape}")
        logger.debug(f"DataFrame columns: {list(df.columns)}")
        
        # Check for required columns
        required_columns = ['timestamp', 'transaction_type', 'category_or_source', 'description', 'amount']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            logger.error(f"Available columns: {list(df.columns)}")
            # Try to create empty report instead of failing
            logger.warning("Creating empty report due to column mismatch")
            return _create_empty_report(report_sheet_name)
        
        # Filter out any rows that are actually headers
        df = df[df['timestamp'] != 'timestamp']
        
        if df.empty:
            logger.info("No valid transaction data found after filtering, creating empty report")
            return _create_empty_report(report_sheet_name)
        
        # Convert data types
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        except Exception as e:
            logger.error(f"Error converting timestamp: {str(e)}")
            logger.error(f"Sample timestamp values: {df['timestamp'].head().tolist()}")
            return _create_empty_report(report_sheet_name)
        
        try:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
            # Remove rows with invalid amounts
            invalid_amounts = df['amount'].isna().sum()
            if invalid_amounts > 0:
                logger.warning(f"Removing {invalid_amounts} rows with invalid amounts")
                df = df.dropna(subset=['amount'])
        except Exception as e:
            logger.error(f"Error converting amount: {str(e)}")
            return _create_empty_report(report_sheet_name)
        
        if df.empty:
            logger.info("No valid data remaining after type conversion, creating empty report")
            return _create_empty_report(report_sheet_name)
        
        # Sort by timestamp (newest first)
        df = df.sort_values('timestamp', ascending=False)
        
        # Generate formatted report content
        report_content = _build_formatted_report_content(df)
        
        # Clear and update the Formatted_Report sheet
        api.clear_worksheet(report_sheet_name)
        
        if report_content:
            api.update_range(report_sheet_name, 'A1', report_content)
        
        logger.info(f"Successfully regenerated formatted report with {len(df)} transactions")
        return True
        
    except Exception as e:
        logger.error(f"Failed to regenerate formatted report: {str(e)}")
        # Try to create empty report as fallback
        try:
            return _create_empty_report(get_formatted_report_sheet_name())
        except:
            raise Exception(f"Failed to regenerate formatted report: {str(e)}")


def fix_data_log_headers() -> bool:
    """
    Fix the Data_Log sheet headers by clearing the sheet and setting correct headers.
    This is a utility function to repair the sheet structure.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        data_sheet_name = get_data_log_sheet_name()
        
        logger.info("Fixing Data_Log sheet headers...")
        
        # Get current values to backup any real data
        all_values = api.get_all_values(data_sheet_name)
        
        if not all_values:
            logger.info("Sheet is empty, just adding headers")
            api.append_row(data_sheet_name, DATA_LOG_COLUMNS)
            return True
        
        # Check if there's any real transaction data to preserve
        real_data = []
        for i, row in enumerate(all_values):
            if i == 0:  # Skip header row
                continue
            
            # Skip rows that are clearly not transaction data
            if (len(row) >= 6 and 
                row[0] not in ['timestamp', '', None] and
                row[1] in ['income', 'expense'] and
                row[4] not in ['amount', '', None]):  # amount column check
                real_data.append(row)
        
        # Clear the sheet
        api.clear_worksheet(data_sheet_name)
        
        # Add correct headers
        api.append_row(data_sheet_name, DATA_LOG_COLUMNS)
        
        # Restore any real data
        if real_data:
            logger.info(f"Preserving {len(real_data)} existing transaction records")
            for row in real_data:
                # Ensure the row has exactly the right number of columns
                padded_row = (row + [''] * len(DATA_LOG_COLUMNS))[:len(DATA_LOG_COLUMNS)]
                api.append_row(data_sheet_name, padded_row)
        
        logger.info("Successfully fixed Data_Log sheet headers")
        return True
        
    except Exception as e:
        logger.error(f"Failed to fix Data_Log headers: {str(e)}")
        return False

# Add this diagnostic function to help troubleshoot
def diagnose_sheet_structure() -> Dict[str, Any]:
    """
    Diagnose the current sheet structure for debugging.
    
    Returns:
        Dict: Information about the sheet structure
    """
    try:
        data_sheet_name = get_data_log_sheet_name()
        
        # Get raw values
        all_values = api.get_all_values(data_sheet_name)
        
        # Get records
        all_records = api.get_all_records(data_sheet_name)
        
        diagnosis = {
            'sheet_name': data_sheet_name,
            'total_rows': len(all_values),
            'expected_headers': DATA_LOG_COLUMNS,
            'actual_first_row': all_values[0] if all_values else None,
            'headers_match': all_values[0] == DATA_LOG_COLUMNS if all_values else False,
            'records_count': len(all_records),
            'sample_record': all_records[0] if all_records else None,
            'all_columns': list(all_records[0].keys()) if all_records else []
        }
        
        logger.info(f"Sheet diagnosis: {diagnosis}")
        return diagnosis
        
    except Exception as e:
        logger.error(f"Failed to diagnose sheet: {str(e)}")
        return {'error': str(e)}
    

def _build_formatted_report_content(df: pd.DataFrame) -> List[List[str]]:
    """
    Build the content for the formatted report with daily grouping.
    
    Args:
        df (pd.DataFrame): Transaction data
        
    Returns:
        List[List[str]]: 2D list ready for sheet update
    """
    content = []
    
    # Add header
    content.append([
        "💰 MESSENGER WALLET BOT - TRANSACTION REPORT",
        "",
        "",
        "",
        ""
    ])
    content.append([
        f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "",
        "",
        ""
    ])
    content.append(["", "", "", "", ""])  # Empty row
    
    # Add column headers
    content.append([
        "Date",
        "Type",
        "Category/Source",
        "Description",
        "Amount (₱)"
    ])
    content.append(["", "", "", "", ""])  # Separator row
    
    # Group transactions by date
    df['date'] = df['timestamp'].dt.date
    grouped = df.groupby('date')
    
    total_income = 0
    total_expenses = 0
    
    for date_group, transactions in grouped:
        # Add date header
        content.append([
            f"📅 {date_group.strftime('%A, %B %d, %Y')}",
            "",
            "",
            "",
            ""
        ])
        
        # Sort transactions for this date by time
        daily_transactions = transactions.sort_values('timestamp')
        daily_income = 0
        daily_expenses = 0
        
        # Add each transaction
        for _, transaction in daily_transactions.iterrows():
            transaction_type = transaction['transaction_type']
            amount = float(transaction['amount'])
            
            # Format amount with currency
            formatted_amount = f"₱{amount:,.2f}"
            
            # Add appropriate emoji and tracking
            if transaction_type == 'income':
                emoji = "💰"
                daily_income += amount
                total_income += amount
            else:
                emoji = "💸"
                daily_expenses += amount
                total_expenses += amount
            
            content.append([
                f"  {transaction['timestamp'].strftime('%H:%M')}",
                f"{emoji} {transaction_type.title()}",
                transaction['category_or_source'],
                transaction['description'],
                formatted_amount
            ])
        
        # Add daily summary
        daily_net = daily_income - daily_expenses
        net_indicator = "📈" if daily_net >= 0 else "📉"
        
        content.append(["", "", "", "", ""])  # Empty row
        content.append([
            f"    Daily Summary:",
            f"Income: ₱{daily_income:,.2f}",
            f"Expenses: ₱{daily_expenses:,.2f}",
            f"{net_indicator} Net: ₱{daily_net:,.2f}",
            ""
        ])
        content.append(["", "", "", "", ""])  # Separator
    
    # Add overall summary
    overall_net = total_income - total_expenses
    net_status = "Surplus 📈" if overall_net >= 0 else "Deficit 📉"
    
    content.append(["═" * 50, "", "", "", ""])
    content.append([
        "📊 OVERALL SUMMARY",
        "",
        "",
        "",
        ""
    ])
    content.append([
        f"💰 Total Income:",
        f"₱{total_income:,.2f}",
        "",
        "",
        ""
    ])
    content.append([
        f"💸 Total Expenses:",
        f"₱{total_expenses:,.2f}",
        "",
        "",
        ""
    ])
    content.append([
        f"📊 Net Amount:",
        f"₱{overall_net:,.2f}",
        f"({net_status})",
        "",
        ""
    ])
    
    # Add financial insights
    if total_expenses > 0:
        savings_rate = (total_income - total_expenses) / total_income * 100 if total_income > 0 else 0
        content.append(["", "", "", "", ""])
        content.append([
            f"📈 Savings Rate:",
            f"{savings_rate:.1f}%",
            "",
            "",
            ""
        ])
        
        # Calculate 10% tithe if there's income
        if total_income > 0:
            tithe_amount = total_income * 0.1
            content.append([
                f"🙏 Suggested Tithe (10%):",
                f"₱{tithe_amount:,.2f}",
                "",
                "",
                ""
            ])
    
    return content


def _create_empty_report(sheet_name: str) -> bool:
    """
    Create an empty formatted report when no data exists.
    
    Args:
        sheet_name (str): Name of the report sheet
        
    Returns:
        bool: True if successful
    """
    try:
        empty_content = [
            ["💰 MESSENGER WALLET BOT - TRANSACTION REPORT", "", "", "", ""],
            [f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "", "", "", ""],
            ["", "", "", "", ""],
            ["📝 No transactions recorded yet.", "", "", "", ""],
            ["", "", "", "", ""],
            ["💡 Start logging your income and expenses by chatting with the bot!", "", "", "", ""]
        ]
        
        api.update_range(sheet_name, 'A1', empty_content)
        logger.info("Created empty formatted report")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create empty report: {str(e)}")
        return False


def get_transaction_count() -> Dict[str, int]:
    """
    Get the total count of transactions by type.
    
    Returns:
        Dict[str, int]: Dictionary with counts for 'total', 'income', and 'expense'
        
    Raises:
        Exception: If operation fails
    """
    try:
        sheet_name = get_data_log_sheet_name()
        all_records = api.get_all_records(sheet_name)
        
        total_count = len(all_records)
        income_count = len([r for r in all_records if r.get('transaction_type') == 'income'])
        expense_count = len([r for r in all_records if r.get('transaction_type') == 'expense'])
        
        counts = {
            'total': total_count,
            'income': income_count,
            'expense': expense_count
        }
        
        logger.info(f"Transaction counts: {counts}")
        return counts
        
    except Exception as e:
        logger.error(f"Failed to get transaction count: {str(e)}")
        raise Exception(f"Failed to get transaction count: {str(e)}")


def initialize_sheets() -> bool:
    """
    Initialize both Data_Log and Formatted_Report sheets with proper headers.
    
    Returns:
        bool: True if successful, False otherwise
        
    Raises:
        Exception: If initialization fails
    """
    try:
        data_sheet = get_data_log_sheet_name()
        report_sheet = get_formatted_report_sheet_name()
        
        logger.info("Initializing sheets...")
        
        # Initialize Data_Log sheet with headers
        try:
            # Check if Data_Log already has data
            existing_data = api.get_all_values(data_sheet)
            if not existing_data or len(existing_data) == 0:
                # Add headers to empty Data_Log sheet
                api.append_row(data_sheet, DATA_LOG_COLUMNS)
                logger.info("Initialized Data_Log sheet with headers")
        except Exception as e:
            logger.warning(f"Could not initialize Data_Log headers: {str(e)}")
        
        # Initialize Formatted_Report sheet with welcome message
        _create_empty_report(report_sheet)
        
        logger.info("Successfully initialized both sheets")
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize sheets: {str(e)}")
        raise Exception(f"Failed to initialize sheets: {str(e)}")


def test_sheets_connection() -> bool:
    """
    Test the connection to both sheets.
    
    Returns:
        bool: True if both sheets are accessible, False otherwise
    """
    try:
        data_sheet = get_data_log_sheet_name()
        report_sheet = get_formatted_report_sheet_name()
        
        # Test access to both sheets
        data_info = api.get_worksheet_info(data_sheet)
        report_info = api.get_worksheet_info(report_sheet)
        
        logger.info(f"Successfully accessed Data_Log: {data_info['title']}")
        logger.info(f"Successfully accessed Formatted_Report: {report_info['title']}")
        
        return True
        
    except Exception as e:
        logger.error(f"Sheets connection test failed: {str(e)}")
        return False


def backup_data() -> List[Dict[str, Any]]:
    """
    Create a backup of all transaction data.
    
    Returns:
        List[Dict[str, Any]]: All transaction records
        
    Raises:
        Exception: If backup operation fails
    """
    try:
        sheet_name = get_data_log_sheet_name()
        all_records = api.get_all_records(sheet_name)
        
        logger.info(f"Created backup of {len(all_records)} transactions")
        return all_records
        
    except Exception as e:
        logger.error(f"Failed to create data backup: {str(e)}")
        raise Exception(f"Failed to create data backup: {str(e)}")


def debug_amount_conversion(user_id: str):
    """
    Debug function to troubleshoot amount conversion issues.
    """
    try:
        logger.info("=== DEBUGGING AMOUNT CONVERSION ===")
        
        # Get transactions for the user
        transactions = get_transactions_for_period("This Week", user_id)
        logger.info(f"Retrieved {len(transactions)} transactions")
        
        if not transactions:
            logger.info("No transactions found")
            return
        
        # Show raw transaction data
        for i, transaction in enumerate(transactions):
            logger.info(f"Transaction {i+1}:")
            logger.info(f"  Type: {transaction.get('transaction_type')} ({type(transaction.get('transaction_type'))})")
            logger.info(f"  Amount: {transaction.get('amount')} ({type(transaction.get('amount'))})")
            logger.info(f"  Category: {transaction.get('category_or_source')}")
            logger.info(f"  Description: {transaction.get('description')}")
            
            # Test amount conversion
            amount_raw = transaction.get('amount')
            try:
                amount_converted = float(str(amount_raw).replace('₱', '').replace(',', '').strip())
                logger.info(f"  Converted amount: {amount_converted}")
            except Exception as e:
                logger.error(f"  Conversion failed: {str(e)}")
        
        # Test DataFrame conversion
        df = pd.DataFrame(transactions)
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"Amount column dtype: {df['amount'].dtype}")
        logger.info(f"Amount column values: {df['amount'].tolist()}")
        
        # Test numeric conversion
        df['amount_numeric'] = pd.to_numeric(df['amount'], errors='coerce')
        logger.info(f"After pd.to_numeric: {df['amount_numeric'].tolist()}")
        
        # Manual conversion test
        def manual_convert(val):
            if isinstance(val, (int, float)):
                return float(val)
            return float(str(val).replace('₱', '').replace(',', '').strip())
        
        df['amount_manual'] = df['amount'].apply(manual_convert)
        logger.info(f"After manual conversion: {df['amount_manual'].tolist()}")
        
        # Calculate sums
        income_sum = df[df['transaction_type'] == 'income']['amount_manual'].sum()
        expense_sum = df[df['transaction_type'] == 'expense']['amount_manual'].sum()
        logger.info(f"Income sum: {income_sum}")
        logger.info(f"Expense sum: {expense_sum}")
        
        logger.info("=== END DEBUG ===")
        
    except Exception as e:
        logger.error(f"Debug function failed: {str(e)}")


def analyze_financial_data(transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze financial data from transactions with improved data type handling.
    
    Args:
        transactions (List[Dict[str, Any]]): List of transaction dictionaries
        
    Returns:
        Dict[str, Any]: Analysis results with financial metrics
    """
    try:
        if not transactions:
            logger.info("No transactions to analyze")
            return {
                'total_income': 0,
                'total_expenses': 0,
                'net_savings': 0,
                'transaction_count': 0,
                'expense_categories': {},
                'income_sources': {},
                'insights': ["No transactions recorded yet."]
            }
        
        logger.info(f"Analyzing {len(transactions)} transactions")
        
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame(transactions)
        
        # Debug: Print column info
        logger.info(f"DataFrame columns: {list(df.columns)}")
        logger.info(f"Amount column sample values: {df['amount'].head().tolist()}")
        logger.info(f"Amount column data types: {df['amount'].dtype}")
        
        # FIXED: More robust amount conversion
        # Handle different possible formats of amount data
        def convert_amount(amount):
            """Convert amount to float, handling various formats"""
            if isinstance(amount, (int, float)):
                return float(amount)
            if isinstance(amount, str):
                # Remove currency symbols and whitespace
                cleaned = str(amount).replace('₱', '').replace(',', '').strip()
                try:
                    return float(cleaned)
                except ValueError:
                    logger.warning(f"Could not convert amount: {amount}")
                    return 0.0
            return 0.0
        
        # Apply the conversion function
        df['amount_numeric'] = df['amount'].apply(convert_amount)
        
        # Debug: Check conversion results
        logger.info(f"Converted amounts: {df['amount_numeric'].tolist()}")
        logger.info(f"Amount numeric dtype: {df['amount_numeric'].dtype}")
        
        # Remove rows with zero amounts (failed conversions)
        original_count = len(df)
        df = df[df['amount_numeric'] > 0]
        if len(df) < original_count:
            logger.warning(f"Removed {original_count - len(df)} rows with invalid amounts")
        
        if df.empty:
            logger.warning("No valid transactions after amount conversion")
            return {
                'total_income': 0,
                'total_expenses': 0,
                'net_savings': 0,
                'transaction_count': 0,
                'expense_categories': {},
                'income_sources': {},
                'insights': ["All transaction amounts were invalid."]
            }
        
        # Separate income and expenses
        income_df = df[df['transaction_type'] == 'income']
        expense_df = df[df['transaction_type'] == 'expense']
        
        # Calculate totals using the numeric column
        total_income = float(income_df['amount_numeric'].sum()) if not income_df.empty else 0
        total_expenses = float(expense_df['amount_numeric'].sum()) if not expense_df.empty else 0
        net_savings = total_income - total_expenses
        
        # Debug: Print calculation results
        logger.info(f"Income transactions: {len(income_df)}")
        logger.info(f"Expense transactions: {len(expense_df)}")
        logger.info(f"Total income calculated: {total_income}")
        logger.info(f"Total expenses calculated: {total_expenses}")
        logger.info(f"Net savings: {net_savings}")
        
        # Analyze categories and sources
        expense_categories = {}
        if not expense_df.empty:
            expense_categories = expense_df.groupby('category_or_source')['amount_numeric'].sum().to_dict()
            # Convert to regular floats
            expense_categories = {k: float(v) for k, v in expense_categories.items()}
        
        income_sources = {}
        if not income_df.empty:
            income_sources = income_df.groupby('category_or_source')['amount_numeric'].sum().to_dict()
            # Convert to regular floats
            income_sources = {k: float(v) for k, v in income_sources.items()}
        
        # Generate insights
        insights = []
        
        if total_income > 0 and total_expenses > 0:
            savings_rate = (net_savings / total_income) * 100
            if savings_rate > 20:
                insights.append(f"Excellent! You're saving {savings_rate:.1f}% of your income.")
            elif savings_rate > 10:
                insights.append(f"Good job! You're saving {savings_rate:.1f}% of your income.")
            elif savings_rate > 0:
                insights.append(f"You're saving {savings_rate:.1f}% of your income. Try to increase this!")
            else:
                insights.append("You're spending more than you earn. Consider reducing expenses.")
        elif total_income > 0:
            insights.append("Great! You've logged income but no expenses yet.")
        elif total_expenses > 0:
            insights.append("You've logged expenses but no income yet. Don't forget to track your earnings!")
        else:
            insights.append("Start tracking both income and expenses to get valuable insights!")
        
        # Category insights
        if expense_categories:
            top_category = max(expense_categories.items(), key=lambda x: x[1])
            insights.append(f"Your biggest expense category is {top_category[0]} (₱{top_category[1]:,.2f}).")
        
        if total_income > 0:
            tithe_suggestion = total_income * 0.1
            insights.append(f"Consider setting aside ₱{tithe_suggestion:,.2f} (10%) for tithing or donations.")
        
        # Format the results
        result = {
            'total_income': total_income,
            'total_expenses': total_expenses,
            'net_savings': net_savings,
            'transaction_count': len(df),
            'expense_categories': expense_categories,
            'income_sources': income_sources,
            'insights': insights
        }
        
        logger.info(f"Analysis complete: Income=₱{total_income:,.2f}, Expenses=₱{total_expenses:,.2f}, Net=₱{net_savings:,.2f}")
        return result
        
    except Exception as e:
        logger.error(f"Failed to analyze financial data: {str(e)}")
        return {
            'total_income': 0,
            'total_expenses': 0,
            'net_savings': 0,
            'transaction_count': 0,
            'expense_categories': {},
            'income_sources': {},
            'insights': [f"Error analyzing data: {str(e)}"]
        }