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
        
        # Create timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
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
        
        if not all_records:
            logger.info("No transactions found in Data_Log sheet")
            return []
        
        # Filter by user_id if specified
        if user_id:
            all_records = [record for record in all_records if record.get('user_id') == user_id]
        
        # Convert to DataFrame for easier date filtering
        df = pd.DataFrame(all_records)
        
        if df.empty:
            logger.info(f"No transactions found for user: {user_id}")
            return []
        
        # Ensure timestamp column exists and convert to datetime
        if 'timestamp' not in df.columns:
            logger.warning("No timestamp column found in Data_Log")
            return all_records
        
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        except Exception as e:
            logger.warning(f"Could not parse timestamps: {str(e)}")
            return all_records
        
        # Filter by period
        filtered_df = _filter_transactions_by_period(df, period)
        
        # Convert back to list of dictionaries
        transactions = filtered_df.to_dict('records')
        
        logger.info(f"Retrieved {len(transactions)} transactions for period '{period}'")
        return transactions
        
    except Exception as e:
        logger.error(f"Failed to get transactions for period '{period}': {str(e)}")
        raise Exception(f"Failed to retrieve transactions: {str(e)}")


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
    and rebuilds it with proper daily grouping and formatting.
    
    Returns:
        bool: True if successful, False otherwise
        
    Raises:
        Exception: If regeneration operation fails
    """
    try:
        data_sheet = get_data_log_sheet_name()
        report_sheet = get_formatted_report_sheet_name()
        
        logger.info("Starting formatted report regeneration...")
        
        # Get all transactions from Data_Log
        all_records = api.get_all_records(data_sheet)
        
        if not all_records:
            logger.info("No data found in Data_Log, creating empty report")
            return _create_empty_report(report_sheet)
        
        # Convert to DataFrame for easier processing
        df = pd.DataFrame(all_records)
        
        # Ensure required columns exist
        required_columns = ['timestamp', 'transaction_type', 'category_or_source', 'description', 'amount']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in Data_Log: {missing_columns}")
        
        # Convert timestamp and amount columns
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        
        # Sort by timestamp (newest first)
        df = df.sort_values('timestamp', ascending=False)
        
        # Generate formatted report content
        report_content = _build_formatted_report_content(df)
        
        # Clear and update the Formatted_Report sheet
        api.clear_worksheet(report_sheet)
        
        if report_content:
            # Update the sheet with formatted content
            api.update_range(report_sheet, 'A1', report_content)
        
        logger.info("Successfully regenerated formatted report")
        return True
        
    except Exception as e:
        logger.error(f"Failed to regenerate formatted report: {str(e)}")
        raise Exception(f"Failed to regenerate formatted report: {str(e)}")


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