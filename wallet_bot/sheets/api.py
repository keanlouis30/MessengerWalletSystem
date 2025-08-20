"""
Low-level Google Sheets API connector for the Messenger Wallet Bot.

This module handles direct interactions with the Google Sheets API using the gspread library.
It provides fundamental I/O operations like authentication, reading, writing, and clearing worksheets.
This is an internal module that should only be called by wallet_bot.sheets.handler.py.
"""

import gspread
import logging
from typing import List, Dict, Any
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound

# --- THIS IS THE CRITICAL FIX ---
# We import the 'config' object and the 'get_google_sheet_id' function.
# We will use the 'config' object to call the get_credentials_path method.
from wallet_bot.config.settings import config, get_google_sheet_id
# -----------------------------

# Set up logging
logger = logging.getLogger(__name__)

# Global variables for connection management
_gc = None
_spreadsheet = None


def _authenticate() -> gspread.Client:
    """
    Authenticate with Google Sheets API using service account credentials.
    
    Returns:
        gspread.Client: Authenticated client instance
        
    Raises:
        Exception: If authentication fails or credentials file is not found
    """
    global _gc
    
    if _gc is not None:
        return _gc
    
    try:
        # --- THIS IS THE SECOND PART OF THE FIX ---
        # We call get_credentials_path() as a method of the imported 'config' object.
        credentials_path = config.get_credentials_path()
        # ----------------------------------------
        
        # Define the scope for Google Sheets and Drive APIs
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # Load credentials from the service account file
        credentials = Credentials.from_service_account_file(
            credentials_path, 
            scopes=scope
        )
        
        # Authorize and create client
        _gc = gspread.authorize(credentials)
        logger.info("Successfully authenticated with Google Sheets API")
        return _gc
        
    except FileNotFoundError:
        logger.error(f"Credentials file not found at: {config.get_credentials_path()}")
        raise Exception("Google Service Account credentials file not found")
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}")
        raise Exception(f"Failed to authenticate with Google Sheets API: {str(e)}")


def _get_spreadsheet() -> gspread.Spreadsheet:
    """
    Get the Google Spreadsheet instance.
    
    Returns:
        gspread.Spreadsheet: The spreadsheet instance
        
    Raises:
        Exception: If spreadsheet cannot be opened
    """
    global _spreadsheet
    
    if _spreadsheet is not None:
        return _spreadsheet
    
    try:
        gc = _authenticate()
        sheet_id = get_google_sheet_id()
        _spreadsheet = gc.open_by_key(sheet_id)
        logger.info(f"Successfully opened spreadsheet: {_spreadsheet.title}")
        return _spreadsheet
        
    except SpreadsheetNotFound:
        logger.error(f"Spreadsheet not found with ID: {get_google_sheet_id()}")
        raise Exception("Google Spreadsheet not found or not accessible")
    except Exception as e:
        logger.error(f"Failed to open spreadsheet: {str(e)}")
        raise Exception(f"Failed to access Google Spreadsheet: {str(e)}")


def get_worksheet(sheet_name: str) -> gspread.Worksheet:
    """
    Get a specific worksheet by name.
    
    Args:
        sheet_name (str): Name of the worksheet to retrieve
        
    Returns:
        gspread.Worksheet: The worksheet instance
        
    Raises:
        Exception: If worksheet cannot be found or accessed
    """
    try:
        spreadsheet = _get_spreadsheet()
        worksheet = spreadsheet.worksheet(sheet_name)
        logger.debug(f"Successfully accessed worksheet: {sheet_name}")
        return worksheet
        
    except WorksheetNotFound:
        logger.error(f"Worksheet '{sheet_name}' not found")
        raise Exception(f"Worksheet '{sheet_name}' not found in spreadsheet")
    except Exception as e:
        logger.error(f"Failed to access worksheet '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to access worksheet '{sheet_name}': {str(e)}")


def append_row(sheet_name: str, row_data: List[Any]) -> bool:
    """
    Append a single row to the specified worksheet.
    
    Args:
        sheet_name (str): Name of the worksheet
        row_data (List[Any]): List of values to append as a new row
        
    Returns:
        bool: True if successful, False otherwise
        
    Raises:
        Exception: If append operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name)
        
        # Convert all values to strings to ensure compatibility
        formatted_row = [str(value) if value is not None else "" for value in row_data]
        
        worksheet.append_row(formatted_row)
        logger.info(f"Successfully appended row to '{sheet_name}': {len(formatted_row)} columns")
        return True
        
    except APIError as e:
        logger.error(f"API error when appending row to '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to append row due to API error: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to append row to '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to append row to worksheet: {str(e)}")


# (The rest of your functions: get_all_records, get_all_values, clear_worksheet, etc., can remain exactly as they were)
# I am omitting them here for brevity, but you should keep them in your file.
# The only changes required are at the very top of the file.
def get_all_records(sheet_name: str) -> List[Dict[str, Any]]:
    """
    Get all records from a worksheet as a list of dictionaries.
    
    Args:
        sheet_name (str): Name of the worksheet
        
    Returns:
        List[Dict[str, Any]]: List of records where each record is a dictionary
                              with column headers as keys
        
    Raises:
        Exception: If reading operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name)
        
        # Get all records as dictionaries
        records = worksheet.get_all_records()
        logger.info(f"Successfully retrieved {len(records)} records from '{sheet_name}'")
        return records
        
    except APIError as e:
        logger.error(f"API error when reading '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to read records due to API error: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to read records from '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to read records from worksheet: {str(e)}")


def get_all_values(sheet_name: str) -> List[List[str]]:
    """
    Get all values from a worksheet as a list of lists.
    
    Args:
        sheet_name (str): Name of the worksheet
        
    Returns:
        List[List[str]]: All values in the worksheet, including headers
        
    Raises:
        Exception: If reading operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name)
        
        # Get all values including headers
        values = worksheet.get_all_values()
        logger.info(f"Successfully retrieved {len(values)} rows from '{sheet_name}'")
        return values
        
    except APIError as e:
        logger.error(f"API error when reading values from '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to read values due to API error: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to read values from '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to read values from worksheet: {str(e)}")


def clear_worksheet(sheet_name: str) -> bool:
    """
    Clear all content from a worksheet.
    
    Args:
        sheet_name (str): Name of the worksheet to clear
        
    Returns:
        bool: True if successful, False otherwise
        
    Raises:
        Exception: If clear operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name)
        
        # Clear all content
        worksheet.clear()
        logger.info(f"Successfully cleared worksheet: '{sheet_name}'")
        return True
        
    except APIError as e:
        logger.error(f"API error when clearing '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to clear worksheet due to API error: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to clear worksheet '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to clear worksheet: {str(e)}")


def update_range(sheet_name: str, range_name: str, values: List[List[Any]]) -> bool:
    """
    Update a specific range in the worksheet with new values.
    
    Args:
        sheet_name (str): Name of the worksheet
        range_name (str): Range to update (e.g., 'A1:C3')
        values (List[List[Any]]): 2D list of values to update
        
    Returns:
        bool: True if successful, False otherwise
        
    Raises:
        Exception: If update operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name)
        
        # Convert all values to strings
        formatted_values = [
            [str(cell) if cell is not None else "" for cell in row]
            for row in values
        ]
        
        worksheet.update(range_name, formatted_values)
        logger.info(f"Successfully updated range '{range_name}' in '{sheet_name}'")
        return True
        
    except APIError as e:
        logger.error(f"API error when updating range in '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to update range due to API error: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to update range in '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to update range in worksheet: {str(e)}")


def batch_update(sheet_name: str, updates: List[Dict[str, Any]]) -> bool:
    """
    Perform multiple updates to a worksheet in a single API call.
    
    Args:
        sheet_name (str): Name of the worksheet
        updates (List[Dict[str, Any]]): List of update operations
                                       Each dict should have 'range' and 'values' keys
        
    Returns:
        bool: True if successful, False otherwise
        
    Raises:
        Exception: If batch update operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name)
        
        # Format updates for batch operation
        formatted_updates = []
        for update in updates:
            formatted_values = [
                [str(cell) if cell is not None else "" for cell in row]
                for row in update['values']
            ]
            formatted_updates.append({
                'range': update['range'],
                'values': formatted_values
            })
        
        worksheet.batch_update(formatted_updates)
        logger.info(f"Successfully performed {len(updates)} batch updates to '{sheet_name}'")
        return True
        
    except APIError as e:
        logger.error(f"API error during batch update to '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to perform batch update due to API error: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to perform batch update to '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to perform batch update: {str(e)}")


def get_worksheet_info(sheet_name: str) -> Dict[str, Any]:
    """
    Get information about a worksheet (row count, column count, etc.).
    
    Args:
        sheet_name (str): Name of the worksheet
        
    Returns:
        Dict[str, Any]: Dictionary containing worksheet information
        
    Raises:
        Exception: If operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name)
        
        info = {
            'title': worksheet.title,
            'row_count': worksheet.row_count,
            'col_count': worksheet.col_count,
            'id': worksheet.id,
            'url': worksheet.url
        }
        
        logger.debug(f"Retrieved info for worksheet '{sheet_name}': {info}")
        return info
        
    except Exception as e:
        logger.error(f"Failed to get info for worksheet '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to get worksheet information: {str(e)}")


# Connection management functions
def reset_connection():
    """
    Reset the global connection variables.
    Useful for testing or when authentication needs to be refreshed.
    """
    global _gc, _spreadsheet
    _gc = None
    _spreadsheet = None
    logger.info("Reset Google Sheets API connection")


def test_connection() -> bool:
    """
    Test the connection to Google Sheets API.
    
    Returns:
        bool: True if connection is working, False otherwise
    """
    try:
        spreadsheet = _get_spreadsheet()
        # Try to get basic spreadsheet info
        _ = spreadsheet.title
        logger.info("Google Sheets API connection test successful")
        return True
        
    except Exception as e:
        logger.error(f"Google Sheets API connection test failed: {str(e)}")
        return False