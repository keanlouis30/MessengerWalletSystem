import gspread
import logging
import os
import json
from typing import List, Dict, Any, Optional
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

# Default worksheet configurations
DEFAULT_WORKSHEET_CONFIGS = {
    'Data_Log': {
        'rows': 1000,
        'cols': 20,
        'headers': [
            'Timestamp', 'Type', 'Amount', 'Currency', 'Description', 
            'Category', 'Source', 'Destination', 'Status', 'Reference',
            'User_ID', 'Session_ID', 'IP_Address', 'User_Agent', 'Notes',
            'Created_At', 'Updated_At', 'Version', 'Hash', 'Extra_Data'
        ]
    },
    'Formatted_Report': {
        'rows': 500,
        'cols': 15,
        'headers': [
            'Date', 'Transaction_Type', 'Amount', 'Currency', 'Description',
            'Category', 'Balance_Before', 'Balance_After', 'Status', 'Reference_ID',
            'User', 'Location', 'Method', 'Fees', 'Notes'
        ]
    },
    'Summary': {
        'rows': 100,
        'cols': 10,
        'headers': [
            'Period', 'Total_Income', 'Total_Expenses', 'Net_Amount', 'Transaction_Count',
            'Average_Transaction', 'Largest_Transaction', 'Categories_Used', 'Active_Users', 'Last_Updated'
        ]
    }
}


def _authenticate() -> gspread.Client:
    """
    Authenticate with Google Sheets API using service account credentials.
    Supports both environment variable and file-based authentication.
    
    Returns:
        gspread.Client: Authenticated client instance
        
    Raises:
        Exception: If authentication fails or credentials are not found
    """
    global _gc
    
    if _gc is not None:
        return _gc
    
    try:
        # Define the scope for Google Sheets and Drive APIs
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # Method 1: Try to get credentials from environment variable first
        credentials_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
        if credentials_json:
            logger.info("Using Google credentials from environment variable")
            try:
                credentials_info = json.loads(credentials_json)
                credentials = Credentials.from_service_account_info(
                    credentials_info, 
                    scopes=scope
                )
                _gc = gspread.authorize(credentials)
                logger.info("Successfully authenticated with Google Sheets API using environment variable")
                return _gc
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in GOOGLE_CREDENTIALS_JSON environment variable: {e}")
                # Fall through to file-based method
            except Exception as e:
                logger.error(f"Failed to authenticate using environment variable: {e}")
                # Fall through to file-based method
        
        # Method 2: Try file-based authentication
        logger.info("Attempting file-based authentication")
        credentials_path = config.get_credentials_path()
        logger.info(f"Looking for credentials file at: {credentials_path}")
        
        # Check if the file exists
        if not os.path.exists(credentials_path):
            # Try alternative paths for Render deployment
            alternative_paths = [
                '/opt/render/project/credentials.json',
                '/opt/render/project/src/credentials.json',
                os.path.join(os.getcwd(), 'credentials.json'),
                'credentials.json'
            ]
            
            logger.info(f"File not found at {credentials_path}, trying alternative paths...")
            credentials_path = None
            
            for alt_path in alternative_paths:
                logger.info(f"Checking: {alt_path}")
                if os.path.exists(alt_path):
                    credentials_path = alt_path
                    logger.info(f"Found credentials file at: {alt_path}")
                    break
            
            if not credentials_path:
                raise FileNotFoundError(f"Credentials file not found. Tried: {[config.get_credentials_path()] + alternative_paths}")
        
        # Load credentials from the service account file
        credentials = Credentials.from_service_account_file(
            credentials_path, 
            scopes=scope
        )
        
        # Authorize and create client
        _gc = gspread.authorize(credentials)
        logger.info(f"Successfully authenticated with Google Sheets API using file: {credentials_path}")
        return _gc
        
    except FileNotFoundError as e:
        logger.error(f"Credentials file not found: {str(e)}")
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


def create_worksheet(sheet_name: str, rows: int = 1000, cols: int = 26, headers: Optional[List[str]] = None) -> gspread.Worksheet:
    """
    Create a new worksheet with the specified parameters.
    
    Args:
        sheet_name (str): Name of the worksheet to create
        rows (int): Number of rows for the worksheet
        cols (int): Number of columns for the worksheet
        headers (Optional[List[str]]): Headers to add to the first row
        
    Returns:
        gspread.Worksheet: The newly created worksheet
        
    Raises:
        Exception: If worksheet creation fails
    """
    try:
        spreadsheet = _get_spreadsheet()
        
        # Create the worksheet
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=rows, cols=cols)
        logger.info(f"Successfully created worksheet: '{sheet_name}' with {rows} rows and {cols} columns")
        
        # Add headers if provided
        if headers:
            try:
                # Convert headers to strings and pad with empty strings if needed
                formatted_headers = [str(header) for header in headers]
                if len(formatted_headers) > cols:
                    logger.warning(f"Headers ({len(formatted_headers)}) exceed columns ({cols}). Truncating.")
                    formatted_headers = formatted_headers[:cols]
                elif len(formatted_headers) < cols:
                    formatted_headers.extend([''] * (cols - len(formatted_headers)))
                
                worksheet.update('1:1', [formatted_headers])
                logger.info(f"Successfully added {len(headers)} headers to worksheet '{sheet_name}'")
            except Exception as e:
                logger.warning(f"Failed to add headers to worksheet '{sheet_name}': {str(e)}")
        
        return worksheet
        
    except APIError as e:
        logger.error(f"API error when creating worksheet '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to create worksheet due to API error: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to create worksheet '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to create worksheet: {str(e)}")


def get_worksheet(sheet_name: str, auto_create: bool = True) -> gspread.Worksheet:
    """
    Get a specific worksheet by name. Creates it if it doesn't exist and auto_create is True.
    
    Args:
        sheet_name (str): Name of the worksheet to retrieve
        auto_create (bool): Whether to create the worksheet if it doesn't exist
        
    Returns:
        gspread.Worksheet: The worksheet instance
        
    Raises:
        Exception: If worksheet cannot be found or accessed
    """
    try:
        spreadsheet = _get_spreadsheet()
        worksheet = spreadsheet.worksheet(sheet_name)
        logger.debug(f"Successfully accessed existing worksheet: {sheet_name}")
        return worksheet
        
    except WorksheetNotFound:
        if auto_create:
            logger.info(f"Worksheet '{sheet_name}' not found. Creating it...")
            
            # Use default configuration if available
            if sheet_name in DEFAULT_WORKSHEET_CONFIGS:
                config = DEFAULT_WORKSHEET_CONFIGS[sheet_name]
                worksheet = create_worksheet(
                    sheet_name=sheet_name,
                    rows=config['rows'],
                    cols=config['cols'],
                    headers=config['headers']
                )
            else:
                # Create with default parameters
                worksheet = create_worksheet(sheet_name=sheet_name)
            
            logger.info(f"Successfully created and accessed worksheet: {sheet_name}")
            return worksheet
        else:
            logger.error(f"Worksheet '{sheet_name}' not found and auto_create is disabled")
            raise Exception(f"Worksheet '{sheet_name}' not found in spreadsheet")
            
    except Exception as e:
        logger.error(f"Failed to access worksheet '{sheet_name}': {str(e)}")
        raise Exception(f"Failed to access worksheet '{sheet_name}': {str(e)}")


def ensure_worksheet_exists(sheet_name: str) -> bool:
    """
    Ensure a worksheet exists, creating it if necessary.
    
    Args:
        sheet_name (str): Name of the worksheet
        
    Returns:
        bool: True if worksheet exists or was created successfully
    """
    try:
        get_worksheet(sheet_name, auto_create=True)
        return True
    except Exception as e:
        logger.error(f"Failed to ensure worksheet '{sheet_name}' exists: {str(e)}")
        return False


def initialize_default_worksheets() -> Dict[str, bool]:
    """
    Initialize all default worksheets defined in DEFAULT_WORKSHEET_CONFIGS.
    
    Returns:
        Dict[str, bool]: Dictionary showing success status for each worksheet
    """
    results = {}
    
    for sheet_name in DEFAULT_WORKSHEET_CONFIGS.keys():
        try:
            ensure_worksheet_exists(sheet_name)
            results[sheet_name] = True
            logger.info(f"Successfully initialized worksheet: {sheet_name}")
        except Exception as e:
            results[sheet_name] = False
            logger.error(f"Failed to initialize worksheet '{sheet_name}': {str(e)}")
    
    return results
    
def append_row(sheet_name: str, row_data: List[Any], auto_create: bool = True) -> bool:
    """
    Append a single row to the specified worksheet.
    
    Args:
        sheet_name (str): Name of the worksheet
        row_data (List[Any]): List of values to append as a new row
        auto_create (bool): Whether to create the worksheet if it doesn't exist
        
    Returns:
        bool: True if successful, False otherwise
        
    Raises:
        Exception: If append operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name, auto_create=auto_create)
        
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


def get_all_records(sheet_name: str, auto_create: bool = True) -> List[Dict[str, Any]]:
    """
    Get all records from a worksheet as a list of dictionaries.
    
    Args:
        sheet_name (str): Name of the worksheet
        auto_create (bool): Whether to create the worksheet if it doesn't exist
        
    Returns:
        List[Dict[str, Any]]: List of records where each record is a dictionary
                              with column headers as keys
        
    Raises:
        Exception: If reading operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name, auto_create=auto_create)
        
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


def get_all_values(sheet_name: str, auto_create: bool = True) -> List[List[str]]:
    """
    Get all values from a worksheet as a list of lists.
    
    Args:
        sheet_name (str): Name of the worksheet
        auto_create (bool): Whether to create the worksheet if it doesn't exist
        
    Returns:
        List[List[str]]: All values in the worksheet, including headers
        
    Raises:
        Exception: If reading operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name, auto_create=auto_create)
        
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


def clear_worksheet(sheet_name: str, auto_create: bool = True) -> bool:
    """
    Clear all content from a worksheet.
    
    Args:
        sheet_name (str): Name of the worksheet to clear
        auto_create (bool): Whether to create the worksheet if it doesn't exist
        
    Returns:
        bool: True if successful, False otherwise
        
    Raises:
        Exception: If clear operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name, auto_create=auto_create)
        
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


def update_range(sheet_name: str, range_name: str, values: List[List[Any]], auto_create: bool = True) -> bool:
    """
    Update a specific range in the worksheet with new values.
    
    Args:
        sheet_name (str): Name of the worksheet
        range_name (str): Range to update (e.g., 'A1:C3')
        values (List[List[Any]]): 2D list of values to update
        auto_create (bool): Whether to create the worksheet if it doesn't exist
        
    Returns:
        bool: True if successful, False otherwise
        
    Raises:
        Exception: If update operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name, auto_create=auto_create)
        
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


def batch_update(sheet_name: str, updates: List[Dict[str, Any]], auto_create: bool = True) -> bool:
    """
    Perform multiple updates to a worksheet in a single API call.
    
    Args:
        sheet_name (str): Name of the worksheet
        updates (List[Dict[str, Any]]): List of update operations
                                       Each dict should have 'range' and 'values' keys
        auto_create (bool): Whether to create the worksheet if it doesn't exist
        
    Returns:
        bool: True if successful, False otherwise
        
    Raises:
        Exception: If batch update operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name, auto_create=auto_create)
        
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


def get_worksheet_info(sheet_name: str, auto_create: bool = True) -> Dict[str, Any]:
    """
    Get information about a worksheet (row count, column count, etc.).
    
    Args:
        sheet_name (str): Name of the worksheet
        auto_create (bool): Whether to create the worksheet if it doesn't exist
        
    Returns:
        Dict[str, Any]: Dictionary containing worksheet information
        
    Raises:
        Exception: If operation fails
    """
    try:
        worksheet = get_worksheet(sheet_name, auto_create=auto_create)
        
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
    Test the connection to Google Sheets API and initialize default worksheets.
    
    Returns:
        bool: True if connection is working, False otherwise
    """
    try:
        spreadsheet = _get_spreadsheet()
        # Try to get basic spreadsheet info
        title = spreadsheet.title
        logger.info(f"Google Sheets API connection test successful for: {title}")
        
        # Initialize default worksheets
        logger.info("Initializing default worksheets...")
        results = initialize_default_worksheets()
        
        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)
        
        logger.info(f"Worksheet initialization complete: {success_count}/{total_count} successful")
        
        return True
        
    except Exception as e:
        logger.error(f"Google Sheets API connection test failed: {str(e)}")
        return False