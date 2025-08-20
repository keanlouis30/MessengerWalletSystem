"""
Configuration settings for the Messenger Wallet Bot.

This module centralizes all configuration management, loading environment variables
and providing access to configuration values throughout the application.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Get the project root directory (two levels up from this file)
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Load environment variables from .env file
load_dotenv(PROJECT_ROOT / '.env')

class Config:
    """Configuration class that holds all application settings."""
    
    # Meta/Facebook Messenger API Configuration
    PAGE_ACCESS_TOKEN = os.getenv('PAGE_ACCESS_TOKEN')
    VERIFY_TOKEN = os.getenv('VERIFY_TOKEN')
    
    # Google Sheets Configuration
    GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
    
    # Google Service Account Credentials (Render-compatible)
    # Try JSON string first (for Render environment variable), then file path
    GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
    CREDENTIALS_PATH = PROJECT_ROOT / 'wallet_bot' / 'config' / 'credentials.json'
    
    # Sheet Names
    DATA_LOG_SHEET = 'Data_Log'
    FORMATTED_REPORT_SHEET = 'Formatted_Report'
    
    # Server Configuration (Render-compatible)
    HOST = os.getenv('HOST', '0.0.0.0')
    PORT = int(os.getenv('PORT', 10000))  # Render's default port
    DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'
    
    # Currency Configuration (for Philippines)
    CURRENCY_SYMBOL = '₱'
    CURRENCY_CODE = 'PHP'
    
    # Webhook Configuration
    WEBHOOK_ENDPOINT = '/webhook'
    
    # Render Environment Detection
    IS_RENDER = os.getenv('RENDER') == 'true' or os.getenv('RENDER_SERVICE_ID') is not None
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Environment Detection
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'production' if IS_RENDER else 'development')
    
    @classmethod
    def validate_required_settings(cls):
        """
        Validate that all required environment variables are set.
        
        Raises:
            ValueError: If any required configuration is missing.
        """
        required_settings = [
            ('PAGE_ACCESS_TOKEN', cls.PAGE_ACCESS_TOKEN),
            ('VERIFY_TOKEN', cls.VERIFY_TOKEN),
            ('GOOGLE_SHEET_ID', cls.GOOGLE_SHEET_ID),
        ]
        
        missing_settings = []
        for setting_name, setting_value in required_settings:
            if not setting_value:
                missing_settings.append(setting_name)
        
        if missing_settings:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_settings)}. "
                f"Please check your .env file."
            )
        
        # Check credentials - support both file and JSON string (for Render)
        if not cls.GOOGLE_CREDENTIALS_JSON and not cls.CREDENTIALS_PATH.exists():
            raise FileNotFoundError(
                f"Google Service Account credentials not found. Either set GOOGLE_CREDENTIALS_JSON "
                f"environment variable or ensure credentials.json exists at: {cls.CREDENTIALS_PATH}"
            )
    
    @classmethod
    def get_credentials_path(self):
        """Find credentials.json using multiple strategies"""
        
        # Strategy 1: Relative to current file
        current_dir = os.path.dirname(__file__)
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        path1 = os.path.join(project_root, 'credentials.json')
        
        # Strategy 2: Current working directory
        path2 = os.path.join(os.getcwd(), 'credentials.json')
        
        # Strategy 3: Common deployment paths
        deployment_paths = [
            '/opt/render/project/credentials.json',
            '/opt/render/project/src/credentials.json',
            './credentials.json',
            'credentials.json'
        ]
        
        # Try all paths
        all_paths = [path1, path2] + deployment_paths
        
        for path in all_paths:
            if os.path.exists(path):
                return path
        
        # If none found, return the original calculated path for error reporting
        return path1
    @classmethod
    def is_production(cls):
        """
        Check if the application is running in production mode.
        
        Returns:
            bool: True if in production (Render), False otherwise.
        """
        return cls.ENVIRONMENT == 'production' or cls.IS_RENDER
    
    @classmethod
    def is_development(cls):
        """
        Check if the application is running in development mode.
        
        Returns:
            bool: True if in development mode, False otherwise.
        """
        return cls.ENVIRONMENT == 'development' and not cls.IS_RENDER


# Create a global config instance for easy importing
config = Config()

# Convenience functions for commonly accessed settings
def get_page_access_token():
    """Get the Facebook Page Access Token."""
    return config.PAGE_ACCESS_TOKEN

def get_verify_token():
    """Get the webhook verification token."""
    return config.VERIFY_TOKEN

def get_google_sheet_id():
    """Get the Google Sheet ID."""
    return config.GOOGLE_SHEET_ID

def get_credentials_data():
    """Get Google Service Account credentials (JSON object or file path)."""
    return config.get_credentials_data()

def get_data_log_sheet_name():
    """Get the name of the data log sheet."""
    return config.DATA_LOG_SHEET

def is_render_environment():
    """Check if running on Render hosting platform."""
    return config.IS_RENDER

def is_production():
    """Check if running in production mode."""
    return config.is_production()

def get_log_level():
    """Get the logging level."""
    return config.LOG_LEVEL

def get_formatted_report_sheet_name():
    """Get the name of the formatted report sheet."""
    return config.FORMATTED_REPORT_SHEET

def validate_configuration():
    """
    Validate the entire configuration setup.
    
    This should be called at application startup to ensure
    all required settings are properly configured.
    
    Raises:
        ValueError: If configuration validation fails.
        FileNotFoundError: If required files are missing.
    """
    config.validate_required_settings()

# Export commonly used settings
__all__ = [
    'config',
    'Config',
    'get_page_access_token',
    'get_verify_token',
    'get_google_sheet_id',
    'get_credentials_data',
    'get_data_log_sheet_name',
    'get_formatted_report_sheet_name',
    'is_render_environment',
    'is_production',
    'get_log_level',
    'validate_configuration'
]