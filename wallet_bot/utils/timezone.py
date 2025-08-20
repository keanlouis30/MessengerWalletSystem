# wallet_bot/utils/timezone.py
"""
Timezone utility module for handling Manila timezone operations.

This module provides functions to get the current time in Manila timezone
and handle timezone-aware datetime operations throughout the application.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional


# Manila timezone (UTC+8)
MANILA_TIMEZONE = timezone(timedelta(hours=8))


def now_manila() -> datetime:
    """
    Get the current datetime in Manila timezone.
    
    Returns:
        datetime: Current datetime in Manila timezone (UTC+8)
    """
    return datetime.now(MANILA_TIMEZONE)


def to_manila_timezone(dt: datetime) -> datetime:
    """
    Convert a datetime to Manila timezone.
    
    Args:
        dt (datetime): DateTime to convert (assumed UTC if naive)
        
    Returns:
        datetime: DateTime converted to Manila timezone
    """
    if dt.tzinfo is None:
        # Assume UTC if naive datetime
        dt = dt.replace(tzinfo=timezone.utc)
    
    return dt.astimezone(MANILA_TIMEZONE)


def format_manila_timestamp(dt: Optional[datetime] = None) -> str:
    """
    Format a datetime as a string in Manila timezone.
    
    Args:
        dt (datetime, optional): DateTime to format. If None, uses current time.
        
    Returns:
        str: Formatted timestamp string (YYYY-MM-DD HH:MM:SS)
    """
    if dt is None:
        dt = now_manila()
    elif dt.tzinfo is None:
        # Convert naive datetime (assume UTC) to Manila time
        dt = dt.replace(tzinfo=timezone.utc).astimezone(MANILA_TIMEZONE)
    elif dt.tzinfo != MANILA_TIMEZONE:
        # Convert to Manila timezone if it's in a different timezone
        dt = dt.astimezone(MANILA_TIMEZONE)
    
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def get_week_start_manila(dt: Optional[datetime] = None) -> datetime:
    """
    Get the start of the week (Monday 00:00:00) in Manila timezone.
    
    Args:
        dt (datetime, optional): Reference datetime. If None, uses current time.
        
    Returns:
        datetime: Start of week in Manila timezone
    """
    if dt is None:
        dt = now_manila()
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc).astimezone(MANILA_TIMEZONE)
    elif dt.tzinfo != MANILA_TIMEZONE:
        dt = dt.astimezone(MANILA_TIMEZONE)
    
    # Get Monday of current week
    days_since_monday = dt.weekday()
    week_start = dt - timedelta(days=days_since_monday)
    week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
    
    return week_start


def get_month_start_manila(dt: Optional[datetime] = None) -> datetime:
    """
    Get the start of the month (1st day 00:00:00) in Manila timezone.
    
    Args:
        dt (datetime, optional): Reference datetime. If None, uses current time.
        
    Returns:
        datetime: Start of month in Manila timezone
    """
    if dt is None:
        dt = now_manila()
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc).astimezone(MANILA_TIMEZONE)
    elif dt.tzinfo != MANILA_TIMEZONE:
        dt = dt.astimezone(MANILA_TIMEZONE)
    
    # Get first day of current month
    month_start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    return month_start


def parse_manila_timestamp(timestamp_str: str) -> datetime:
    """
    Parse a timestamp string and return as Manila timezone datetime.
    
    Args:
        timestamp_str (str): Timestamp string in format 'YYYY-MM-DD HH:MM:SS'
        
    Returns:
        datetime: Parsed datetime in Manila timezone
    """
    # Parse the timestamp string
    dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    
    # Assume it's already in Manila timezone if no timezone info
    dt = dt.replace(tzinfo=MANILA_TIMEZONE)
    
    return dt
