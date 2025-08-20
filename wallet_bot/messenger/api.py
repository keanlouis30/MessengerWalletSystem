"""
Messenger API Module - The Bot's "Mouth"

This module handles all outgoing communication to the Meta Messenger API.
It provides functions to send text messages, quick replies, and other message types
to users through Facebook Messenger.
"""

import requests
import json
import logging
from typing import List, Dict, Any, Optional
from wallet_bot.config.settings import get_page_access_token

# Configure logging
logger = logging.getLogger(__name__)

# Meta Messenger API endpoints
MESSENGER_API_URL = "https://graph.facebook.com/v18.0/me/messages"


def send_text_message(user_id: str, text: str) -> bool:
    """
    Send a plain text message to a user.
    
    Args:
        user_id (str): The recipient's Facebook user ID
        text (str): The message text to send
        
    Returns:
        bool: True if message sent successfully, False otherwise
    """
    try:
        payload = {
            "recipient": {"id": user_id},
            "message": {"text": text}
        }
        
        return _send_message(payload)
        
    except Exception as e:
        logger.error(f"Error sending text message to {user_id}: {str(e)}")
        return False


def send_quick_replies(user_id: str, text: str, replies: List[Dict[str, str]]) -> bool:
    """
    Send a message with quick reply buttons.
    
    Args:
        user_id (str): The recipient's Facebook user ID
        text (str): The message text to display above the buttons
        replies (List[Dict]): List of quick reply options
                             Each dict should have 'title' and 'payload' keys
                             
    Example:
        replies = [
            {"title": "Log Expense", "payload": "LOG_EXPENSE"},
            {"title": "Log Income", "payload": "LOG_INCOME"},
            {"title": "View Statistics", "payload": "VIEW_STATS"}
        ]
        
    Returns:
        bool: True if message sent successfully, False otherwise
    """
    try:
        # Format quick replies for Meta API
        quick_replies = []
        for reply in replies:
            quick_replies.append({
                "content_type": "text",
                "title": reply["title"],
                "payload": reply["payload"]
            })
        
        payload = {
            "recipient": {"id": user_id},
            "message": {
                "text": text,
                "quick_replies": quick_replies
            }
        }
        
        return _send_message(payload)
        
    except Exception as e:
        logger.error(f"Error sending quick replies to {user_id}: {str(e)}")
        return False


def send_typing_indicator(user_id: str, action: str = "typing_on") -> bool:
    """
    Send typing indicator to show the bot is processing.
    
    Args:
        user_id (str): The recipient's Facebook user ID
        action (str): Either "typing_on" or "typing_off"
        
    Returns:
        bool: True if indicator sent successfully, False otherwise
    """
    try:
        payload = {
            "recipient": {"id": user_id},
            "sender_action": action
        }
        
        return _send_message(payload)
        
    except Exception as e:
        logger.error(f"Error sending typing indicator to {user_id}: {str(e)}")
        return False


def send_button_message(user_id: str, text: str, buttons: List[Dict[str, str]]) -> bool:
    """
    Send a message with persistent menu buttons.
    
    Args:
        user_id (str): The recipient's Facebook user ID
        text (str): The message text to display
        buttons (List[Dict]): List of button options
                             Each dict should have 'type', 'title', and 'payload' keys
                             
    Example:
        buttons = [
            {"type": "postback", "title": "Get Started", "payload": "GET_STARTED"},
            {"type": "postback", "title": "Help", "payload": "HELP"}
        ]
        
    Returns:
        bool: True if message sent successfully, False otherwise
    """
    try:
        # Format buttons for Meta API
        formatted_buttons = []
        for button in buttons:
            formatted_buttons.append({
                "type": button.get("type", "postback"),
                "title": button["title"],
                "payload": button["payload"]
            })
        
        payload = {
            "recipient": {"id": user_id},
            "message": {
                "attachment": {
                    "type": "template",
                    "payload": {
                        "template_type": "button",
                        "text": text,
                        "buttons": formatted_buttons
                    }
                }
            }
        }
        
        return _send_message(payload)
        
    except Exception as e:
        logger.error(f"Error sending button message to {user_id}: {str(e)}")
        return False


def send_generic_template(user_id: str, elements: List[Dict[str, Any]]) -> bool:
    """
    Send a generic template message (carousel of cards).
    
    Args:
        user_id (str): The recipient's Facebook user ID
        elements (List[Dict]): List of template elements
                              Each element can have title, subtitle, image_url, buttons
        
    Returns:
        bool: True if message sent successfully, False otherwise
    """
    try:
        payload = {
            "recipient": {"id": user_id},
            "message": {
                "attachment": {
                    "type": "template",
                    "payload": {
                        "template_type": "generic",
                        "elements": elements
                    }
                }
            }
        }
        
        return _send_message(payload)
        
    except Exception as e:
        logger.error(f"Error sending generic template to {user_id}: {str(e)}")
        return False


def send_welcome_message(user_id: str) -> bool:
    """
    Send the initial welcome message with main menu options.
    
    Args:
        user_id (str): The recipient's Facebook user ID
        
    Returns:
        bool: True if message sent successfully, False otherwise
    """
    welcome_text = (
        "👋 Welcome to Messenger Wallet Bot! 💰\n\n"
        "I'll help you track your income and expenses effortlessly. "
        "Just chat with me like you would with a friend!\n\n"
        "What would you like to do?"
    )
    
    quick_replies = [
        {"title": "💸 Log Expense", "payload": "LOG_EXPENSE"},
        {"title": "💰 Log Income", "payload": "LOG_INCOME"},
        {"title": "📊 View Statistics", "payload": "VIEW_STATS"}
    ]
    
    return send_quick_replies(user_id, welcome_text, quick_replies)


def send_confirmation_message(user_id: str, transaction_type: str, amount: float, 
                            description: str = "", category: str = "") -> bool:
    """
    Send a confirmation message after logging a transaction.
    
    Args:
        user_id (str): The recipient's Facebook user ID
        transaction_type (str): "income" or "expense"
        amount (float): The transaction amount
        description (str): Optional transaction description
        category (str): Optional transaction category
        
    Returns:
        bool: True if message sent successfully, False otherwise
    """
    try:
        # Format amount as Philippine Peso
        formatted_amount = f"₱{amount:,.2f}"
        
        if transaction_type.lower() == "income":
            emoji = "💰"
            action = "added"
        else:
            emoji = "💸"
            action = "logged"
        
        # Build confirmation message
        confirmation_text = f"{emoji} Successfully {action}!\n\n"
        confirmation_text += f"Amount: {formatted_amount}\n"
        
        if category:
            confirmation_text += f"Category: {category}\n"
        
        if description:
            confirmation_text += f"Description: {description}\n"
        
        confirmation_text += "\nWhat would you like to do next?"
        
        # Offer main menu options again
        quick_replies = [
            {"title": "💸 Log Another Expense", "payload": "LOG_EXPENSE"},
            {"title": "💰 Log Income", "payload": "LOG_INCOME"},
            {"title": "📊 View Statistics", "payload": "VIEW_STATS"}
        ]
        
        return send_quick_replies(user_id, confirmation_text, quick_replies)
        
    except Exception as e:
        logger.error(f"Error sending confirmation message to {user_id}: {str(e)}")
        return False


def send_error_message(user_id: str, error_type: str = "general") -> bool:
    """
    Send an appropriate error message to the user.
    
    Args:
        user_id (str): The recipient's Facebook user ID
        error_type (str): Type of error (general, invalid_amount, etc.)
        
    Returns:
        bool: True if message sent successfully, False otherwise
    """
    error_messages = {
        "general": (
            "😅 Oops! Something went wrong. Please try again.\n\n"
            "If the problem persists, try restarting our conversation."
        ),
        "invalid_amount": (
            "❌ Please enter a valid amount (numbers only).\n\n"
            "Example: 150 or 1500.50"
        ),
        "missing_description": (
            "❌ Please provide a description for this transaction.\n\n"
            "Example: 'Lunch at restaurant' or 'Freelance payment'"
        ),
        "sheets_error": (
            "📊 Unable to save to your financial log right now. "
            "Please try again in a moment.\n\n"
            "Your data is important to us!"
        )
    }
    
    message = error_messages.get(error_type, error_messages["general"])
    return send_text_message(user_id, message)


def _send_message(payload: Dict[str, Any]) -> bool:
    """
    Internal helper function to send messages to Meta API.
    
    Args:
        payload (Dict): The message payload to send
        
    Returns:
        bool: True if message sent successfully, False otherwise
    """
    try:
        # Get access token
        access_token = get_page_access_token()
        
        # Prepare headers
        headers = {
            "Content-Type": "application/json"
        }
        
        # Prepare parameters
        params = {
            "access_token": access_token
        }
        
        # Send request to Meta API
        response = requests.post(
            MESSENGER_API_URL,
            params=params,
            headers=headers,
            json=payload,
            timeout=10
        )
        
        # Check if request was successful
        if response.status_code == 200:
            logger.info(f"Message sent successfully to user {payload['recipient']['id']}")
            return True
        else:
            logger.error(f"Failed to send message. Status: {response.status_code}, Response: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        logger.error("Request to Meta API timed out")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error when sending message: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error in _send_message: {str(e)}")
        return False


def get_user_profile(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get user profile information from Meta API.
    
    Args:
        user_id (str): The user's Facebook ID
        
    Returns:
        Dict or None: User profile data if successful, None otherwise
    """
    try:
        access_token = get_page_access_token()
        
        url = f"https://graph.facebook.com/v18.0/{user_id}"
        params = {
            "fields": "first_name,last_name,profile_pic",
            "access_token": access_token
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to get user profile. Status: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Error getting user profile for {user_id}: {str(e)}")
        return None


# Utility function for testing
def test_api_connection() -> bool:
    """
    Test if the Meta API connection is working.
    
    Returns:
        bool: True if connection is working, False otherwise
    """
    try:
        access_token = get_page_access_token()
        
        # Test with a simple API call
        url = "https://graph.facebook.com/v18.0/me"
        params = {"access_token": access_token}
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            logger.info("Meta API connection test successful")
            return True
        else:
            logger.error(f"Meta API connection test failed. Status: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Error testing API connection: {str(e)}")
        return False