"""
Messenger Handler Module - The Conversation "Brain"

This module processes incoming messages from users and manages the conversation's 
state and logic. It serves as the core orchestrator, deciphering user intent and 
guiding users through multi-step interactions.
"""

import logging
import re
from typing import Dict, Any, Optional, List
from datetime import datetime

from wallet_bot.messenger.api import (
    send_text_message, send_quick_replies, send_welcome_message,
    send_confirmation_message, send_error_message, send_typing_indicator
)
from wallet_bot.sheets.handler import log_transaction, get_transactions_for_period, regenerate_formatted_report
from wallet_bot.analytics.generator import generate_report
from wallet_bot.utils.timezone import format_manila_timestamp

# Configure logging
logger = logging.getLogger(__name__)

# In-memory conversation state storage
# In production, this could be Redis or database
conversation_states = {}

# Conversation states
class ConversationState:
    IDLE = "idle"
    WAITING_EXPENSE_CATEGORY = "waiting_expense_category"
    WAITING_EXPENSE_DESCRIPTION = "waiting_expense_description"
    WAITING_EXPENSE_AMOUNT = "waiting_expense_amount"
    WAITING_INCOME_SOURCE = "waiting_income_source"
    WAITING_INCOME_DESCRIPTION = "waiting_income_description"
    WAITING_INCOME_AMOUNT = "waiting_income_amount"
    WAITING_STATS_PERIOD = "waiting_stats_period"

# Predefined categories and sources
EXPENSE_CATEGORIES = [
    {"title": "🍔 Food", "payload": "CATEGORY_FOOD"},
    {"title": "🚗 Transportation", "payload": "CATEGORY_TRANSPORT"},
    {"title": "🏠 Housing", "payload": "CATEGORY_HOUSING"},
    {"title": "🛒 Shopping", "payload": "CATEGORY_SHOPPING"},
    {"title": "⚡ Utilities", "payload": "CATEGORY_UTILITIES"},
    {"title": "🎬 Entertainment", "payload": "CATEGORY_ENTERTAINMENT"},
    {"title": "💊 Healthcare", "payload": "CATEGORY_HEALTHCARE"},
    {"title": "📚 Education", "payload": "CATEGORY_EDUCATION"},
    {"title": "🔧 Other", "payload": "CATEGORY_OTHER"}
]

INCOME_SOURCES = [
    {"title": "💼 Salary", "payload": "SOURCE_SALARY"},
    {"title": "💻 Freelance", "payload": "SOURCE_FREELANCE"},
    {"title": "📈 Business", "payload": "SOURCE_BUSINESS"},
    {"title": "🎁 Gift", "payload": "SOURCE_GIFT"},
    {"title": "💰 Investment", "payload": "SOURCE_INVESTMENT"},
    {"title": "🔄 Refund", "payload": "SOURCE_REFUND"},
    {"title": "🔧 Other", "payload": "SOURCE_OTHER"}
]

STATS_PERIODS = [
    {"title": "📅 This Week", "payload": "PERIOD_WEEK"},
    {"title": "📆 This Month", "payload": "PERIOD_MONTH"}
]


def process_webhook_message(payload: Dict[str, Any]) -> bool:
    """
    Main entry point for processing incoming webhook messages from Meta.
    
    Args:
        payload (Dict): The JSON payload from Meta's webhook
        
    Returns:
        bool: True if message processed successfully, False otherwise
    """
    try:
        # Extract messaging data
        if "entry" not in payload:
            logger.warning("No 'entry' field in webhook payload")
            return False
            
        for entry in payload["entry"]:
            if "messaging" not in entry:
                continue
                
            for message_event in entry["messaging"]:
                success = _handle_message_event(message_event)
                if not success:
                    logger.error(f"Failed to handle message event: {message_event}")
                    
        return True
        
    except Exception as e:
        logger.error(f"Error processing webhook message: {str(e)}")
        return False


def _handle_message_event(event: Dict[str, Any]) -> bool:
    """
    Handle individual message events from the webhook.
    
    Args:
        event (Dict): Individual message event from Meta
        
    Returns:
        bool: True if handled successfully, False otherwise
    """
    try:
        sender_id = event.get("sender", {}).get("id")
        if not sender_id:
            logger.warning("No sender ID in message event")
            return False
            
        # Handle different types of events
        if "message" in event:
            return _handle_incoming_message(sender_id, event["message"])
        elif "postback" in event:
            return _handle_postback(sender_id, event["postback"])
        else:
            logger.info(f"Unhandled event type from {sender_id}")
            return True
            
    except Exception as e:
        logger.error(f"Error handling message event: {str(e)}")
        return False


def _handle_incoming_message(user_id: str, message: Dict[str, Any]) -> bool:
    """
    Handle incoming text messages and quick reply responses.
    
    Args:
        user_id (str): The sender's Facebook user ID
        message (Dict): The message data from Meta
        
    Returns:
        bool: True if handled successfully, False otherwise
    """
    try:
        # Send typing indicator
        send_typing_indicator(user_id)
        
        # Handle quick reply responses
        if "quick_reply" in message:
            payload = message["quick_reply"]["payload"]
            return _handle_quick_reply(user_id, payload)
            
        # Handle text messages
        if "text" in message:
            text = message["text"].strip()
            return _handle_text_message(user_id, text)
            
        # Handle other message types (stickers, attachments, etc.)
        return _handle_unsupported_message(user_id)
        
    except Exception as e:
        logger.error(f"Error handling incoming message from {user_id}: {str(e)}")
        return send_error_message(user_id)


def _handle_postback(user_id: str, postback: Dict[str, Any]) -> bool:
    """
    Handle postback events (usually from persistent menu or Get Started button).
    
    Args:
        user_id (str): The sender's Facebook user ID
        postback (Dict): The postback data from Meta
        
    Returns:
        bool: True if handled successfully, False otherwise
    """
    try:
        payload = postback.get("payload", "")
        
        # Handle Get Started button
        if payload == "GET_STARTED":
            _reset_conversation_state(user_id)
            return send_welcome_message(user_id)
            
        # Treat other postbacks as quick replies
        return _handle_quick_reply(user_id, payload)
        
    except Exception as e:
        logger.error(f"Error handling postback from {user_id}: {str(e)}")
        return send_error_message(user_id)


def _handle_quick_reply(user_id: str, payload: str) -> bool:
    """
    Handle quick reply button presses.
    
    Args:
        user_id (str): The sender's Facebook user ID
        payload (str): The payload from the quick reply button
        
    Returns:
        bool: True if handled successfully, False otherwise
    """
    try:
        # Main menu options
        if payload == "LOG_EXPENSE":
            return _start_expense_logging(user_id)
        elif payload == "LOG_INCOME":
            return _start_income_logging(user_id)
        elif payload == "VIEW_STATS":
            return _start_stats_request(user_id)
            
        # Expense category selection
        elif payload.startswith("CATEGORY_"):
            return _handle_expense_category_selection(user_id, payload)
            
        # Income source selection
        elif payload.startswith("SOURCE_"):
            return _handle_income_source_selection(user_id, payload)
            
        # Statistics period selection
        elif payload.startswith("PERIOD_"):
            return _handle_stats_period_selection(user_id, payload)
            
        else:
            logger.warning(f"Unhandled quick reply payload: {payload}")
            return send_error_message(user_id)
            
    except Exception as e:
        logger.error(f"Error handling quick reply from {user_id}: {str(e)}")
        return send_error_message(user_id)


def _handle_text_message(user_id: str, text: str) -> bool:
    """
    Handle free-form text messages based on conversation state.
    
    Args:
        user_id (str): The sender's Facebook user ID
        text (str): The message text
        
    Returns:
        bool: True if handled successfully, False otherwise
    """
    try:
        state = _get_conversation_state(user_id)
        
        # Handle greetings and common phrases
        if state == ConversationState.IDLE:
            return _handle_idle_text_message(user_id, text)
            
        # Handle expense flow
        elif state == ConversationState.WAITING_EXPENSE_DESCRIPTION:
            return _handle_expense_description(user_id, text)
        elif state == ConversationState.WAITING_EXPENSE_AMOUNT:
            return _handle_expense_amount(user_id, text)
            
        # Handle income flow
        elif state == ConversationState.WAITING_INCOME_DESCRIPTION:
            return _handle_income_description(user_id, text)
        elif state == ConversationState.WAITING_INCOME_AMOUNT:
            return _handle_income_amount(user_id, text)
            
        else:
            logger.warning(f"Unhandled conversation state: {state}")
            return _reset_and_show_menu(user_id)
            
    except Exception as e:
        logger.error(f"Error handling text message from {user_id}: {str(e)}")
        return send_error_message(user_id)


def _handle_idle_text_message(user_id: str, text: str) -> bool:
    """
    Handle text messages when user is in idle state.
    
    Args:
        user_id (str): The sender's Facebook user ID
        text (str): The message text
        
    Returns:
        bool: True if handled successfully, False otherwise
    """
    text_lower = text.lower()
    
    # Greetings
    if any(greeting in text_lower for greeting in ["hi", "hello", "hey", "start", "menu"]):
        return send_welcome_message(user_id)
        
    # Direct commands
    elif any(word in text_lower for word in ["expense", "spend", "cost", "buy", "paid"]):
        return _start_expense_logging(user_id)
    elif any(word in text_lower for word in ["income", "earn", "salary", "money", "receive"]):
        return _start_income_logging(user_id)
    elif any(word in text_lower for word in ["stats", "report", "summary", "total", "view"]):
        return _start_stats_request(user_id)
    elif any(word in text_lower for word in ["help", "what", "how"]):
        return _send_help_message(user_id)
    else:
        # Default to showing menu
        return send_welcome_message(user_id)


def _start_expense_logging(user_id: str) -> bool:
    """Start the expense logging flow."""
    _set_conversation_state(user_id, ConversationState.WAITING_EXPENSE_CATEGORY)
    
    text = "💸 Let's log your expense!\n\nFirst, what category does this expense fall under?"
    return send_quick_replies(user_id, text, EXPENSE_CATEGORIES)


def _start_income_logging(user_id: str) -> bool:
    """Start the income logging flow."""
    _set_conversation_state(user_id, ConversationState.WAITING_INCOME_SOURCE)
    
    text = "💰 Great! Let's log your income.\n\nWhat's the source of this income?"
    return send_quick_replies(user_id, text, INCOME_SOURCES)


def _start_stats_request(user_id: str) -> bool:
    """Start the statistics request flow."""
    _set_conversation_state(user_id, ConversationState.WAITING_STATS_PERIOD)
    
    text = "📊 I'll generate your financial report!\n\nWhich time period would you like to see?"
    return send_quick_replies(user_id, text, STATS_PERIODS)


def _handle_expense_category_selection(user_id: str, payload: str) -> bool:
    """Handle expense category selection."""
    try:
        # Extract category from payload
        category_map = {
            "CATEGORY_FOOD": "Food",
            "CATEGORY_TRANSPORT": "Transportation", 
            "CATEGORY_HOUSING": "Housing",
            "CATEGORY_SHOPPING": "Shopping",
            "CATEGORY_UTILITIES": "Utilities",
            "CATEGORY_ENTERTAINMENT": "Entertainment",
            "CATEGORY_HEALTHCARE": "Healthcare",
            "CATEGORY_EDUCATION": "Education",
            "CATEGORY_OTHER": "Other"
        }
        
        category = category_map.get(payload, "Other")
        
        # Store category in conversation state
        _update_conversation_data(user_id, {"expense_category": category})
        _set_conversation_state(user_id, ConversationState.WAITING_EXPENSE_DESCRIPTION)
        
        text = f"✅ Category: {category}\n\nNow, please provide a brief description of this expense.\n\nExample: 'Lunch at McDonald's' or 'Gas for car'"
        return send_text_message(user_id, text)
        
    except Exception as e:
        logger.error(f"Error handling expense category selection: {str(e)}")
        return send_error_message(user_id)


def _handle_income_source_selection(user_id: str, payload: str) -> bool:
    """Handle income source selection."""
    try:
        # Extract source from payload
        source_map = {
            "SOURCE_SALARY": "Salary",
            "SOURCE_FREELANCE": "Freelance",
            "SOURCE_BUSINESS": "Business",
            "SOURCE_GIFT": "Gift",
            "SOURCE_INVESTMENT": "Investment",
            "SOURCE_REFUND": "Refund",
            "SOURCE_OTHER": "Other"
        }
        
        source = source_map.get(payload, "Other")
        
        # Store source in conversation state
        _update_conversation_data(user_id, {"income_source": source})
        _set_conversation_state(user_id, ConversationState.WAITING_INCOME_DESCRIPTION)
        
        text = f"✅ Source: {source}\n\nPlease provide a description for this income.\n\nExample: 'Monthly salary' or 'Website project payment'"
        return send_text_message(user_id, text)
        
    except Exception as e:
        logger.error(f"Error handling income source selection: {str(e)}")
        return send_error_message(user_id)


def _handle_stats_period_selection(user_id: str, payload: str) -> bool:
    """Handle statistics period selection."""
    try:
        period_map = {
            "PERIOD_WEEK": "This Week",
            "PERIOD_MONTH": "This Month"
        }
        
        period = period_map.get(payload, "This Week")
        
        # Generate and send report
        return _generate_and_send_report(user_id, period)
        
    except Exception as e:
        logger.error(f"Error handling stats period selection: {str(e)}")
        return send_error_message(user_id)


def _handle_expense_description(user_id: str, description: str) -> bool:
    """Handle expense description input."""
    try:
        if len(description.strip()) < 2:
            return send_error_message(user_id, "missing_description")
            
        # Store description
        _update_conversation_data(user_id, {"expense_description": description.strip()})
        _set_conversation_state(user_id, ConversationState.WAITING_EXPENSE_AMOUNT)
        
        text = f"✅ Description: {description.strip()}\n\nFinally, how much did you spend?\n\nJust enter the amount (numbers only):\nExample: 150 or 1500.50"
        return send_text_message(user_id, text)
        
    except Exception as e:
        logger.error(f"Error handling expense description: {str(e)}")
        return send_error_message(user_id)


def _handle_income_description(user_id: str, description: str) -> bool:
    """Handle income description input."""
    try:
        if len(description.strip()) < 2:
            return send_error_message(user_id, "missing_description")
            
        # Store description
        _update_conversation_data(user_id, {"income_description": description.strip()})
        _set_conversation_state(user_id, ConversationState.WAITING_INCOME_AMOUNT)
        
        text = f"✅ Description: {description.strip()}\n\nHow much did you receive?\n\nJust enter the amount (numbers only):\nExample: 5000 or 15000.75"
        return send_text_message(user_id, text)
        
    except Exception as e:
        logger.error(f"Error handling income description: {str(e)}")
        return send_error_message(user_id)


def _handle_expense_amount(user_id: str, amount_text: str) -> bool:
    """Handle expense amount input and complete the transaction."""
    try:
        # Parse amount
        amount = _parse_amount(amount_text)
        if amount is None:
            return send_error_message(user_id, "invalid_amount")
            
        # Get stored data
        conversation_data = _get_conversation_data(user_id)
        category = conversation_data.get("expense_category", "Other")
        description = conversation_data.get("expense_description", "Expense")
        
        # Log transaction
        success = _log_expense_transaction(user_id, amount, description, category)
        if not success:
            return send_error_message(user_id, "sheets_error")
            
        # Send confirmation and reset state
        _reset_conversation_state(user_id)
        return send_confirmation_message(user_id, "expense", amount, description, category)
        
    except Exception as e:
        logger.error(f"Error handling expense amount: {str(e)}")
        return send_error_message(user_id)


def _handle_income_amount(user_id: str, amount_text: str) -> bool:
    """Handle income amount input and complete the transaction."""
    try:
        # Parse amount
        amount = _parse_amount(amount_text)
        if amount is None:
            return send_error_message(user_id, "invalid_amount")
            
        # Get stored data
        conversation_data = _get_conversation_data(user_id)
        source = conversation_data.get("income_source", "Other")
        description = conversation_data.get("income_description", "Income")
        
        # Log transaction
        success = _log_income_transaction(user_id, amount, description, source)
        if not success:
            return send_error_message(user_id, "sheets_error")
            
        # Send confirmation and reset state
        _reset_conversation_state(user_id)
        return send_confirmation_message(user_id, "income", amount, description, source)
        
    except Exception as e:
        logger.error(f"Error handling income amount: {str(e)}")
        return send_error_message(user_id)


def _log_expense_transaction(user_id: str, amount: float, description: str, category: str) -> bool:
    """Log expense transaction to Google Sheets."""
    try:
        # Log to sheets by passing the correct arguments for an EXPENSE
        success = log_transaction(
            transaction_type='expense',             # <-- FIX 1: Changed to 'expense'
            category_or_source=category,          # <-- FIX 2: Changed to use the 'category' variable
            description=description,
            amount=amount,
            user_id=user_id
        )

        if success:
            # Trigger report regeneration in background
            regenerate_formatted_report()
            
        return success
        
    except Exception as e:
        logger.error(f"Error logging expense transaction: {str(e)}")
        return False


def _log_income_transaction(user_id: str, amount: float, description: str, source: str) -> bool:
    """Log income transaction to Google Sheets."""
    try:
        # Prepare transaction data (using Manila timezone)
        transaction_data = {
            "timestamp": format_manila_timestamp(),
            "user_id": user_id,
            "type": "income",
            "amount": amount,
            "description": description,
            "category": source  # Using category field for income source
        }
        
        # Log to sheets
        success = log_transaction(
            transaction_type='income',
            category_or_source=source,
            description=description,
            amount=amount,
            user_id=user_id
        ) # <--- THIS IS THE FIX

        if success:
            # Trigger report regeneration in background
            regenerate_formatted_report()
            
        return success
        
    except Exception as e:
        logger.error(f"Error logging income transaction: {str(e)}")
        return False


def _generate_and_send_report(user_id: str, period: str) -> bool:
    """Generate and send financial report to user."""
    try:
        # Get transactions for period
        transactions = get_transactions_for_period(period, user_id)
        
        if not transactions:
            text = f"📊 No transactions found for {period.lower()}.\n\nStart logging your income and expenses to see your financial report!"
            _reset_conversation_state(user_id)
            return send_text_message(user_id, text)
        
        # Generate report
        report = generate_report(transactions, period)
        
        # Send report
        _reset_conversation_state(user_id)
        return send_text_message(user_id, report)
        
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        return send_error_message(user_id)


def _parse_amount(amount_text: str) -> Optional[float]:
    """Parse amount text into float value."""
    try:
        # Remove common currency symbols and whitespace
        cleaned = re.sub(r'[₱$,\s]', '', amount_text)
        
        # Try to convert to float
        amount = float(cleaned)
        
        # Validate reasonable range
        if amount <= 0 or amount > 1_000_000:  # Up to 1 million pesos
            return None
            
        return round(amount, 2)
        
    except (ValueError, TypeError):
        return None


def _handle_unsupported_message(user_id: str) -> bool:
    """Handle unsupported message types."""
    text = "I can only process text messages right now. 😅\n\nPlease send me text or use the quick reply buttons."
    return send_text_message(user_id, text)


def _send_help_message(user_id: str) -> bool:
    """Send help message to user."""
    help_text = (
        "🤖 **Messenger Wallet Bot Help**\n\n"
        "I help you track income and expenses easily!\n\n"
        "**Main Features:**\n"
        "💸 Log Expense - Record money you spent\n"
        "💰 Log Income - Record money you received\n"
        "📊 View Statistics - See your financial reports\n\n"
        "**Tips:**\n"
        "• Use the quick reply buttons for fastest navigation\n"
        "• Be specific with descriptions (e.g., 'Lunch at Jollibee')\n"
        "• Enter amounts as numbers only (e.g., 150 or 1500.50)\n\n"
        "Ready to get started?"
    )
    
    return send_welcome_message(user_id)


def _reset_and_show_menu(user_id: str) -> bool:
    """Reset conversation state and show main menu."""
    _reset_conversation_state(user_id)
    return send_welcome_message(user_id)


# Conversation state management functions
def _get_conversation_state(user_id: str) -> str:
    """Get the current conversation state for a user."""
    return conversation_states.get(user_id, {}).get("state", ConversationState.IDLE)


def _set_conversation_state(user_id: str, state: str) -> None:
    """Set the conversation state for a user."""
    if user_id not in conversation_states:
        conversation_states[user_id] = {}
    conversation_states[user_id]["state"] = state


def _get_conversation_data(user_id: str) -> Dict[str, Any]:
    """Get the conversation data for a user."""
    return conversation_states.get(user_id, {}).get("data", {})


def _update_conversation_data(user_id: str, data: Dict[str, Any]) -> None:
    """Update conversation data for a user."""
    if user_id not in conversation_states:
        conversation_states[user_id] = {"state": ConversationState.IDLE, "data": {}}
    if "data" not in conversation_states[user_id]:
        conversation_states[user_id]["data"] = {}
    conversation_states[user_id]["data"].update(data)


def _reset_conversation_state(user_id: str) -> None:
    """Reset conversation state for a user."""
    conversation_states[user_id] = {"state": ConversationState.IDLE, "data": {}}