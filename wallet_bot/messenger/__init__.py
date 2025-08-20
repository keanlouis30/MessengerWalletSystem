# wallet_bot/messenger/__init__.py

# "Lift" the most important functions from the handler and api modules
# to the package level for easier access.

# This allows you to import them like this:
# from wallet_bot.messenger import process_message, send_text_message
#
# Instead of the longer:
# from wallet_bot.messenger.handler import process_message
# from wallet_bot.messenger.api import send_text_message

from .handler import process_webhook_message
from .api import send_text_message, send_quick_replies