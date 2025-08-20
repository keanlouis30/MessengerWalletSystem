# wallet_bot/sheets/__init__.py

# Expose the high-level data handling functions.
# Other parts of the application should not need to access the low-level api.py directly.
# This creates a clean interface for all database operations.
#
# You can now import like this:
# from wallet_bot.sheets import log_transaction, regenerate_formatted_report

from .handler import (
    log_transaction,
    get_transactions_for_period,
    regenerate_formatted_report,
    get_user_categories
)