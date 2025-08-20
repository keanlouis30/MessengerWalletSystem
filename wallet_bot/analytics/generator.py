# wallet_bot/analytics/generator.py
"""
Data Analytics Engine for Messenger Wallet Bot

This module is the "Data Analyst" of the application. It receives clean transaction data
and performs statistical calculations to generate insightful summaries for users.

Key responsibilities:
- Calculate total income and expenses for specified periods
- Identify spending patterns and biggest expenses
- Generate formatted statistical reports
- Calculate financial insights like savings and tithe recommendations

The module is designed to be data-source agnostic - it works with clean Python data
structures and knows nothing about Google Sheets or Messenger APIs.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional


def generate_report(transactions: List[Dict[str, Any]], period: str = "This Week") -> str:
    """
    Generate a comprehensive financial report from transaction data.
    
    Args:
        transactions: List of transaction dictionaries with keys:
                     - 'timestamp': datetime object or ISO string
                     - 'type': 'income' or 'expense'
                     - 'amount': float or string representing money amount
                     - 'category': category name (for expenses) or source (for income)
                     - 'description': transaction description
        period: Time period for the report ("This Week" or "This Month")
    
    Returns:
        str: Formatted report string ready to send to user
    """
    if not transactions:
        return f"📊 *{period} Financial Summary*\n\nNo transactions found for this period. Start logging your income and expenses to see insights!"
    
    # Convert to DataFrame for easier analysis
    df = _prepare_dataframe(transactions, period)
    
    if df.empty:
        return f"📊 *{period} Financial Summary*\n\nNo transactions found for this period."
    
    # Calculate core metrics
    income_total = _calculate_total_income(df)
    expense_total = _calculate_total_expenses(df)
    net_savings = income_total - expense_total
    
    # Generate insights
    biggest_expense = _find_biggest_expense(df)
    top_expense_category = _find_top_expense_category(df)
    income_breakdown = _get_income_breakdown(df)
    expense_breakdown = _get_expense_breakdown(df)
    
    # Calculate additional insights
    tithe_recommendation = income_total * 0.10
    savings_rate = (net_savings / income_total * 100) if income_total > 0 else 0
    
    # Build the report
    report = _build_formatted_report(
        period=period,
        income_total=income_total,
        expense_total=expense_total,
        net_savings=net_savings,
        biggest_expense=biggest_expense,
        top_expense_category=top_expense_category,
        income_breakdown=income_breakdown,
        expense_breakdown=expense_breakdown,
        tithe_recommendation=tithe_recommendation,
        savings_rate=savings_rate,
        transaction_count=len(df)
    )
    
    return report


def _prepare_dataframe(transactions: List[Dict[str, Any]], period: str) -> pd.DataFrame:
    """
    Convert transaction list to filtered pandas DataFrame for the specified period.
    """
    if not transactions:
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(transactions)
    
    # Ensure required columns exist
    required_columns = ['timestamp', 'type', 'amount', 'category', 'description']
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''
    
    # Convert timestamp to datetime if it's not already
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Convert amount to float
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
    
    # Filter by period
    df = _filter_by_period(df, period)
    
    return df


def _filter_by_period(df: pd.DataFrame, period: str) -> pd.DataFrame:
    """
    Filter DataFrame to include only transactions from the specified period.
    """
    if df.empty or 'timestamp' not in df.columns:
        return df
    
    now = datetime.now()
    
    if period == "This Week":
        # Get start of current week (Monday)
        days_since_monday = now.weekday()
        week_start = now - timedelta(days=days_since_monday)
        week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff_date = week_start
    elif period == "This Month":
        # Get start of current month
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        cutoff_date = month_start
    else:
        # Default to past 7 days if period not recognized
        cutoff_date = now - timedelta(days=7)
    
    # Filter transactions
    filtered_df = df[df['timestamp'] >= cutoff_date].copy()
    return filtered_df


def _calculate_total_income(df: pd.DataFrame) -> float:
    """Calculate total income from the DataFrame."""
    income_df = df[df['type'].str.lower() == 'income']
    return income_df['amount'].sum()


def _calculate_total_expenses(df: pd.DataFrame) -> float:
    """Calculate total expenses from the DataFrame."""
    expense_df = df[df['type'].str.lower() == 'expense']
    return expense_df['amount'].sum()


def _find_biggest_expense(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Find the single largest expense transaction."""
    expense_df = df[df['type'].str.lower() == 'expense']
    
    if expense_df.empty:
        return None
    
    biggest_idx = expense_df['amount'].idxmax()
    biggest = expense_df.loc[biggest_idx]
    
    return {
        'amount': biggest['amount'],
        'description': biggest['description'],
        'category': biggest['category']
    }


def _find_top_expense_category(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Find the expense category with the highest total spending."""
    expense_df = df[df['type'].str.lower() == 'expense']
    
    if expense_df.empty:
        return None
    
    category_totals = expense_df.groupby('category')['amount'].sum()
    
    if category_totals.empty:
        return None
    
    top_category = category_totals.idxmax()
    top_amount = category_totals.max()
    
    return {
        'category': top_category,
        'amount': top_amount
    }


def _get_income_breakdown(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Get breakdown of income by source."""
    income_df = df[df['type'].str.lower() == 'income']
    
    if income_df.empty:
        return []
    
    breakdown = income_df.groupby('category')['amount'].sum().sort_values(ascending=False)
    
    return [
        {'source': source, 'amount': amount}
        for source, amount in breakdown.items()
    ]


def _get_expense_breakdown(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Get breakdown of expenses by category."""
    expense_df = df[df['type'].str.lower() == 'expense']
    
    if expense_df.empty:
        return []
    
    breakdown = expense_df.groupby('category')['amount'].sum().sort_values(ascending=False)
    
    return [
        {'category': category, 'amount': amount}
        for category, amount in breakdown.items()
    ]


def _format_currency(amount: float) -> str:
    """Format amount as currency string."""
    return f"₱{amount:,.2f}"


def _build_formatted_report(
    period: str,
    income_total: float,
    expense_total: float,
    net_savings: float,
    biggest_expense: Optional[Dict[str, Any]],
    top_expense_category: Optional[Dict[str, Any]],
    income_breakdown: List[Dict[str, Any]],
    expense_breakdown: List[Dict[str, Any]],
    tithe_recommendation: float,
    savings_rate: float,
    transaction_count: int
) -> str:
    """
    Build the final formatted report string.
    """
    report_lines = []
    
    # Header
    report_lines.append(f"📊 *{period} Financial Summary*")
    report_lines.append("=" * 30)
    
    # Core metrics
    report_lines.append("💰 *OVERVIEW*")
    report_lines.append(f"Total Income: {_format_currency(income_total)}")
    report_lines.append(f"Total Expenses: {_format_currency(expense_total)}")
    report_lines.append(f"Net Savings: {_format_currency(net_savings)}")
    
    if income_total > 0:
        report_lines.append(f"Savings Rate: {savings_rate:.1f}%")
    
    report_lines.append(f"Transactions Logged: {transaction_count}")
    report_lines.append("")
    
    # Income breakdown
    if income_breakdown:
        report_lines.append("💼 *INCOME SOURCES*")
        for item in income_breakdown[:3]:  # Show top 3
            report_lines.append(f"• {item['source']}: {_format_currency(item['amount'])}")
        report_lines.append("")
    
    # Expense breakdown
    if expense_breakdown:
        report_lines.append("💸 *EXPENSE CATEGORIES*")
        for item in expense_breakdown[:3]:  # Show top 3
            report_lines.append(f"• {item['category']}: {_format_currency(item['amount'])}")
        report_lines.append("")
    
    # Insights
    report_lines.append("🔍 *KEY INSIGHTS*")
    
    if biggest_expense:
        report_lines.append(f"Biggest Expense: {_format_currency(biggest_expense['amount'])} ({biggest_expense['description']})")
    
    if top_expense_category:
        report_lines.append(f"Top Spending Category: {top_expense_category['category']} ({_format_currency(top_expense_category['amount'])})")
    
    if income_total > 0:
        report_lines.append(f"Suggested Tithe (10%): {_format_currency(tithe_recommendation)}")
    
    # Financial health indicator
    report_lines.append("")
    if net_savings > 0:
        report_lines.append("✅ Great job! You're saving money this period.")
    elif net_savings == 0:
        report_lines.append("⚖️ You're breaking even this period.")
    else:
        report_lines.append("⚠️ You're spending more than you earn this period.")
    
    # Footer
    report_lines.append("")
    report_lines.append("Keep tracking to build better financial habits! 💪")
    
    return "\n".join(report_lines)


# Additional utility functions for potential future use

def get_spending_trend(transactions: List[Dict[str, Any]], days: int = 30) -> Dict[str, float]:
    """
    Calculate daily spending trend over the specified number of days.
    
    Returns:
        Dict with 'daily_average', 'trend_direction' (-1, 0, 1), 'trend_strength'
    """
    if not transactions:
        return {'daily_average': 0.0, 'trend_direction': 0, 'trend_strength': 0.0}
    
    df = pd.DataFrame(transactions)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
    
    # Filter to expenses only and last N days
    cutoff = datetime.now() - timedelta(days=days)
    expense_df = df[(df['type'].str.lower() == 'expense') & (df['timestamp'] >= cutoff)]
    
    if expense_df.empty:
        return {'daily_average': 0.0, 'trend_direction': 0, 'trend_strength': 0.0}
    
    # Group by date and sum daily expenses
    daily_expenses = expense_df.groupby(expense_df['timestamp'].dt.date)['amount'].sum()
    daily_average = daily_expenses.mean()
    
    # Simple trend calculation (comparing first and second half)
    if len(daily_expenses) < 4:
        return {'daily_average': daily_average, 'trend_direction': 0, 'trend_strength': 0.0}
    
    mid_point = len(daily_expenses) // 2
    first_half_avg = daily_expenses.iloc[:mid_point].mean()
    second_half_avg = daily_expenses.iloc[mid_point:].mean()
    
    trend_direction = 1 if second_half_avg > first_half_avg else -1 if second_half_avg < first_half_avg else 0
    trend_strength = abs(second_half_avg - first_half_avg) / first_half_avg if first_half_avg > 0 else 0.0
    
    return {
        'daily_average': daily_average,
        'trend_direction': trend_direction,
        'trend_strength': trend_strength
    }


def calculate_budget_recommendations(transactions: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Generate budget recommendations based on historical spending patterns.
    
    Returns:
        Dict with category-based budget suggestions
    """
    if not transactions:
        return {}
    
    df = pd.DataFrame(transactions)
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
    
    # Focus on last 90 days of expenses
    cutoff = datetime.now() - timedelta(days=90)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    expense_df = df[(df['type'].str.lower() == 'expense') & (df['timestamp'] >= cutoff)]
    
    if expense_df.empty:
        return {}
    
    # Calculate monthly average spending per category
    monthly_spending = expense_df.groupby('category')['amount'].sum() / 3  # 3 months
    
    # Add 10% buffer for budgeting
    budget_recommendations = (monthly_spending * 1.1).round(2).to_dict()
    
    return budget_recommendations