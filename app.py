#!/usr/bin/env python3
"""
Messenger Wallet Bot - Production-Ready Flask Application
WSGI-compatible web server for hosting platforms (Heroku, Railway, Render, etc.)
"""

import os
import sys
import logging
import signal
import atexit
from flask import Flask, request, jsonify
from datetime import datetime
from werkzeug.middleware.proxy_fix import ProxyFix
import threading
import time

# Add the parent directory to the Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import application modules
from wallet_bot.config.settings import (
    Config, 
    validate_configuration,
    get_verify_token,
    get_page_access_token
)
from wallet_bot.messenger.handler import process_webhook_message
from wallet_bot.sheets.handler import initialize_sheets, test_sheets_connection

# Global application state
_app_initialized = False
_initialization_lock = threading.Lock()

def setup_logging():
    """
    Configure production-ready logging with different levels for different environments.
    """
    # Determine log level from environment
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    if log_level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        log_level = 'INFO'
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        handlers=[
            # Always log to stdout for hosting platforms
            logging.StreamHandler(sys.stdout)
        ] + ([logging.FileHandler('wallet_bot.log')] if os.getenv('ENABLE_FILE_LOGGING', 'false').lower() == 'true' else [])
    )
    
    # Set specific loggers for better production debugging
    logging.getLogger('werkzeug').setLevel(logging.WARNING)  # Reduce Flask noise
    logging.getLogger('urllib3').setLevel(logging.WARNING)   # Reduce requests noise
    
    return logging.getLogger(__name__)

logger = setup_logging()

def create_app():
    """
    Application factory pattern for better testing and deployment.
    """
    app = Flask(__name__)
    
    # Production configurations
    app.config.update({
        'ENV': os.getenv('FLASK_ENV', 'production'),
        'DEBUG': os.getenv('DEBUG', 'false').lower() == 'true',
        'TESTING': False,
        'SECRET_KEY': os.getenv('SECRET_KEY', os.urandom(24)),
        'JSON_SORT_KEYS': False,
        'JSONIFY_PRETTYPRINT_REGULAR': False,  # Disable in production
    })
    
    # Add proxy fix for hosting platforms (handles X-Forwarded headers)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
    
    # Load config
    config = Config()
    
    def ensure_initialization():
        """
        Thread-safe initialization that only runs once.
        """
        global _app_initialized
        
        if _app_initialized:
            return True
            
        with _initialization_lock:
            if _app_initialized:
                return True
                
            try:
                logger.info("🚀 Initializing Messenger Wallet Bot...")
                
                # Validate all configuration settings
                logger.info("📋 Validating configuration...")
                validate_configuration()
                logger.info("✅ Configuration validated successfully")
                
                # Test Google Sheets connection with retry logic
                logger.info("📊 Testing Google Sheets connection...")
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        test_sheets_connection()
                        logger.info("✅ Google Sheets connection successful")
                        break
                    except Exception as e:
                        if attempt == max_retries - 1:
                            raise
                        logger.warning(f"Sheets connection attempt {attempt + 1} failed, retrying...")
                        time.sleep(2 ** attempt)  # Exponential backoff
                
                # Initialize sheets with proper headers if needed
                logger.info("📋 Initializing sheet structure...")
                initialize_sheets()
                logger.info("✅ Sheets initialized successfully")
                
                _app_initialized = True
                logger.info("🎉 Application initialization complete!")
                return True
                
            except Exception as e:
                logger.error(f"❌ Application initialization failed: {str(e)}")
                # In production, don't fail completely - allow health checks to work
                if config.is_development():
                    raise
                return False

    @app.before_first_request
    def initialize_on_first_request():
        """Initialize the app on first request for hosting platforms."""
        ensure_initialization()

    @app.route('/', methods=['GET'])
    def health_check():
        """
        Simple health check endpoint for hosting platforms.
        """
        return jsonify({
            'status': 'healthy',
            'service': 'Messenger Wallet Bot',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'version': '1.0.0',
            'environment': app.config['ENV']
        })

    @app.route('/health', methods=['GET'])
    def detailed_health():
        """
        Detailed health check for load balancers and monitoring.
        """
        try:
            # Basic checks
            health_status = {
                'status': 'healthy',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'checks': {
                    'initialization': _app_initialized,
                    'database': False,
                    'configuration': False
                }
            }
            
            # Test configuration
            try:
                validate_configuration()
                health_status['checks']['configuration'] = True
            except Exception as e:
                logger.warning(f"Configuration check failed: {e}")
            
            # Test database connectivity
            try:
                test_sheets_connection()
                health_status['checks']['database'] = True
            except Exception as e:
                logger.warning(f"Database check failed: {e}")
            
            # Determine overall status
            all_healthy = all(health_status['checks'].values())
            if not all_healthy:
                health_status['status'] = 'degraded'
            
            return jsonify(health_status), 200 if all_healthy else 503
            
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return jsonify({
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }), 500

    @app.route('/webhook', methods=['GET'])
    def webhook_verification():
        """
        Handle Meta's webhook verification process.
        """
        try:
            # Get verification parameters from Meta
            verify_token = request.args.get('hub.verify_token')
            challenge = request.args.get('hub.challenge')
            mode = request.args.get('hub.mode')
            
            logger.info(f"📥 Webhook verification request received")
            
            # Verify that this is a valid subscription request
            if mode == 'subscribe' and verify_token == get_verify_token():
                logger.info("✅ Webhook verification successful")
                return challenge, 200
            else:
                logger.warning("❌ Webhook verification failed - invalid verify token")
                return 'Forbidden', 403
                
        except Exception as e:
            logger.error(f"❌ Error during webhook verification: {str(e)}")
            return 'Internal Server Error', 500

    @app.route('/webhook', methods=['POST'])
    def webhook_handler():
        """
        Handle incoming webhook messages from Meta.
        """
        try:
            # Ensure app is initialized
            if not _app_initialized:
                ensure_initialization()
            
            # Get the JSON payload from Meta
            data = request.get_json()
            
            if not data:
                logger.warning("❌ Received empty webhook payload")
                return 'Bad Request', 400
            
            logger.info("📥 Processing webhook message")
            
            # Process the webhook message through our conversation handler
            result = process_webhook_message(data)
            
            if result:
                logger.info("✅ Message processed successfully")
            else:
                logger.warning("⚠️ Message processing returned False")
            
            return 'OK', 200  # Always return 200 to prevent Meta retries
            
        except Exception as e:
            logger.error(f"❌ Error processing webhook message: {str(e)}")
            logger.error(f"Request data: {data if 'data' in locals() else 'N/A'}")
            
            # Return 200 to prevent Meta from retrying failed messages
            return 'OK', 200

    @app.errorhandler(404)
    def not_found(error):
        """Handle 404 errors"""
        logger.warning(f"404 - Path not found: {request.path}")
        return jsonify({'error': 'Endpoint not found'}), 404

    @app.errorhandler(500)
    def internal_error(error):
        """Handle 500 errors"""
        logger.error(f"500 - Internal server error: {str(error)}")
        return jsonify({'error': 'Internal server error'}), 500

    @app.errorhandler(Exception)
    def handle_exception(e):
        """Handle all unhandled exceptions"""
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

    # Graceful shutdown handlers
    def cleanup():
        """Cleanup function for graceful shutdown"""
        logger.info("🧹 Performing cleanup...")
        
    atexit.register(cleanup)
    
    def signal_handler(signum, frame):
        """Handle shutdown signals"""
        logger.info(f"📡 Received signal {signum}, shutting down gracefully...")
        cleanup()
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    return app

# Create the Flask app instance (required for WSGI servers)
app = create_app()

def main():
    """
    Development server entry point.
    In production, use a WSGI server like Gunicorn.
    """
    try:
        config = Config()
        
        logger.info("=" * 60)
        logger.info("🤖 MESSENGER WALLET BOT - DEVELOPMENT SERVER")
        logger.info("=" * 60)
        logger.info("⚠️  For production, use: gunicorn wallet_bot.app:app")
        logger.info("=" * 60)
        
        # Initialize the application
        from wallet_bot.app import app
        with app.app_context():
            ensure_initialization = app.view_functions['webhook_handler'].__globals__['ensure_initialization']
            if not ensure_initialization():
                logger.error("❌ Failed to initialize application. Exiting.")
                sys.exit(1)
        
        # Log configuration info
        logger.info(f"🌐 Server starting on {config.HOST}:{config.PORT}")
        logger.info(f"🔧 Debug mode: {config.is_development()}")
        logger.info(f"📊 Google Sheet configured")
        logger.info(f"🔗 Webhook endpoint: {config.WEBHOOK_ENDPOINT}")
        logger.info("✅ Ready to receive webhook messages!")
        
        # Start the Flask development server
        app.run(
            host=config.HOST,
            port=int(config.PORT),
            debug=config.is_development(),
            threaded=True,
            use_reloader=False  # Disable reloader in production
        )
        
    except KeyboardInterrupt:
        logger.info("\n👋 Development server stopped")
    except Exception as e:
        logger.error(f"❌ Fatal error starting development server: {str(e)}")
        sys.exit(1)

# WSGI entry point for production servers
application = app

if __name__ == '__main__':
    main()