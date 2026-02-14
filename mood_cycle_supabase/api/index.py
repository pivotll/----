from app import app

# This is required for Vercel to find the Flask app instance
# Vercel looks for a variable named 'app' or 'application' in the entry point
application = app
