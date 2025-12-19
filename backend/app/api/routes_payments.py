import stripe
from flask import Blueprint, jsonify, request, redirect, current_app
from flask_login import login_required, current_user
from app.models.user_model import User
from app.core.database import db
import os

payments_bp = Blueprint('payments', __name__)

# Configure Stripe
stripe.api_key = os.environ.get('STRIPE_SECRET_KEY')
endpoint_secret = os.environ.get('STRIPE_WEBHOOK_SECRET')

DOMAIN = 'http://localhost:5000'

@payments_bp.route('/create-checkout-session', methods=['POST'])
@login_required
def create_checkout_session():
    try:
        data = request.get_json()
        price_id = data.get('priceId') # e.g., 'price_123...'

        # In a real app, map plan names to Stripe Price IDs
        # For demo purposes, we accept any dummy ID or mapping here
        # IF using test mode keys, use real Price IDs from your Stripe Dashboard
        
        # Fallback for demo without real keys
        if not stripe.api_key:
             return jsonify({'error': 'Stripe not configured'}), 500

        checkout_session = stripe.checkout.Session.create(
            customer_email=current_user.email,
            client_reference_id=str(current_user.id),
            line_items=[
                {
                    # Provide the exact Price ID (for example, pr_1234) of the product you want to sell
                    'price': price_id,
                    'quantity': 1,
                },
            ],
            mode='subscription',
            success_url=DOMAIN + '/app?success=true',
            cancel_url=DOMAIN + '/app?canceled=true',
        )
        return jsonify({'url': checkout_session.url})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@payments_bp.route('/webhook', methods=['POST'])
def webhook():
    event = None
    payload = request.data
    sig_header = request.headers.get('STRIPE_SIGNATURE')

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, endpoint_secret
        )
    except ValueError as e:
        # Invalid payload
        return jsonify({'error': 'Invalid payload'}), 400
    except stripe.error.SignatureVerificationError as e:
        # Invalid signature
        return jsonify({'error': 'Invalid signature'}), 400

    # Handle the event
    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        
        # Fulfill the purchase...
        user_id = session.get('client_reference_id')
        stripe_customer_id = session.get('customer')
        
        if user_id:
            user = User.query.get(int(user_id))
            if user:
                user.stripe_customer_id = stripe_customer_id
                user.subscription_status = 'active'
                db.session.commit()
                print(f"User {user.email} subscription activated!")

    return jsonify({'status': 'success'}), 200
