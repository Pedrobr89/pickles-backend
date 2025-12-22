"""
Email service for sending password recovery emails
"""

from flask_mail import Mail, Message
from flask import current_app
import logging

logger = logging.getLogger(__name__)

mail = Mail()

def send_password_reset_email(user_email, reset_token):
    """
    Sends a password recovery email with reset link
    
    Args:
        user_email (str): User's email address
        reset_token (str): Unique reset token
    
    Returns:
        bool: True if email sent successfully, False otherwise
    """
    try:
        # Construct reset URL
        reset_url = f"http://{current_app.config['API_HOST']}:{current_app.config['API_PORT']}/reset-senha?token={reset_token}"
        
        # Create message
        msg = Message(
            subject="Recuperação de Senha - Pickles Analytics",
            sender=current_app.config['MAIL_DEFAULT_SENDER'],
            recipients=[user_email]
        )
        
        # HTML email body
        msg.html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 600px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                .container {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    padding: 40px;
                    border-radius: 10px;
                }}
                .content {{
                    background: white;
                    padding: 30px;
                    border-radius: 8px;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                }}
                h1 {{
                    color: #667eea;
                    margin-top: 0;
                }}
                .button {{
                    display: inline-block;
                    padding: 12px 30px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    text-decoration: none;
                    border-radius: 5px;
                    margin: 20px 0;
                    font-weight: bold;
                }}
                .button:hover {{
                    opacity: 0.9;
                }}
                .footer {{
                    margin-top: 20px;
                    padding-top: 20px;
                    border-top: 1px solid #eee;
                    font-size: 12px;
                    color: #666;
                }}
                .warning {{
                    background: #fff3cd;
                    border-left: 4px solid #ffc107;
                    padding: 12px;
                    margin: 20px 0;
                    border-radius: 4px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="content">
                    <h1>🔒 Recuperação de Senha</h1>
                    
                    <p>Olá,</p>
                    
                    <p>Recebemos uma solicitação para redefinir a senha da sua conta no <strong>Pickles Analytics</strong>.</p>
                    
                    <p>Para criar uma nova senha, clique no botão abaixo:</p>
                    
                    <center>
                        <a href="{reset_url}" class="button">Redefinir Senha</a>
                    </center>
                    
                    <p>Ou copie e cole este link no seu navegador:</p>
                    <p style="word-break: break-all; background: #f5f5f5; padding: 10px; border-radius: 4px; font-size: 12px;">
                        {reset_url}
                    </p>
                    
                    <div class="warning">
                        <strong>⚠️ Importante:</strong> Este link expira em 1 hora por motivos de segurança.
                    </div>
                    
                    <p>Se você não solicitou a redefinição de senha, ignore este email. Sua senha permanecerá inalterada.</p>
                    
                    <div class="footer">
                        <p>Este é um email automático, por favor não responda.</p>
                        <p>© 2024 Pickles Analytics - Análise Inteligente de Dados</p>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Plain text version
        msg.body = f"""
        Recuperação de Senha - Pickles Analytics
        
        Olá,
        
        Recebemos uma solicitação para redefinir a senha da sua conta no Pickles Analytics.
        
        Para criar uma nova senha, acesse o link abaixo:
        {reset_url}
        
        IMPORTANTE: Este link expira em 1 hora por motivos de segurança.
        
        Se você não solicitou a redefinição de senha, ignore este email. Sua senha permanecerá inalterada.
        
        ---
        Este é um email automático, por favor não responda.
        © 2024 Pickles Analytics
        """
        
        # Send email
        mail.send(msg)
        logger.info(f"Password reset email sent to {user_email}")
        return True
        
    except Exception as e:
        logger.error(f"Error sending password reset email to {user_email}: {str(e)}")
        return False
