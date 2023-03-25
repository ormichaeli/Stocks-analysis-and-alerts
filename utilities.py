import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from logs.logging_config import write_to_log

# Get configuration data
with open('/tmp/pycharm_project_301/config.json') as f:
    config = json.load(f)


# Define a function for sending an email
def send_email(recipient, subject, body, message):
    # Create a MIMEMultipart message object with appropriate headers
    msg = MIMEMultipart()
    msg['From'] = 'Naya Trades'
    msg['To'] = recipient
    msg['Subject'] = subject
    # Attach the HTML body to the message
    msg.attach(MIMEText(body, 'html'))

    # Connect to the SMTP server
    server = smtplib.SMTP(config['email']['smtp_server'], config['email']['smtp_port'])
    # Start a secure connection
    server.starttls()
    # Login with the credentials specified in the configuration
    server.login(config['email']['sender'], config['email']['password'])
    # Convert the message to a string and send it
    text = msg.as_string()
    server.sendmail(msg['From'], msg['To'], text)
    # Close the connection to the SMTP server
    server.quit()
    # Write a log message indicating that an email was sent
    write_to_log('sending an email', message)