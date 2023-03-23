import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from logging_config import write_to_log

dir = '/tmp/pycharm_project_681'

# Get configuration data
with open(f'{dir}/config.json') as f:
    config = json.load(f)


# General function for sending email by gmail
def send_email(recipient, subject, body, message):
    msg = MIMEMultipart()
    msg['From'] = 'Naya Trades'
    msg['To'] = recipient
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))
    # Connect to the SMTP server
    server = smtplib.SMTP(config['email']['smtp_server'], config['email']['smtp_port'])
    server.starttls()
    server.login(config['email']['sender'], config['email']['password'])
    # Send the email
    text = msg.as_string()
    server.sendmail(msg['From'], msg['To'], text)
    # Close the connection to the SMTP server
    server.quit()

    write_to_log('sending an email', message)
