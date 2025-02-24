import pika
import json
import smtplib
import os
from dotenv import load_dotenv

load_dotenv()

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='notification_queue', durable=True)

#function to send an email securely
def send_email(user_email, message):
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, user_email, f"Subject: Notification\n\n{message}")
        server.quit()
        print(f"  Email sent to {user_email}: {message}")
    except Exception as e:
        print(f"  Email Error: {e}")

#function to process incoming messages

def callback(ch, method, properties, body):
    notification = json.loads(body)
    event_type = notification["event_type"]
    user_id = notification["user_id"]
    message = notification["message"]

    print(f" Received Notification for User {user_id}: {message}")

    # send an email
    user_email = EMAIL_USER
    send_email(user_email, message)

# subscribe to the queue so we can listen for the messages
channel.basic_consume(queue='notification_queue', on_message_callback=callback)

print(' Waiting for notifications...')
channel.start_consuming()