
import pika
import json
import smtplib
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_USER = os.getenv("RABBITMQ_CONSUMER_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_CONSUMER_PASS")
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

# Set up authentication
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
channel = connection.channel()

# Declare exchanges (ensuring they exist)
channel.exchange_declare(exchange='task_exchange', exchange_type='direct')
channel.exchange_declare(exchange='dlx', exchange_type='direct')  # Declare DLX for failed messages
# Declare queues
channel.queue_declare(
    queue='notification_queue',
    durable=True,
    arguments={'x-dead-letter-exchange': 'dlx', 'x-dead-letter-routing-key': 'dead_letter'}  # DLQ binding
)
channel.queue_bind(exchange='task_exchange', queue='notification_queue', routing_key='notifications')

# Declare Dead Letter Queue (DLQ)
channel.queue_declare(queue='dead_letter_queue', durable=True)
channel.queue_bind(exchange='dlx', queue='dead_letter_queue', routing_key='dead_letter')

# Function to send an email
def send_email(user_email, message):
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_USER, user_email, f"Subject: Notification\n\n{message}")
        print(f" Email sent to {user_email}: {message}")
        return True  # Email sent successfully
    except Exception as e:
        print(f" Email Error: {e}")
        return False  # Email failed

# Function to process messages
def callback(ch, method, properties, body):
    try:
        # Parse message
        notification = json.loads(body)
        event_type = notification.get("event_type")
        user_id = notification.get("user_id")
        message = notification.get("message")

        print(f" Received Notification for User {user_id}: {message}")

        # Send email
        user_email = EMAIL_USER  # Replace with real user email if needed
        email_success = send_email(user_email, message)

        if email_success:
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge successful message
            print("  Message processed and acknowledged.\n")
        else:
            print(" Retrying message...\n")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)  #Retry message

    except json.JSONDecodeError:
        print(" Invalid message format. Discarding message.")
        ch.basic_ack(delivery_tag=method.delivery_tag)  # Remove invalid message

    except Exception as e:
        print(f" Unexpected error: {str(e)}")
        if method.redelivered:
            print(" Message failed multiple times. Sending to Dead Letter Queue.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  # Move to DLQ
        else:
            print(" Retrying message...\n")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)  # Retry message

# Ensure fair message distribution
channel.basic_qos(prefetch_count=1)

# Subscribe to queue
channel.basic_consume(queue='notification_queue', on_message_callback=callback)

print(' Waiting for notifications...')
channel.start_consuming()
