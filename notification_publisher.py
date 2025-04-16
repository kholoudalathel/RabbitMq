
import json
import pika
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")

# Set up authentication
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
channel = connection.channel()

# Declare only the exchange
channel.exchange_declare(exchange='task_exchange', exchange_type='direct')

# Function to send notifications
def send_notification(event_type, user_id, message):
    notification = {
        'event_type': event_type,
        'user_id': user_id,
        'message': message
    }

    channel.basic_publish(
        exchange='task_exchange',
        routing_key='notifications',
        body=json.dumps(notification),
        properties=pika.BasicProperties(delivery_mode=2)  # Persistent messages
    )

    print(f" Sent notification: {notification}")

# Example messages
send_notification("property_status_changes", 100, "The property has been sold!")
send_notification("new_message", 101, "You have received a new message from the agent!")

# Close the connection properly
connection.close()

