import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='notification_queue', durable=True)

def send_notification(event_type, user_id, message):
        notification = {
            'event_type': event_type,
            'user_id': user_id,
            'message': message
        }

        channel.basic_publish(
            exchange='',
            routing_key='notification_queue',
            body=json.dumps(notification),
            properties=pika.BasicProperties(delivery_mode=2) #Make the message persistent
        )

        print(f" Sent notification {notification}")

# example to send a notification
send_notification("property_status_changes", 100, "the property has been sold!")
send_notification("new_message", 101, "you have received a new message from the agent!")

connection.close()

