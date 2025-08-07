import os
import json
from flask import Flask, render_template, request, Response
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

# Set up GCP project and credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\DELL\Downloads\chat-468309-6376e7e6738a.json"
PROJECT_ID = "chat-468309"
TOPIC_ID = "chat-topic"
SUBSCRIPTION_ID = "chat-subscription"

app = Flask(__name__)

# Pub/Sub clients
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/publish', methods=['POST'])
def publish_message():
    data = request.json
    message = data.get('message', '')
    username = data.get('username', 'Anonymous')

    # Prepare the message payload
    payload = json.dumps({'username': username, 'message': message}).encode('utf-8')

    # Publish the message to the Pub/Sub topic
    future = publisher.publish(topic_path, payload)

    try:
        future.result(timeout=5)
        return {"status": "success", "message_id": future.result()}, 200
    except TimeoutError:
        return {"status": "error", "message": "Publish timeout"}, 500


@app.route('/stream')
def stream():
    def generate():
        while True:
            response = subscriber.pull(
                subscription=subscription_path,
                max_messages=1,
                timeout=5
            )

            if response.received_messages:
                for received_message in response.received_messages:
                    message_data = json.loads(received_message.message.data)
                    yield f"data: {json.dumps(message_data)}\n\n"
                    ack_ids = [received_message.ack_id]
                    subscriber.acknowledge(
                        request={"subscription": subscription_path, "ack_ids": ack_ids}
                    )
            else:
                yield ": keep-alive\n\n"

    return Response(generate(), mimetype='text/event-stream')


if __name__ == '__main__':
    app.run(debug=True, threaded=True)
