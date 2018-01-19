import json
import time


REDIS_KEY = {
    'actions': 'ion-mq:actions',
    'outbox': 'ion-mq:outbox',
    'listeners': 'ion-mq:listeners',
    'inbox': 'ion-mq:inbox:%s:%s'  # % (channel_name, client_id)
}


class IonMQBroker(object):
    def __init__(self, redis, loop_delay=0.5):
        self._redis = redis
        self._listener_map = {}
        self._loop_delay = loop_delay

    def action_handler(self):
        actions = self._redis.lrange(REDIS_KEY['actions'], 0, -1) or []
        if actions:
            self._redis.ltrim(REDIS_KEY['actions'], len(actions), -1)
            self._pull_listener_map()

            for a in map(lambda a: json.loads(a.decode('utf-8')), actions):
                self._manage_subscription(
                    active=a['action'] == 'subscribe',
                    client=a['client'],
                    channel=a['channel']
                )

            self._remove_empty_channels()
            self._push_listener_map()

    def message_handler(self):
        messages = self._redis.lrange(REDIS_KEY['outbox'], 0, -1) or []
        if messages:
            self._redis.ltrim(REDIS_KEY['outbox'], len(messages), -1)

            for m in map(lambda m: (m, json.loads(m.decode('utf-8'))),
                         messages):
                decoded_message = json.loads(m.decode('utf-8'))
                subscribers = list(
                    self._listener_map[decoded_message['channel']]
                )

                if decoded_message['client'] in subscribers:
                    subscribers.remove(decoded_message['client'])

                for subscriber in subscribers:
                    channel_name = REDIS_KEY['inbox'] % (
                        decoded_message['channel'],
                        subscriber
                    )
                    self._redis.rpush(channel_name, m)

    def start(self):
        self._pull_listener_map()

        while True:
            start_time = time.time()
            self.action_handler()
            self.message_handler()
            delay = max(0, self._loop_delay - (time.time() - start_time))
            time.sleep(delay)

    def _pull_listener_map(self):
        raw = self._redis.get(REDIS_KEY['listeners'])
        raw = raw.decode('utf-8') if raw else "{}"
        self._listener_map = json.loads(raw)

    def _push_listener_map(self):
        self._redis.set(
            REDIS_KEY['listeners'],
            json.dumps(self._listener_map).encode('utf-8')
        )

    def _manage_subscription(self, active, client, channel):
        if channel not in self._listener_map:
            self._listener_map[channel] = []

        if active and client not in self._listener_map[channel]:
            self._listener_map[channel].append(client)
        elif client in self._listener_map[channel] and not active:
            self._listener_map[channel].remove(client)
            self._redis.delete(REDIS_KEY['inbox'] % (channel, client))

    def _remove_empty_channels(self):
        for channel in dict(self._listener_map):
            if not self._listener_map[channel]:
                self._listener_map.pop(channel)


class IonMQClient(object):
    def __init__(self, imq_client_id, redis):
        self._redis = redis
        self._calback_map = {}
        self.imq_client_id = imq_client_id

    def subscribe(self, channel, callback):
        if not callable(callback):
            raise NotImplementedError('Callback must be callable.')

        request = json.dumps({
            'client': self.imq_client_id,
            'channel': channel,
            'action': 'subscribe',
        })

        self._redis.rpush(REDIS_KEY['actions'], request.encode('utf-8'))
        self._calback_map[channel] = callback

    def unsubscribe(self, channel):
        request = json.dumps({
            'client': self.imq_client_id,
            'channel': channel,
            'action': 'unsubscribe',
        })

        self._redis.rpush(REDIS_KEY['actions'], request.encode('utf-8'))
        self._calback_map.pop(channel)

    def unsubscribe_from_all(self):
        for channel in list(self._calback_map.keys()):
            self.unsubscribe(channel)

    def listen(self):
        received = {}

        for channel in self._calback_map:
            inbox = REDIS_KEY['inbox'] % (channel, self.imq_client_id)
            messages = self._redis.lrange(inbox, 0, -1) or []
            if messages:
                self._redis.ltrim(inbox, len(messages), -1)
                received[channel] = (m.decode('utf-8') for m in messages)

        for channel, messages in received.items():
            callback = self._calback_map[channel]
            for msg in messages:
                callback(message=msg)

    def publish(self, channel, message):
        request = json.dumps({
            'client': self.imq_client_id,
            'channel': channel,
            'message': message,
        })

        self._redis.rpush(REDIS_KEY['outbox'], request.encode('utf-8'))
