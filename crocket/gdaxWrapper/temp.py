import gdax
import time


class MyWebsocketClient(gdax.WebsocketClient):

    def on_open(self):

        self.url = "wss://ws-feed.gdax.com/"
        self.products = ["LTC-USD"]
        self.message_count = 0
        self.channels = ['full']
        print("Lets count the messages!")

    def on_message(self, msg):
        self.message_count += 1
        print(msg)
        if 'price' in msg and 'type' in msg:
            print(self.message_count, "Message type:", msg["type"],
                   "\t@ {:.3f}".format(float(msg["price"])))

    def on_close(self):
        print("-- Goodbye! --")


wsClient = MyWebsocketClient()
wsClient.start()

print(wsClient.url, wsClient.products)

while wsClient.message_count < 10:

    print("\nmessage_count =", "{} \n".format(wsClient.message_count))
    time.sleep(1)

wsClient.close()
