from datetime import datetime
import websocket
import pandas as pd

from multiprocessing.dummy import Process as Thread
import json
import logging

LOG = logging.getLogger(__name__)

COLUMNS = ['currencyPair', 'last', 'lowestAsk', 'highestBid', 'percentChange',
           'baseVolume', 'quoteVolume', 'isFrozen', '24hrHigh', '24hrLow', 'timestamp']

data_path = 'data_collection/poloniex.csv'


class PoloniexWebsocket(object):
    def __init__(self):
        self.data = pd.DataFrame(columns=COLUMNS)

        self._ws = websocket.WebSocketApp("wss://api2.poloniex.com/",
                                          on_open=self.on_open,
                                          on_message=self.on_message,
                                          on_error=self.on_error,
                                          on_close=self.on_close)

        self.message_nr = 0
        self.file_exists = False

    def on_message(self, ws, message):
        message = json.loads(message)
        if 'error' in message:
            LOG.debug(message['error'])

        if message[0] == 1002:
            if message[1] == 1:
                return LOG.debug('Subscribed to ticker')

            if message[1] == 0:
                return LOG.debug('Unsubscribed to ticker')

            data = message[2]
            data.append(datetime.now().replace(microsecond=0))
            df = pd.DataFrame([data])
            df.columns = COLUMNS
            self.data = pd.concat([self.data, df])
            self.message_nr += 1

            if self.message_nr % 1000 == 0:
                print('Everything ok on', self.message_nr)
            if self.message_nr == 5000:
                if self.file_exists:
                    data_file = pd.read_csv(data_path)
                    data_file = pd.concat([data_file, self.data])
                    data_file.to_csv(data_path, index=False)
                else:
                    self.data.to_csv(data_path, index=False)
                    self.file_exists = True

                self.data = pd.DataFrame()
                self.message_nr = 0
                print('File (re)saved')

    def on_error(self, ws, error):
        self.data.to_csv(data_path, index=False)
        LOG.debug(error)

    def on_close(self, ws):
        if self._t._running:
            try:
                self.stop()
            except Exception as e:
                LOG.debug(e)
            try:
                self.start()
            except Exception as e:
                LOG.debug(e)
                self.stop()
        else:
            LOG.debug('Websocket closed!')

    def on_open(self, ws):
        self._ws.send(json.dumps({'command': 'subscribe', 'channel': 1002}))

    @property
    def status(self):
        """
        Returns True if the websocket is running, False if not
        """
        try:
            return self._t._running
        except:
            return False

    def start(self):
        """
         Run websocket in a thread
         """
        self._t = Thread(target=self._ws.run_forever)
        self._t.daemon = True
        self._t._running = True
        self._t.start()
        LOG.debug('Websocket thread started')

    def stop(self):
        """ Stop/join the websocket thread """
        self._t._running = False
        self._ws.close()
        self._t.join()
        LOG.debug('Websocket thread stopped/joined')
