import datetime
import asyncio
import time

import ccxt
from elasticsearch import Elasticsearch

COINS_OF_INTEREST = ['BTC', 'ETH', 'USDT', 'BNB', 'USDC', 'ADA', 'SOL', 'XRP', 'LUNA', 'DOT', 'DODGE', 'AVAX', 'BUSD',
                     'MATIC', 'SHIB', 'UST', 'BCH', 'RVN', 'USD']


class CryptoPolling():
    def __init__(self, base: str, minimum: float):
        self.exchanges = []
        self.base = base
        self.minimum = minimum
        self.es = Elasticsearch([{"host": "10.0.0.55"}], http_auth=("cryptobot", "kukuriku99"))
        self.exchanges.append({"exchange": ccxt.binanceus()})
        self.exchanges.append({"exchange": ccxt.coinbase()})
        self.exchanges.append({"exchange": ccxt.kraken()})
        self.exchanges.append({"exchange": ccxt.cryptocom()})
        self.exchanges.append({"exchange": ccxt.gemini()})
        self.exchanges.append({"exchange": ccxt.gateio()})
        self.exchanges.append({"exchange": ccxt.bitstamp()})
        self.exchanges.append({"exchange": ccxt.bittrex()})
        self.exchanges.append({"exchange": ccxt.bitflyer()})
        self.exchanges.append({"exchange": ccxt.aax()})
        self.exchanges.append({"exchange": ccxt.huobi()})
        self.exchanges.append({"exchange": ccxt.ascendex()})
        self.exchanges.append({"exchange": ccxt.bitmart()})
        # self.exchanges.append({"exchange": ccxt.bitvavo()})
        self.exchanges.append({"exchange": ccxt.currencycom()})
        self.exchanges.append({"exchange": ccxt.ftx()})
        self.exchanges.append({"exchange": ccxt.ftxus()})
        self.exchanges.append({"exchange": ccxt.idex()})
        self.exchanges.append({"exchange": ccxt.mexc()})
        self.exchanges.append({"exchange": ccxt.okex()})
        self.exchanges.append({"exchange": ccxt.wavesexchange()})
        self.exchanges.append({"exchange": ccxt.zb()})

    def start(self):
        for e in self.exchanges:
            client = e['exchange']
            client.load_markets()
            e['pairs'] = self.filter_pairs(e)
            e['name'] = client.name

    def filter_pairs(self, e):
        client = e['exchange']
        result = []
        for symbol in client.symbols:
            if '/' not in symbol:
                continue
            [symbol1, symbol2] = symbol.split('/')
            if symbol1 is None or symbol2 is None:
                continue
            if symbol1 != self.base and symbol2 != self.base:
                continue
            if symbol1 in COINS_OF_INTEREST and symbol2 in COINS_OF_INTEREST:
                result.append(symbol)
        return result

    async def poll_exchange(self, exchange):
        queue = []
        client = exchange['exchange']
        name = exchange['name']
        for pair in exchange['pairs']:
            queue.append(pair)

        while len(queue):
            pair = queue.pop(0)
            now = datetime.datetime.now()
            lastQueryAt = exchange.get('updated') or (now - datetime.timedelta(days=1))
            diffTime = now - lastQueryAt
            if diffTime.total_seconds() * 1000 < client.rateLimit:
                queue.insert(0, pair)
            else:
                if client.has['fetchOrderBook']:
                    try:
                        book = client.fetch_order_book(pair)
                        if len(book['bids']) == 0 or len(book['asks']) == 0:
                            queue.append(pair)
                            continue
                        res = self.calculate_rate(pair, book)
                        client.markets
                    except Exception:
                        queue.insert(0, pair)
                        await asyncio.sleep(5000)
                        continue
                else:
                    try:
                        t = client.fetch_ticker(pair)
                        res = self.calculate_ticker(t)
                    except Exception:
                        queue.insert(0, pair)
                        await asyncio.sleep(5000)
                        continue

                fee = self.calculate_fees(client, pair)
                self.store_result(name, pair, res, fee, now)
                exchange['updated'] = now
                queue.append(pair)
                await asyncio.sleep(client.rateLimit / 1000)

            # Notify Health status if required
            now = datetime.datetime.now()
            live_updated = exchange.get('live_updated') or (now - datetime.timedelta(days=1))
            if (now - live_updated).total_seconds() * 1000 >= 1000:
                self.store_live_event(exchange['name'])
                exchange['live_updated'] = now
            await asyncio.sleep(0.0001)

    @staticmethod
    def calculate_fees(client, pair):
        market = client.markets[pair]
        return market['taker'] if 'taker' in market and isinstance(market['taker'], float) else 0

    def run(self):
        loop = asyncio.get_event_loop()
        try:
            for exchange in self.exchanges:
                asyncio.ensure_future(self.poll_exchange(exchange))
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            print("Closing Loop")
            loop.close()

    def store_result(self, exchange, pair, result, fee, updated):
        try:
            now = datetime.datetime.now()
            doc = {
                "timestamp": datetime.datetime.utcnow(),
                "updated": updated,
                "exchange": exchange,
                "type": "data",
                "pair": pair,
                "ask.price": result['ask']['price'],
                "ask.volume": result['ask']['volume'],
                "bid.price": result['bid']['price'],
                "bid.volume": result['bid']['volume'],
                "fee.percent": fee
            }
            self.es.index(index="crypto-info", id=str(now.timestamp()) + exchange + pair, document=doc)
            print(doc)
        except Exception as e:
            time.sleep(2)
            print(e)

    def store_live_event(self, exchange):
        now = datetime.datetime.now()
        doc = {
            "timestamp": datetime.datetime.utcnow(),
            "exchange": exchange,
            "base": self.base,
            "type": "health"
        }
        self.es.index(index="crypto-health", id=str(now.timestamp()) + exchange + self.base, document=doc)
        print(doc)

    @staticmethod
    def calculate_ticker(ticker):
        return {
            "bid": {
                "price": ticker['bid'],
                "volume": 100000
            },
            "ask": {
                "price": ticker['ask'],
                "volume": 100000
            }
        }

    def calculate_rate(self, pair, order_book):
        bids = order_book['bids']
        asks = order_book['asks']

        volume = 0
        total_price = 0
        for bid in bids:
            volume += bid[1]
            total_price += bid[0] * bid[1]
            if volume > self.minimum:
                break
        bid_price = total_price / volume
        bid_volume = volume
        volume = 0
        total_price = 0
        for ask in asks:
            volume += ask[1]
            total_price += ask[0] * ask[1]
            if volume > self.minimum:
                break
        ask_price = total_price / volume
        ask_volume = volume

        return {
            "bid": {
                "price": bid_price,
                "volume": bid_volume
            },
            "ask": {
                "price": ask_price,
                "volume": ask_volume
            }
        }


if __name__ == "__main__":
    poll = CryptoPolling("ETH", 0.05)
    poll.start()
    poll.run()
