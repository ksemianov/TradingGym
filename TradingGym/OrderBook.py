class OrderBook:
    """
    Implements data structure to append messages one at a time
    """
    def __init__(self):
        # bids, asks
        self.book = (dict(), dict())
    
    def bestBid(self):
        if not self.book[0].keys():
            return (float('nan'), float('nan'))
        price = max(self.book[0].keys())
        return (price, self.book[0][price])
    
    def bestAsk(self):
        if not self.book[1].keys():
            return (float('nan'), float('nan'))
        price = min(self.book[1].keys())
        return (price, self.book[1][price])
    
    def update(self, message):
        buySell = 'Buy' in message.Flags
        addDel = 'Add' in message.Flags
        price = message.Price
        amount = message.Amount
        amountRest = message.AmountRest
        if addDel:
            if not price in self.book[1-buySell]:
                self.book[1-buySell][price] = 0
            self.book[1-buySell][price] += amount
        else:
            self.book[1-buySell][price] -= amount
            if self.book[1-buySell][price] < 0:
                raise RuntimeError('Negative ammount is generated in order book')
            if self.book[1-buySell][price] == 0:
                del self.book[1-buySell][price]
                
    def updateBulk(self, messages):
        for name, message in messages.iterrows():
            self.update(message)
                