class OrderBook:
    """
    Implements data structure to append messages one at a time
    """
    # bids, asks
    book = (dict(), dict())
    
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
                