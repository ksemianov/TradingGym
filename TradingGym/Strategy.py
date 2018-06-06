from TradingGym.OrderBook import OrderBook

class Strategy:
    """
    Implements base strategy which holds constant orders
    """
    def __init__(self):
        self.sleep = 100 # ms
    def action(self, position, history, old_book, market_book):
        """Override this method in subclasses"""
        new_book = OrderBook()
        new_book.book = (old_book.book[0].copy(), old_book.book[1].copy())
        return new_book, self.sleep # order-book held by our strategy and time until next rebalancing
    
class SpreadStrategy(Strategy):
    """
    Implements strategy which places orders on best bid-ask prices 
    """
    def __init__(self, value = 10, offset = 10):
        super().__init__()
        self.value = value
        self.offset = offset
    def action(self, position, history, old_book, market_book):
        new_book = OrderBook()
        new_book.book[0][max(market_book.book[0].keys()) - self.offset] = self.value
        new_book.book[1][min(market_book.book[1].keys()) + self.offset] = self.value
        return new_book, self.sleep
    