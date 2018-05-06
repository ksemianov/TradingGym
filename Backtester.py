from OrderFlow import OrderFlow
from OrderBook import OrderBook

class Backtester:
    """
    Implements strategy tester in terms of PnL
    """
    def __init__(self, flow, strategy):
        self.flow = OrderFlow()
        self.flow.df = flow.df.copy(deep=True)
        self.strategy = strategy
        
        self.position = []
        self.r_pnl = []
        self.ur_pnl = []
        self.commission = 0.0002
        self.max_length = 100000
        self.trader_book = OrderBook()
        self.strongPriority = False # trader's orders are matched first if True
        
    def commissions(self, book1, book2):
        acc = 0.0
        book_tmp = OrderBook()
        book_tmp.book = (book1.book[0].copy(), book1.book[1].copy())
        for i in range(2):
            for price, amount in book2.book[i].items():
                if price in book_tmp.book[i]:
                    book_tmp.book[i][price] -= amount
                else:
                    book_tmp.book[i][price] = -amount
        for i in range(2):
            for price, amount in book_tmp.book[i].items():
                acc += abs(amount) * self.commission
        return acc
    
    def unrealizedPnl(self, book):
        # assuming discharge of position at current best bid-ask
        if self.position[-1] > 0:
            return self.position[-1] * book.bestBid()[0]
        else:
            return self.position[-1] * book.bestAsk()[0]
        
    def run(self):
        deals = self.flow.df[self.flow.df.DealId != 0].drop_duplicates('ExchTime')
        book = OrderBook()
        used_idx = 0
        for name, deal in deals.iterrows():
            if name > self.max_length:
                break
                
            self.position.append(self.position[-1] if self.position else 0.0)
            self.r_pnl.append(self.r_pnl[-1] if self.r_pnl else 0.0)
            
            idx = name
            messages = self.flow.df.iloc[used_idx:idx]
            used_idx = idx
            book.updateBulk(messages)
            
            new_book = self.strategy.action(self.position, 
                self.flow.df.iloc[:idx], self.trader_book, book)
            self.r_pnl[-1] -= self.commissions(self.trader_book, new_book)
            
            buySell = 'Buy' in self.flow.df.iloc[idx - 1].Flags
            total_amount = self.flow.df.iloc[idx - 1].Amount
            price = self.flow.df.iloc[idx - 1].Price
            if buySell:
                """Buy trader's asks"""
                for price, amount in book.book[1].items():
                    bestAsk = new_book.bestAsk()
                    if bestAsk[0] != float('nan') and (bestAsk[0] < price or (bestAsk[0] == price and self.strongPriority)):
                        if bestAsk[0] >= total_amount:
                            new_book.book[1][bestAsk[0]] -= total_amount
                            
                            self.position[-1] -= total_amount
                            self.r_pnl[-1] += total_amount * bestAsk[0]
                            
                            total_amount = 0
                        else:
                            del new_book.book[1][bestAsk[0]]
                            
                            self.position[-1] -= bestAsk[1]
                            self.r_pnl[-1] += bestAsk[1] * bestAsk[0]
                            
                            total_amount -= bestAsk[1]
                    else:
                        total_amount -= amount
                    if total_amount <= 0:
                        break
                if total_amount > 0:
                    pass # assuming that order was FillOrKill
            else:
                """Sell trader's bids"""
                for price, amount in book.book[0].items():
                    bestBid = new_book.bestBid()
                    if bestBid[0] != float('nan') and (bestBid[0] > price or (bestBid[0] == price and self.strongPriority)):
                        if bestBid[1] >= total_amount:
                            new_book.book[0][bestBid[0]] -= total_amount
                            
                            self.position[-1] += total_amount
                            self.r_pnl[-1] -= total_amount * bestBid[0]
                            
                            total_amount = 0
                        else:
                            del new_book.book[0][bestBid[0]]
                            
                            self.position[-1] += bestBid[1]
                            self.r_pnl[-1] -= bestBid[1] * bestBid[0]
                            
                            total_amount -= bestBid[1]
                    else:
                        total_amount -= amount
                    if total_amount <= 0:
                        break
                if total_amount > 0:
                    pass # assuming that order was FillOrKill
            
            self.trader_book = new_book
            self.ur_pnl.append(self.unrealizedPnl(book))
            
        
        return [deal.ExchTime for name, deal in deals.iterrows()
                                             if name <= self.max_length], self.position, self.r_pnl, self.ur_pnl
    