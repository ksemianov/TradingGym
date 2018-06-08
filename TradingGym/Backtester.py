from TradingGym.OrderFlow import OrderFlow
from TradingGym.OrderBook import OrderBook
from pandas import Timedelta
import numpy as np
import math
import time
from tqdm import tqdm
import sys


class Backtester:
    """
    Implements strategy tester in terms of PnL
    """
    def __init__(self, flow, strategy):
        self.flow = OrderFlow()
        self.flow.df = flow.df.copy(deep=True)
        self.strategy = strategy
        
        self.ts = []
        self.position = []
        self.r_pnl = []
        self.ur_pnl = []
        self.price = []
        self.commission = 0.0002
        self.max_length = 10**6
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
        # assuming full discharge of position now
        position = self.position[-1]
        ret = 0.0
        if position > 0:
            for price, value in sorted(book.book[0].items(), reverse=True):
                if position <= 0.0:
                    break
                position -= value
                ret += price * max(value, position)
        else:
            for price, value in sorted(book.book[1].items(), reverse=False):
                if position >= 0.0:
                    break
                position += value
                ret -= price * max(value, -position)
        return ret

    def finalize_book(self, book, new_book):
        for price, value in sorted(book.book[1].items(), reverse=False):
            bestBid = new_book.bestBid()
            if bestBid[0] < price or math.isnan(bestBid[0]):
                break
            if bestBid[1] > value:
                self.position[-1] += value
                new_book.book[0][bestBid[0]] -= value
            else:
                self.position[-1] += bestBid[1]
                del new_book.book[0][bestBid[0]]
        for price, value in sorted(book.book[0].items(), reverse=True):
            bestAsk = new_book.bestAsk()
            if bestAsk[0] > price or math.isnan(bestAsk[0]):
                break
            if bestAsk[1] > value:
                self.position[-1] -= value
                new_book.book[1][bestAsk[0]] -= value
            else:
                self.position[-1] -= bestAsk[1]
                del new_book.book[1][bestAsk[0]]

        return new_book
        
    def run(self, max_length = 10**6):
        self.max_length = max_length
        
        deals = self.flow.df[self.flow.df.DealId != 0].drop_duplicates('ExchTime')
        book = OrderBook()
        used_idx = 0
        
        right_before_trading = self.flow.df[self.flow.df.Flags.str.contains('Snapshot')].iloc[-1]
        trading_start = self.flow.df[self.flow.df.Flags.str.contains('Add') & (self.flow.df.index > right_before_trading.name)].iloc[0]
        trading_close_time = trading_start.ExchTime.round('h') + Timedelta('8h45m')
        trading_close_idx = self.flow.df.ExchTime.searchsorted(trading_close_time)[0] - 1
        trading_close = self.flow.df.iloc[trading_close_idx]
        trading_end = self.flow.df.iloc[
            min(self.max_length + trading_start.name - 1, trading_close.name)]
        total_time = trading_end.ExchTime.value - trading_start.ExchTime.value
        total_idx = trading_end.name - trading_start.name
        print('Started simulation from time: {}'.format(trading_start.ExchTime))
        print('Planned end time: {}'.format(trading_end.ExchTime))
        sys.stdout.flush()
        pbar = tqdm(total=total_idx, smoothing=0.01)
        progress = 0
        
        self.ts.append(trading_start.ExchTime)
        self.position.append(0.0)
        self.r_pnl.append(0.0)
        self.ur_pnl.append(0.0)
        
        idx = trading_start.name
        messages = self.flow.df.iloc[used_idx:idx]
        used_idx = idx
        book.updateBulk(messages)
        self.price.append((max(book.book[0].keys()) + min(book.book[1].keys())) / 2)
        strategy_time = trading_start.ExchTime
        new_book, sleep = self.strategy.action(self.position[-1], 
                self.flow.df.iloc[:idx], self.trader_book, book)
        self.r_pnl[-1] -= self.commissions(self.trader_book, new_book)
        new_book = self.finalize_book(book, new_book)
        self.trader_book = new_book

        for name, deal in deals.iterrows():
            if name > trading_end.name:
                break
            
            idx = used_idx
            while (deal.ExchTime - strategy_time).value > sleep:
                strategy_time += Timedelta(np.timedelta64(sleep, 'ms'))
                
                self.ts.append(strategy_time)
                self.position.append(self.position[-1])
                self.r_pnl.append(self.r_pnl[-1])
                self.price.append((max(book.book[0].keys()) + min(book.book[1].keys())) / 2)

                while (self.flow.df.iloc[idx].ExchTime < strategy_time or 'EndOfTransaction' not in self.flow.df.iloc[idx].Flags) and idx < name-1:
                    message = self.flow.df.iloc[idx]
                    book.update(message)
                    idx += 1
                used_idx = idx
                new_book, sleep = self.strategy.action(self.position[-1], 
                    self.flow.df.iloc[:idx], self.trader_book, book)
                self.r_pnl[-1] -= self.commissions(self.trader_book, new_book)
                new_book = self.finalize_book(book, new_book)
                self.ur_pnl.append(self.unrealizedPnl(book))

            assert(used_idx <= name-1)
            messages = self.flow.df.iloc[used_idx:name-1]
            used_idx = name-1
            book.updateBulk(messages)
            
            buySell = 'Buy' in self.flow.df.iloc[name - 1].Flags
            deal_amount = self.flow.df.iloc[name - 1].Amount
            deal_price = self.flow.df.iloc[name - 1].Price

            if buySell:
                """Buy trader's asks"""
                for price, amount in sorted(book.book[1].items(), reverse=False):
                    bestAsk = new_book.bestAsk()
                    if (bestAsk[0] > deal_price or bestAsk[0] == float('nan')) and price > deal_price:
                        break
                    if bestAsk[0] != float('nan') and (bestAsk[0] < price or (bestAsk[0] == price and self.strongPriority)):
                        if bestAsk[1] >= deal_amount:
                            new_book.book[1][bestAsk[0]] -= deal_amount
                            
                            self.position[-1] -= deal_amount
                            self.r_pnl[-1] += deal_amount * bestAsk[0]
                            
                            deal_amount = 0
                        else:
                            del new_book.book[1][bestAsk[0]]
                            
                            self.position[-1] -= bestAsk[1]
                            self.r_pnl[-1] += bestAsk[1] * bestAsk[0]
                            
                            deal_amount -= bestAsk[1]
                    else:
                        deal_amount -= amount
                    if deal_amount <= 0:
                        break
                if deal_amount > 0:
                    pass # assuming that order was FillOrKill
            else:
                """Sell trader's bids"""
                for price, amount in sorted(book.book[0].items(), reverse=True):
                    bestBid = new_book.bestBid()
                    if (bestBid[0] < deal_price or bestBid[0] == float('nan')) and price < deal_price:
                        break
                    if bestBid[0] != float('nan') and (bestBid[0] > price or (bestBid[0] == price and self.strongPriority)):
                        if bestBid[1] >= deal_amount:
                            new_book.book[0][bestBid[0]] -= deal_amount
                            
                            self.position[-1] += deal_amount
                            self.r_pnl[-1] -= deal_amount * bestBid[0]
                            
                            deal_amount = 0
                        else:
                            del new_book.book[0][bestBid[0]]
                            
                            self.position[-1] += bestBid[1]
                            self.r_pnl[-1] -= bestBid[1] * bestBid[0]
                            
                            deal_amount -= bestBid[1]
                    else:
                        deal_amount -= amount
                    if deal_amount <= 0:
                        break
                if deal_amount > 0:
                    pass # assuming that order was FillOrKill

            self.trader_book = new_book
            self.ur_pnl[-1] = self.unrealizedPnl(book)
            
            pbar.update(used_idx - progress)
            progress = used_idx
                
        pbar.close()
        
        return [self.ts, self.position, self.r_pnl, self.ur_pnl, self.price]
    