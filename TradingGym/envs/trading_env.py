# Gym: Imports
import gym
from gym import error, spaces, utils
from gym.utils import seeding

# Backtester: Imports
from TradingGym.OrderFlow import OrderFlow
from TradingGym.OrderBook import OrderBook
import pandas as pd
from pandas import Timedelta
import numpy as np
import math
import time
from tqdm import tqdm
import sys
import os

class TradingEnv(gym.Env, utils.EzPickle):
    metadata = {'render.modes': ['human']}
    DEFAULT_SEED = 123

    # Backtester: Calculate comissions
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
    
    # Backtester: Calculate unrealized PnL
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

    # Backtester: Match orders from traders book with market
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

    # Backtester: Load dataset up to trading start
    def loadData(self):
        self.deals = self.flow.df[self.flow.df.DealId != 0].drop_duplicates('ExchTime')
        self.book = OrderBook()
        self.used_idx = 0

        self.trading_start = self.flow.df[self.flow.df.Flags.str.contains('Add') & ~self.flow.df.Flags.str.contains('Snapshot')].iloc[0]
        trading_close_time = self.trading_start.ExchTime.round('h') + Timedelta('8h45m')
        trading_close_idx = self.flow.df.ExchTime.searchsorted(trading_close_time)[0] - 1
        trading_close = self.flow.df.iloc[trading_close_idx]
        self.trading_end = self.flow.df.iloc[
            min(self.max_length + self.trading_start.name - 1, trading_close.name)]
        self.total_time = self.trading_end.ExchTime.value - self.trading_start.ExchTime.value
        self.total_idx = self.trading_end.name - self.trading_start.name
        
        self.ts.append(self.trading_start.ExchTime)
        self.position.append(0.0)
        self.r_pnl.append(0.0)
        self.ur_pnl.append(0.0)
        
        self.idx = self.trading_start.name
        messages = self.flow.df.iloc[self.used_idx:self.idx]
        self.used_idx = self.idx
        self.book.updateBulk(messages)
        self.strategy_time = self.trading_start.ExchTime

    # Gym
    def convertAction(self, action):
        delta_bid_idx, delta_ask_idx = divmod(action, 8)
        delta_bid = self.DELTA_SEQ[delta_bid_idx]
        delta_ask = self.DELTA_SEQ[delta_ask_idx]
        return (self.VOLUME, delta_bid, delta_ask)

    # Gym-Backtester: Init both Gym and Backtester
    def __init__(self):
        # Gym Init
        self.seed_ = self.DEFAULT_SEED

        # 3 dimensions: volume, delta bid, delta ask
        self.action_space = spaces.Box(
            low=np.array([0.0, -100.0, -100.0]), 
            high=np.array([100.0, 1000.0, 1000.0]),
            dtype=np.float32
        )
        self.ACTION_SPACE = 64 # volume will stay fixed, but delta bid and delta ask is divided into 8 parts
        self.DELTA_SEQ = [-5, 0, 5, 10, 50, 100, 200, 500]
        self.VOLUME = 10
        # 2 dimensions: position, mid price
        self.observation_space = spaces.Box(
            low=np.array([-1000.0, 10000.0]),
            high=np.array([1000.0, 200000.0]),
            dtype=np.float32
        )

        # Backtester Init
        self.ts = []
        self.position = []
        self.r_pnl = []
        self.ur_pnl = []
        self.price = []
        self.commission = 0.0002
        self.max_length = 10**7
        self.trader_book = OrderBook()
        self.strongPriority = False # trader's orders are matched first if True
        self.sleep = 100 # ms per step
        self.EPISODE = 100
        

    def init(self, hdf_path, key):
        self.hdf_path = hdf_path
        self.key = key

        self.flow = OrderFlow()
        self.flow.df = pd.read_hdf(self.hdf_path, key=key)
        self.loadData()


    # Backtester: Handle deal message
    def handleDeal(self, deal):
        buySell = 'Buy' in deal.Flags
        deal_amount = deal.Amount
        deal_price = deal.Price
        deal_time = deal.ExchTime

        self.ts.append(deal_time)
        self.position.append(self.position[-1])
        self.r_pnl.append(self.r_pnl[-1])
        self.ur_pnl.append(self.unrealizedPnl(self.book))
        self.price.append((max(self.book.book[0].keys()) + min(self.book.book[1].keys())) / 2)


        if buySell:
            """Buy trader's asks"""
            for price, amount in sorted(self.book.book[1].items(), reverse=False):
                bestAsk = self.new_book.bestAsk()
                if (bestAsk[0] > deal_price or bestAsk[0] == float('nan')) and price > deal_price:
                    break
                if bestAsk[0] != float('nan') and (bestAsk[0] < price or (bestAsk[0] == price and self.strongPriority)):
                    if bestAsk[1] >= deal_amount:
                        self.new_book.book[1][bestAsk[0]] -= deal_amount
                        
                        self.position[-1] -= deal_amount
                        self.r_pnl[-1] += deal_amount * bestAsk[0]
                        
                        deal_amount = 0
                    else:
                        del self.new_book.book[1][bestAsk[0]]
                        
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
            for price, amount in sorted(self.book.book[0].items(), reverse=True):
                bestBid = self.new_book.bestBid()
                if (bestBid[0] < deal_price or bestBid[0] == float('nan')) and price < deal_price:
                    break
                if bestBid[0] != float('nan') and (bestBid[0] > price or (bestBid[0] == price and self.strongPriority)):
                    if bestBid[1] >= deal_amount:
                        self.new_book.book[0][bestBid[0]] -= deal_amount
                        
                        self.position[-1] += deal_amount
                        self.r_pnl[-1] -= deal_amount * bestBid[0]
                        
                        deal_amount = 0
                    else:
                        del self.new_book.book[0][bestBid[0]]
                        
                        self.position[-1] += bestBid[1]
                        self.r_pnl[-1] -= bestBid[1] * bestBid[0]
                        
                        deal_amount -= bestBid[1]
                else:
                    deal_amount -= amount
                if deal_amount <= 0:
                    break
            if deal_amount > 0:
                pass # assuming that order was FillOrKill

    # Backtester: adjust traders book with action | IMPLEMENT
    def tradersBookFromAction(self, action):
        volume, delta_bid, delta_ask = action

        new_book = OrderBook()
        midPrice = (max(self.book.book[0].keys()) + min(self.book.book[1].keys())) / 2
        new_book.book[0][midPrice - delta_bid] = volume
        new_book.book[1][midPrice + delta_ask] = volume

        return new_book

    # Gym: Set random seed
    def seed(self, seed=None):
        self.seed_ = seed or DEFAULT_SEED

    # Gym: Perform one step
    def step(self, action):
        action = self.convertAction(action)

        self.ts.append(self.strategy_time)
        self.position.append(self.position[-1])
        self.new_book = self.tradersBookFromAction(action)
        self.r_pnl.append(self.r_pnl[-1] - self.commissions(self.trader_book, self.new_book))
        self.new_book = self.finalize_book(self.book, self.new_book)
        self.ur_pnl.append(self.unrealizedPnl(self.book))
        self.price.append((max(self.book.book[0].keys()) + min(self.book.book[1].keys())) / 2)
        self.trader_book = self.new_book        


        self.strategy_time += Timedelta(np.timedelta64(self.sleep, 'ms'))
        while self.flow.df.iloc[self.idx].ExchTime < self.strategy_time or 'EndOfTransaction' not in self.flow.df.iloc[self.idx].Flags:
            message = self.flow.df.iloc[self.idx]
            if self.idx+1 in self.deals.index:
                self.handleDeal(message)

            self.book.update(message)
            self.idx += 1
        self.used_idx = self.idx
        self.steps += 1

        position = self.position[-1]
        mid_price = self.price[-1]
        observation = (position, mid_price)
        reward = self.r_pnl[-1] + self.ur_pnl[-1]
        done = False if self.steps < self.EPISODE else True
        info = {}

        if self.strategy_time + Timedelta(np.timedelta64(self.sleep, 'ms')) >= self.trading_end.ExchTime:
            print("Exhausted key: %s" % self.key)
            self.init(self.hdf_path, self.key)
            done = True

        return observation, reward, done, info

    # Gym: Reset for new episode
    def reset(self):
        self.steps = 0

        self.ts = []
        self.position = []
        self.r_pnl = []
        self.ur_pnl = []
        self.price = []

        self.ts.append(self.strategy_time)
        self.position.append(0.0)
        self.r_pnl.append(0.0)
        self.ur_pnl.append(0.0)
        self.price.append((max(self.book.book[0].keys()) + min(self.book.book[1].keys())) / 2)

        position = self.position[-1]
        mid_price = self.price[-1]
        observation = (position, mid_price)

        return observation

    # Gym: Show PnL graph
    def render(self, mode='human', close=False):
        pass
