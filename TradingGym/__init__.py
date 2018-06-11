from gym.envs.registration import register

register(
    id='trading-v0',
    entry_point='TradingGym.envs:TradingEnv',
)
