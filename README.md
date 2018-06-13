# TradingGym
TradingGym is a platform for automated optimal trading. It implements [OpenAI Gym](https://github.com/openai/gym) environment to train and test reinforcement learning agents. The environment is created from level II stock exchange data and takes into account commissions, bid-ask spreads and slippage (but still assumes no market impact). 

## Installation
```bash
git clone https://github.com/ksemianov/TradingGym
cd TradingGym
python3 -m pip install -e .
```

## Usage
The platform supports level II market data in Plaza II format from MOEX. It expects hdf5 file where every key has data for a separate trading session. The example value for a key should look similar to this:
![](/images/dataset.png)

See [RL](notebooks/RL.ipynb) notebook for examples of training and testing agents based on [keras-rl](https://github.com/keras-rl/keras-rl). 
