from setuptools import setup

setup(
    name='TradingGym',
    version='0.1.0',
    description='Order book data ML playground',
    url='https://github.com/ksemianov/TradingGym',
    author='Konstantin Semianov',
    author_email='semyanovk@gmail.com',
    license='Apache 2.0',
    packages=['TradingGym', 'TradingGym.envs'],
    zip_safe=False,
    install_requires=['gym', 'seaborn', 'matplotlib', 'tqdm', 'pandas',
     'tables', 'numpy', 'tensorflow', 'keras', 'keras-rl', 'notebook']
)
