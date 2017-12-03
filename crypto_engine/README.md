## Basic idea

Main idea, which can be followed in main.py docstrings is to use os cycle-like
system, which incorporates event-driven system while perserving pandas
vectorized approaches to reach some level of suboptimality.


## TODOs:
**POC**

- implement websocket/or some other type of data exchange
- perform adjustments once we decide on bittrex usage (it does not provide api method 
for fetching ticker for all markets at once, request limit results with market_nr * 1s long ticker collection, which goes against our idea to have 1s-like strategies execution frequency;
there are several approaches to tackle this: replace bittrex with bitfinex or some other exchange which suits main idea better, 
perform analysis per one market in one cycle; rely mostly on candle data and v2 (unstable) api (frequency would fall to 1min);
parallel data collection (which might be expensive)...)
- adopt backtesting and risk analysis frameworks to new cycle architecture
  (idea is to have backtester fully pluggable to every strategy and strategy handler - to have
  strong testing tool for every considered/incorporated strategy,
  and general risk analysis framework with specific metrics per strategies
   which will take place in signal generations)
- implement visualization framework to support backtester and risk framework
- refactoring and optimization
- logging
- improve error handling

~~
- add additional strategies
- add amount to buy/sell in output results
- validity analysis of collected ticker data



