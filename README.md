# Steam-Bandwidth-Forecasting

Data Sources:
- [Steam Download Stats](https://store.steampowered.com/stats/content/)
- [Steam Support Stats](https://store.steampowered.com/stats/support/)

Some preliminary ideas:
- Support requests look random af, so will probably just focus on bandwidth usage
- Worth trying out a global forecasting model and then using hierarchical reconciliation to get good aggregate forecasts
- Could also use a metric like MAGE (Mean Absolute aGgregate Error) and train individual region-level models to minimize this metric
- Focus on 48-hour forecast horizons, don't have a whole lot of data yet
- AutoGluonTS seems to be pretty decent AutoML for time series, will be a good baseline to try to beat with my own approach
