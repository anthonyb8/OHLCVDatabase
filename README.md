***OHLCV Database***

**Project Description:**
  
This project makes use of QuestDB, an open-source time series database, as well as the Interactive Brokers Native API. This project was created with the intention of researching trading strategies. While the Interactive Brokers API is robust in its functionality, it is somewhat limited in the amount of data that can be pulled in a single request, which is limited to one year. This proved to be limiting when trying to research and develop strategies.

The goal of this project was to create a database of 5-minute bar data for a specific stock universe. Data can be aggregated to obtain a longer timeframe using 5-minute bar data. The database is initially built by pulling historical data year by year up to the present. I chose to start 10 years in the past, but Interactive Brokers has older data if desired. Once the database is current, it can easily be updated on a regular basis, such as daily or hourly.

I began developing this project because of my passion and curiosity for quantitative trading. I'm always looking for ways to improve the project, so if anyone is interested in contributing, or if you have any questions or comments, please let me know.
