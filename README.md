# Stocks-analysis-and-alerts
### Excelling graduation project in Data Engineering course at Naya College
#### OVERVIEW
The stock market is a complex and dynamic environment where prices can change rapidly and unpredictably. 
Investors, traders and analysts need to keep track of these changes in real-time to make informed decisions and take advantage of market opportunities.
However, it can be challenging to monitor stock data manually, especially for those who are new to the market.
To address this need, we have developed a program that provides alerts and analysis on stock data.
The program uses historical and real-time stock data from reliable sources to provide insights into market trends, volatility, and performance.

#### BUSINESS GOALS
* Create a tool that simplifies the process of monitoring stock data and provides valuable insights to investors, traders, and analysts.
* Provide users with real-time alerts and analysis on stock data, enabling them to make informed decisions and take advantage of market opportunities.
* Help users understand market trends, volatility, and performance by generating charts and graphs that show trends and patterns in stock prices, volume, and other key indicators.
* Provide a customizable and user-friendly program that meets the specific needs of its users.

#### TECH STACK 
* Kafka
* Spark Structured Streaming 
* MySQL
* Elasticsearch
* MongoDB
* Amazon S3
* Kibana
* Grafana
* Airflow

#### The project consists of five processes that perform different tasks:
1. Real-time data pipeline: This pipeline fetches stocks prices in real-time and stores them in MongoDB. It also sends alerts to registered users and archives the data to S3 for future analysis
2. Daily batch data pipeline: This pipeline runs once at night and aggregates daily data. The aggregated data is stored in MySQL database and can be analyzed using Grafana
3. Articles pipeline: This pipeline runs once a day and fetches relevant articles about stocks. The articles are stored in MongoDB and sent to registered users via email
4. Logs: A general process that writes logs into Elasticsearch and presents them on Kibana for analysis
5. Flask web app: Simple and friendly user interface that displays real-time stock prices and percentage change from the last trading day’s closing price. Additionally, allows users to register for stocks prices alerts and receive articles about specific stocks

In Addition, we used Airflow to schedule the three first main processes of the project.