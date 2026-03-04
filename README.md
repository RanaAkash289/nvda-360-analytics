# NVDA-360 Analytics (Power BI + Python)

NVDA-360 Analytics is a three-page Power BI dashboard that analyzes NVIDIA (NVDA) from three angles: market performance, quarterly financials, and news and sentiment signals. The project is designed as a portfolio-ready case study with interactive slicers, clean KPI cards, and clear trend visuals suitable for GitHub and LinkedIn.

## What this project includes

This repository contains:
- A three-page Power BI dashboard
- Python scripts for collecting and preparing external news and sentiment data
- Output datasets used by Power BI
- Screenshots for quick preview and documentation

## Dashboard pages

### Page 1: Market Overview
This page focuses on market price behavior and technical signals.
- Close vs MA(30) and MA(200) trend comparison
- 30-day rolling volatility to highlight risk and regime changes
- KPI cards such as Latest Close, YTD Return, and Latest Volatility (30d)
- Interactive date range slicer

### Page 2: Financial Performance
This page summarizes quarterly fundamentals and profitability trends.
- KPI cards such as Latest Quarterly Revenue, Latest Quarterly Net Income, and Latest Gross Margin
- Quarterly trend visuals for Revenue and Net Income
- Gross Margin percentage trend
- R&D to Revenue percentage trend
- Quarter range slicer for period-based exploration

### Page 3: NVDA News and Market Sentiment (GDELT)
This page links news intensity and sentiment to market reaction.
- KPI cards for News Volume (30D), Avg Tone (30D), and Avg Daily Return (30D)
- News volume versus NVDA Close to compare attention spikes with price movement
- Tone versus NVDA Close using dual-axis scaling for readability
- Tone versus Return (30D) to observe sentiment and return alignment over time
- Date-only slicer to avoid timestamp matching issues across datasets

## Data sources

This project combines three datasets:
- Market data for NVDA including daily prices and derived features such as returns, moving averages, and volatility
- Quarterly financial metrics including revenue, net income, gross margin, and R&D intensity
- News and sentiment data sourced from the GDELT 2.1 API, including volume, tone, and headline metadata

The external GDELT outputs used by Power BI are:
- data/external/gdelt_nvda_daily.csv
- data/external/gdelt_nvda_headlines.csv

Note: GDELT endpoints may occasionally rate-limit requests. The script uses retry and backoff logic to reduce failures.

## Project structure

nvda-360-analytics
data
raw
processed
external
gdelt_nvda_daily.csv
gdelt_nvda_headlines.csv
src
fetch_gdelt_nvda_daily_v2.py
notebooks
reports
NVDA-360-Analytics.pbix
assets
screenshots
page1.png
page2.png
page3.png
README.md

## How to reproduce

### Step 1: Install dependencies
Install Python dependencies from your requirements file:
- Python 3.10 or later is recommended
- requests and pandas are required

### Step 2: Fetch news and sentiment data from GDELT
Run the script that pulls GDELT timeline and headline data:
python src/fetch_gdelt_nvda_daily_v2.py

This will generate:
- data/external/gdelt_nvda_daily.csv
- data/external/gdelt_nvda_headlines.csv

### Step 3: Open and refresh the Power BI report
Open the Power BI file:
- reports/NVDA-360-Analytics.pbix

Then refresh the data inside Power BI so the visuals load the latest CSV outputs.

## Key metrics and logic

The dashboard uses a combination of calculated columns and measures, including:
- News Volume (30D): rolling 30-day sum of article counts
- Avg Tone (30D): rolling 30-day tone average, optionally weighted by volume
- Avg Daily Return (30D): rolling 30-day average of daily returns
- YTD Return: start-of-year price versus latest price
- Volatility (30D): rolling volatility measure based on price returns
- Date-only model: date-only fields are used for reliable filtering across tables

## What you can learn from this dashboard

This dashboard helps answer questions such as:
- Do spikes in news volume align with higher volatility or notable return periods?
- Does positive or negative tone move alongside price trends or reversals?
- How do quarterly fundamentals evolve during major market transitions?
- How does R&D intensity change over time relative to financial performance?

## Tools used
- Power BI Desktop for dashboard creation and DAX measures
- Python for data collection and ETL
- GDELT 2.1 API for news volume, tone, and headlines

## Author
Akash Rana
Portfolio project: NVDA-360 Analytics
