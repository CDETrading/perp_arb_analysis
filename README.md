# perp_arb_analysis

A Python-based system for collecting and analyzing real-time order book data from cryptocurrency exchanges to identify arbitrage opportunities.

## Overview

This project provides a framework for collecting and analyzing cryptocurrency market data:

- Real-time order book data collection via WebSocket connections
- Standardized data format across exchanges
- High-precision timestamping for accurate spread analysis
- Extensible design for adding new exchanges and trading pairs

## Current Implementation

The system currently collects data from:
- Binance Futures (BTCUSDT)
- OKX Perpetual Swaps (BTC-USDT-SWAP)

Data points collected for each order book update:
- Local receive timestamp (nanoseconds)
- Exchange timestamp (milliseconds) 
- Best bid/ask prices and quantities
- Exchange name
- Trading pair (base/quote)

## Planned Extensions

The framework is designed to be expanded with:
1. Additional exchanges and trading pairs
2. Real-time spread analysis between exchanges
3. Automated arbitrage opportunity detection
4. Historical data analysis capabilities
5. Market microstructure analysis tools

## Data Storage

All collected data is logged to daily CSV files for further analysis and backtesting. The standardized format allows for easy comparison across exchanges and time periods.