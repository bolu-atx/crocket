# Crocket (Crypto rocket)

## What is crocket?

Crocket collects real-time cryptocurrency data from Bittrex and stores it in an SQL database. Crocket runs an algorithm on the collected data to determine when a buy and sell order should be executed. Buy and sell orders are executed and handled by crocket.

## Crocket components

 * Scraper
    
    * Scraper collects data from Bittrex through the API and stores the collected data in an SQL database. All data is sent to the tradebot.
    
 * Tradebot
 
    * Tradebot runs an algorithm from the collected data received from scraper. It determines when a buy and sell order should be executed. Buy and sell orders are sent to the manager. 
    
 * Manager
 
    * Manager executes all buy and sell orders received from tradebot.

## How to run

1. Open a new screen.

```bash
screen
```

2. Start the server in the screen.

 * To detach screen: CTRL+A -> d
 
```bash
cd crocket/crocket
python run_server.py
```

3. Open another terminal session.

4. Set the total wallet amount.

```bash
curl localhost:9999/tradebot/set/wallet/<insert_wallet_amount>
```

5. Set the target amount per buy order.

```bash
curl localhost:9999/tradebot/set/call/<insert_buy_amount>
```

6. Start scraper.

```bash
curl localhost:9999/scraper/start/<database_name>
```

7. Start tradebot + manager.

 * Use same database_name in step 6.
 
```bash
curl localhost:9999/tradebot/start/<database_name>
```

## How to stop

1. Stop tradebot + manager.

```bash
curl localhost:9999/tradebot/stop
```

2. Stop scraper.

```bash
curl localhost:9999/scraper/stop
```

3. Shutdown server.

```bash
curl -X POST localhost:9999/shutdown
```
