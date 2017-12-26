# Crocket (Crypto rocket)

## How to run

1. Open a new screen.

```bash
screen
```

2. Start the server.

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


