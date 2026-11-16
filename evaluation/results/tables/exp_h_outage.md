| run | requests | status mix | allowed | events on Kafka for the user |
|---|---|---|---|---|
| pause-kafka-1 | 388 | {"200": 388} | 388 | 388 |
| pause-kafka-2 | 387 | {"200": 387} | 387 | 387 |
| pause-kafka-3 | 388 | {"200": 388} | 388 | 388 |
| pause-redis-1 | 292 | {"200": 289, "error ReadTimeout": 3} | 289 | 289 |
| pause-redis-2 | 292 | {"200": 289, "error ReadTimeout": 3} | 289 | 289 |
| pause-redis-3 | 292 | {"200": 289, "error ReadTimeout": 3} | 289 | 289 |

docker pause of the named service for 20 s during an 80 s probe (one request every 0.2 s).
