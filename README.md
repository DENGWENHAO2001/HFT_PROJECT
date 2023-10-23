## step1: Open celery workers
`python run_celery.py`

## step2: Start a producer application that fetches data streams from binance and writes them to RabbitMQ in real time
`python websocket_producer.py`

## step3: Turn on the consumer and get the data stream from RabbitMQ in real time, then 
sub1: pre-process, sub2: input to the agent decision and sub3: interact with binance

`python websocket_consumer.py`

NOTE: 
1. step3 three parts are still incomplete, I am in charge of sub1 and sub2, lingxuan is in charge of the sub3
2. binance official api: https://binance-docs.github.io/apidocs/spot/en/#api-library
3. unofficial Python wrapper for the Binance exchange REST API v3: https://github.com/sammchardy/python-binance
4. python3.9

    `conda install pytorch==1.12.1 torchvision==0.13.1 torchaudio==0.12.1 cpuonly -c pytorch`

    `pip install -r requirements.txt`
