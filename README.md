# Realtime Subtitle Rating Classification

This Repo contains the implementation of a subtitle rating classifier which was done as a project for the Data Intensive Computing course at KTH. The report of the project can be seen in `Real_time_Subtitle_Rating.pdf`.

## Implementation

For easyness of deployment and reproducibility, docker was selected as the main tool to orchestrate this project (details on how to run it below). We were able to get everything running with 5 containers: zookeeper, kafka, cassandra and two for running the producer and streaming algorithms (via a spark-scala container with sbt built in).

Within the streaming program, our main task, most of the implementation parts were straight forward (having to keep into account that the external services were not running in `localhost`). Regarding the stateful mapping function, we decided the state could be a tuple consisting of the current average and the total count of messages seen. We decided to keep the current average instead of the total sum (even if it is slightly harder to compute) to avoid overflow errors (if the streaming application is running for a really long time or the numbers seen are too high).


## Requisites

- docker
- docker-compose

## Running

You'll need to have four terminals open:

- In one run `docker-compose up` to start all needed services
- In another, run the producer once (`./run-producer.sh`) beacause it will create the kafka topic.
- In the next one run `./run-streaming.sh` to start the streaming application which will wait for messages to come from kafka.
- After the streaming is ready you can run the producer again `./run-producer.sh`, which will input 10000 random messages into kafka.
- While this process is ongoing you can connect to cassandra via `./cqlsh.sh` and query the database using `use avg_space; select * from avg;`.

## Results

The spark-streaming app doesn't provide output while it is running. The producer outputs lines like these ones while generating messages:

```
ProducerRecord(topic=avg, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=null, value=c,21, timestamp=null)
ProducerRecord(topic=avg, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=null, value=v,14, timestamp=null)
```

At the same time, if we query the database we will see outputs like the following, with the numbers changing over time.

```
 word | count
------+----------
    z | 11.74937
    a | 12.61789
    c | 12.62069
    m | 12.51654
    f | 12.07756
    o | 11.86189
    n | 12.50914
    q | 12.17259
    g | 12.39036
    p | 12.07783
    e | 12.08264
    r | 12.93862
    d | 11.74728
    h | 12.15235
    w | 13.18883
    l | 12.49406
    j |   12.735
    v | 11.89948
    y | 12.28288
    u | 12.52809
    i | 12.71067
    k | 12.74444
    t | 11.92428
    x | 12.22973
    b | 11.70297
    s | 12.62368
```

