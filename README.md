ActiveMQ River Plugin for ElasticSearch
==================================

Creating the ActiveMQ river is as simple as:

```bash
  curl -XPUT 'localhost:9200/_river/dsm/_meta' -d '{
      "type" : "activemq",
      "activemq" : {
          "host" : "127.0.0.1",
          "port" : 6163,
          "user" : "guest",
          "pass" : "guest",
          "queue" : "/queue/nature/dsm"
      }
  }'
```
