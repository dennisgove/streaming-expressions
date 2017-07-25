---
title: Kafka Topic Consumer
keywords: kafka
tags: [kafka]
sidebar: site_sidebar
permalink: kafka_topic_consumer.html
function_name: kafkaConsume
---

## Overview

The Kafka Topic Consumer function is a source stream which reads records off of a Kafka topic and turns each into a valid tuple object. Every call to `read()` will return a single tuple.

## Expression

The `{{ page.function_name }}` function allows you to provide a topic name, a client id, and a connection string to the Kafka broker from which records should be read. You can also provide any arbitrary parameters which will be passed, as is, to the Kafka consumer client.

```
{{ page.function_name }}(
  topic = topicName,
  groupId = clientGroupId,
  bootstrapServers = kafkaBootstrapServers
)

{{ page.function_name }}(
  topic = topicName,
  groupId = clientGroupId,
  bootstrapServers = kafkaBootstrapServers,
  enable.auto.commit = true
)
```

### Parameters

* __topic__: (required) This is the name of the Kafka topic
* __groupId__: (required) Any valid consumer group id
* __bootstrapServers__: (required) Connection string to the Kafka brokers

You can also provide any other name/value pair which will be passed directly to the Kafka Consumer client object. A list of available conection config parameters can be [found here](https://kafka.apache.org/0101/documentation.html#connectconfigs).

## Registering with Solr

As with any Solr Streaming function, it can be registered by adding the following line to the standard `solrconfig.xml` file.

```xml
<expressible name="kafkaConsumer" 
             class="com.dennisgove.streaming.expressions.kafka.KafkaTopicConsumerStream"/>
```

{% include links.html %}
