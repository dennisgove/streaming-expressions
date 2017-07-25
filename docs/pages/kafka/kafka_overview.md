---
title: Kafka Expressions
keywords: kafka
tags: [kafka]
sidebar: site_sidebar
permalink: kafka_overview.html
module: streaming_expressions_kafka
kafka_clients_version: 0.10.2.0
---

## Overview

Available in this library are Streaming Expressions which interact with Kafka. They provide the ability for a Solr pipeline to act as both Kafka consumers and publishers.

## Dependencies

To make use of this library the following `.jar` files will need to be added to your Solr classpath.

### Kafka Clients

Because the Apache Kafka Clients library is used for all communication with Kafka brokers it will need to be included in your Solr classpath. Main releases can be [found here](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients) and javadocs can be [found here](https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients).

* __Minimum Version__: {{ page.kafka_clients_version }}
  * [Maven Dependency](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/{{ page.kafka_clients_version }})
  * [Download Jar](http://central.maven.org/maven2/org/apache/kafka/kafka-clients/{{ page.kafka_clients_version }})

{% include links.html %}
