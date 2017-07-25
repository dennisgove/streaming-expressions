---
title: Solr Streaming Expressions
keywords: homepage started
tags: [getting_started]
sidebar: site_sidebar
permalink: index.html
toc: false
---

## About

This documentation will introduce you to Streaming Expression functions which are not included with core Solr releases. These functions exist independently of Solr because they contain dependencies on libraries not part of a standard Solr build. For this reason we are building them independently so that only people who want to use them need to worry about these extra dependencies.

## Streaming Expressions - Overview

Streaming Expressions provide a simple yet powerful stream processing language for Solr Cloud. They are a suite of functions that can be combined to perform many different parallel computing tasks. Full documentation for the growing list of default functions can be found [here](https://lucene.apache.org/solr/guide/streaming-expressions.html). These can be combined to implement:

* Request/response stream processing
* Batch stream processing
* Fast interactive MapReduce
* Aggregations (Both pushed down faceted and shuffling MapReduce)
* Parallel relational algebra (distributed joins, intersections, unions, complements)
* Publish/subscribe messaging
* Distributed graph traversal
* Machine learning and parallel iterative model training
* Anomaly detection
* Recommendation systems
* Retrieve and rank services
* Text classification and feature extraction
* Streaming NLP

{% include links.html %}
