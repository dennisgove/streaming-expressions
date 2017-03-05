## Streaming Expressions - Extensions

This project houses various Solr Streaming Expression implementations which are too specific or specialized to be included in Solr core. The purpose is to allow for the creation and use of specialized expression logic without causing an increase in Solr's dependency set.

The project is broken up into sub-modules such that people can use one without the others. For example, making use of a the Kafka expressions should not necessitate dependency on the SMS expressions. Each sub-module will result in its own .jar which can be included in th Solr class path, or even by adding to the blob store.
