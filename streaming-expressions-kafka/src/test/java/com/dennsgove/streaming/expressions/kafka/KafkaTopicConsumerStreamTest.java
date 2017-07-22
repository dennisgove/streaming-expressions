package com.dennsgove.streaming.expressions.kafka;

import java.io.IOException;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;
import com.dennisgove.streaming.expressions.kafka.KafkaTopicConsumerStream;

public class KafkaTopicConsumerStreamTest {

  @Test
  public void test() throws IOException{

    StreamFactory factory = new StreamFactory();
    StreamExpression expression = StreamExpressionParser.parse("kafkaConsumer(topic=stream-test, bootstrapServers=localhost:9092, groupId=foo)");

    KafkaTopicConsumerStream stream = new KafkaTopicConsumerStream(expression, factory);
    stream.open();

    Tuple first = stream.read();
  }
}
