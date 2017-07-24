package com.dennisgove.streaming.expressions.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import com.dennisgove.streaming.expressions.kafka.KafkaTopicConsumerStream;
import com.google.gson.Gson;

public class KafkaTopicConsumerStreamTest {

  // http://www.java-allandsundry.com/2016/11/using-kafka-with-junit.html

  @ClassRule
  public static KafkaEmbedded kafka = new KafkaEmbedded(1, true, 1, "stream-test");
  public KafkaProducer<String,String> producer;
  private Gson gson = new Gson();

  @Before
  public void setupProducer() {

    Properties producerProps = new Properties();
    producerProps.setProperty("bootstrap.servers", kafka.getBrokersAsString());
    producerProps.put("key.serializer", StringSerializer.class.getName());
    producerProps.put("value.serializer", StringSerializer.class.getName());
    producer = new KafkaProducer<>(producerProps);
  }

  @Test
  public void testSimpleRead() throws IOException, InterruptedException, ExecutionException {
    AtomicInteger counter = new AtomicInteger(0);

    Runnable producerRunnable = new Runnable() {
      private Tuple createTuple(String id, String fieldA) {
        Tuple tuple = new Tuple();
        tuple.put("id", id);
        tuple.put("fieldA", fieldA);

        return tuple;
      }

      @Override
      public void run() {
        ProducerRecord<String,String> record = new ProducerRecord<>("stream-test", "1", gson.toJson(createTuple("1","valueA").fields));
        Future<RecordMetadata> f = producer.send(record);
        try {
          RecordMetadata m = f.get();
          long offset = m.offset();
        }
        catch(InterruptedException | ExecutionException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        counter.getAndIncrement();
      }
    };

    StreamFactory factory = new StreamFactory();
    StreamExpression expression = StreamExpressionParser.parse("kafkaConsumer(topic=stream-test, groupId=testGroup, bootstrapServers=" + kafka.getBrokersAsString() + ")");

    KafkaTopicConsumerStream consumer = new KafkaTopicConsumerStream(expression, factory);
    consumer.open();

    producerRunnable.run();

    while(counter.get() < 0) {
      int a = 9;
    }

    Tuple readTuple = consumer.read();

    Assert.assertEquals(new Long(1), readTuple.getLong("id"));
    Assert.assertEquals("valueA", readTuple.getString("fieldA"));
  }
}
