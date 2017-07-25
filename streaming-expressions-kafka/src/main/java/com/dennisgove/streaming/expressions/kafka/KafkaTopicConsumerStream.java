/**
 * Copyright 2017 Dennis Gove
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dennisgove.streaming.expressions.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h1>Kafka Topic Consumer Stream</h1>
 * The <b>KafkaTopicConsumerStream</b> provides a new
 * <a href="https://lucene.apache.org/solr/guide/stream-sources.html">Solr Stream Source</a>
 * to consume records from a Kafka topic.
 * <pre>
 * {@code
 * kafkaConsumer(
 *   topic=topicName,
 *   groupId=clientGroup,
 *   bootstrapServers=kafkaBootstrapServers
 * )
 * }
 * </pre>
 * You can also provide any other valid kafka consumer parameters of the form {@code paramName=value},
 * for example {@code enable.auto.commit=true }. Any extra parameters will be passed directly to the
 * Kafka Consumer.
 * <p>
 * When opened this will read all existing records off the topic and hold onto them until a
 * call to {@link KafkaTopicConsumerStream#read()} which will return the first record. Each call
 * to {@link KafkaTopicConsumerStream#read() read()} will return the next record, and when there aren't any
 * left in the list it will get the next batch from Kafka.
 * <p>
 * {@link KafkaTopicConsumerStream#read() read()} will continue returning records until an EOF record is
 * found or the stream is closed with {@link KafkaTopicConsumerStream#close()}.
 * {@link KafkaTopicConsumerStream#read() read()} will not return until at least one record is read
 * off the topic (or the stream is closed). There is a maximum delay of 1s between closing the stream and
 * {@link KafkaTopicConsumerStream#read() read()} returning.
 * <p>
 * Records are expected to be valid JSON of the form.
 *
 * <pre>
 * {@code
 * {
 *   "fieldA": "valueA",
 *   "fieldB": 1
 * }
 * }
 * </pre>
 *
 * An EOF record is one containing field:value {@code "EOF":"true" }.
 * <p>
 * <b>Note:</b> KafkaTopicConsumerStream is still under active development and is expected to change.
 *
 * @author Dennis Gove
 * @since v0.0.1
 */
public class KafkaTopicConsumerStream extends TupleStream implements Expressible {
  private static final long serialVersionUID = 1L;

  private static Set<String> knownParameters = new HashSet<String>() {
    {
      add("bootstrapServers");
      add("groupId");
      add("topic");
      //      add("partitions");
    }
  };

  private Logger log = LoggerFactory.getLogger(getClass());

  private StreamContext context;
  private KafkaConsumer<String,String> consumerClient;

  private String bootstrapServers;
  private String groupId;
  private String topic;
  //  private List<String> partitions;
  private Map<String,String> otherConsumerParams;

  private AtomicBoolean isOpen = new AtomicBoolean(false);
  private long recordsRead = 0;
  private LinkedList<Tuple> tupleList = new LinkedList<>();

  /**
   * Accepts a valid {@link StreamExpression} adhering to the form above and constructs
   * a new instance of the KafkaTopicConsumerStream.
   *
   * @param expression A valid kafkaConsume {@link StreamExpression}
   * @param factory A valid {@link StreamFactory}
   * @throws IOException If unable to parse or deal with the incoming expression
   */
  public KafkaTopicConsumerStream(StreamExpression expression, StreamFactory factory) throws IOException {
    String bootstrapServers = getStringParameter("bootstrapServers", expression, factory);
    String groupId = getStringParameter("groupId", expression, factory);
    String topic = getStringParameter("topic", expression, factory);
    //    List<String> partitions = getMultiStringParameter("partitions", expression, factory);

    Map<String,String> otherParams = new HashMap<>();
    for(StreamExpressionNamedParameter param : factory.getNamedOperands(expression).stream().filter(item -> !knownParameters.contains(item.getName())).collect(Collectors.toList())) {
      if(param.getParameter() instanceof StreamExpressionValue) {
        otherParams.put(param.getName(), ((StreamExpressionValue)param.getParameter()).getValue());
      }
    }

    if(null == bootstrapServers) {
      throw new IOException(String.format(Locale.ROOT, "Invalid %s expressions '%s' - expecting a list of bootstrapServers but found none", factory.getFunctionName(getClass()), expression));
    }

    if(null == topic) {
      throw new IOException(String.format(Locale.ROOT, "Invalid %s expressions '%s' - expecting a single topic but found none", factory.getFunctionName(getClass()), expression));
    }

    this.bootstrapServers = bootstrapServers;
    this.otherConsumerParams = otherParams;
    this.groupId = groupId;
    this.topic = topic;
    //    this.partitions = partitions;
  }

  /**
   * Open the consumer and make a valid consumer connection to the Kafka topic.
   */
  @Override
  public void open() throws IOException {
    // https://www.confluent.io/blog/tutorial-getting-started-with-the-new-apache-kafka-0-9-consumer-client/

    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    //    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    if(null != groupId){
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }

    for(Entry<String,String> entry : otherConsumerParams.entrySet()){
      properties.put(entry.getKey(), entry.getValue());
    }

    consumerClient = new KafkaConsumer<>(properties);
    consumerClient.subscribe(new ArrayList(){{ add(topic); }});

    // read any records already on the
    loadTupleList(0);

    isOpen.set(true);
  }

  private void loadTupleList(long pollTimeout){
    for(ConsumerRecord<String,String> record : consumerClient.poll(pollTimeout)){
      try{
        // Here we are assuming the json is representative of the fields
        // part of a tuple. This should be changed if and when we want to
        // handle the passing of full tuples via kafka
        tupleList.add(new Tuple((Map)ObjectBuilder.fromJSON(record.value())));
      }
      catch(Throwable e){
        // log that there was an error, but continue going
        log.error(String.format(Locale.ROOT, "Failed to convert kafka record a valid Tuple. Topic '%s', group '%s', and record key '%s' - %s", topic, groupId, record.key(), e.getMessage()), e);
      }
    }
  }

  /**
   * Returns the next record off the topic in Tuple form. Will wait until at least one record is read from
   * from the topic, or the stream is closed.
   */
  @Override
  public Tuple read() throws IOException {

    // Get next set of available records, keep repeating until we get something
    while(tupleList.isEmpty()) {

      // if we're closed then return EOF
      if(!isOpen.get()){
        Tuple eof = new Tuple();
        eof.EOF = true;
        return eof;
      }

      // wait at most 1s
      loadTupleList(1000);
    }

    // At this point we will absolutely have a tuple.
    Tuple tuple = tupleList.pop();
    if(tuple.EOF){
      tuple.put("__kafkaRecordCount__", recordsRead);
    }
    else{
      recordsRead += 1;
    }

    return tuple;
  }

  private String getStringParameter(String paramName, StreamExpression expression, StreamFactory factory) {
    StreamExpressionNamedParameter param = factory.getNamedOperand(expression, paramName);
    if(null != param) {
      if(param.getParameter() instanceof StreamExpressionValue) {
        return ((StreamExpressionValue)param.getParameter()).getValue();
      }
    }

    return null;
  }

  private List<String> getMultiStringParameter(String paramName, StreamExpression expression, StreamFactory factory) {
    List<String> values = new ArrayList<>();

    for(StreamExpressionNamedParameter param : factory.getNamedOperands(expression)){
      if(param.getName().equals(paramName) && param.getParameter() instanceof StreamExpressionValue){
        String value = ((StreamExpressionValue)param.getParameter()).getValue();
        for(String part : value.split(",")){
          part = part.trim();
          if(part.length() > 0){
            values.add(part);
          }
        }
      }
    }

    return values;
  }

  /**
   * Returns a valid expression for this instance.
   */
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(getClass()));

    expression.addParameter(new StreamExpressionNamedParameter("bootstrapServers", bootstrapServers));
    expression.addParameter(new StreamExpressionNamedParameter("topic", topic));
    if(null != groupId){
      expression.addParameter(new StreamExpressionNamedParameter("groupId", groupId));
    }

    return expression;
  }

  /**
   * Returns a valid explanation of this stream instance
   */
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    explanation.setFunctionName(String.format(Locale.ROOT, factory.getFunctionName(getClass())));
    explanation.setImplementingClass(getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_SOURCE);

    // child is a kafka topic so add it at this point
    StreamExplanation child = new StreamExplanation(getStreamNodeId() + "-kafka-topic");
    child.setFunctionName(String.format(Locale.ROOT, "kafka (%s)", topic));
    child.setImplementingClass("Kafka");
    child.setExpressionType(ExpressionType.DATASTORE);
    child.setExpression("Consuming from " + bootstrapServers);

    explanation.addChild(child);

    return explanation;
  }

  /**
   * Closes the stream after committing the last read offset.
   */
  @Override
  public void close() throws IOException {
    isOpen.set(false);

    if(null != consumerClient) {
      consumerClient.commitSync();
      consumerClient.unsubscribe();
      consumerClient.close();
    }
  }

  /**
   * Returns null as there is no appropriate sort for a Kafka topic
   */
  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  @Override
  public void setStreamContext(StreamContext context) {
    this.context = context;
  }

  @Override
  public List<TupleStream> children() {
    return new ArrayList<>();
  }

}
