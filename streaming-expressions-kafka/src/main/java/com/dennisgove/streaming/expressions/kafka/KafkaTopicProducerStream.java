/**
 * Copyright 2017 Dennis Gove
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTopicProducerStream extends TupleStream implements Expressible {
  private static final long serialVersionUID = 1L;

  private Logger log = LoggerFactory.getLogger(getClass());

  private StreamContext context;
  private KafkaProducer<?,?> producerClient;

  private TupleStream incomingStream;
  private String bootstrapServers;
  private StreamEvaluator topicEvaluator;
  private StreamEvaluator keyEvaluator;
  private String keyType;
  private StreamEvaluator valueEvaluator;
  private String valueType;
  private StreamEvaluator partitionEvaluator;
  private Map<String,String> otherProducerParams;

  public KafkaTopicProducerStream(StreamExpression expression, StreamFactory factory) throws IOException {
    /*
     * kafkaProducer(
     * <incoming stream>,
     * topic=<evaluator>,
     * bootstrapServers=<kafka servers>,
     * key=<evaluator>, // optional, if not provided then no key
     * keyType=[string,int,long,double,boolean], // only required if using key
     * and can't figure out from key param
     * value=<evaluator> // optional, if not provided then whole tuple as json
     * string is used
     * valueType=[string,int,long,double,boolean], // only required if can't
     * figure out from value param
     * partition=<evaluator>, // optional, if not provided then no partition
     * used when sending record
     * )
     * Anything that is <evaluator> is allowed to be any valid StreamEvaluator,
     * such as
     * raw("foo") // raw value "foo"
     * "foo" // value of field foo
     * add(foo, bar) // value of foo + value of bar
     */

    List<StreamExpression> streamParams = factory.getExpressionOperandsRepresentingTypes(expression, TupleStream.class, Expressible.class);

    String bootstrapServers = getStringParameter("bootstrapServers", expression, factory);
    StreamEvaluator topicEvaluator = getEvaluatorParameter("topic", expression, factory);

    StreamEvaluator keyEvaluator = getEvaluatorParameter("key", expression, factory);
    String keyType = getStringParameter("keyType", expression, factory);

    StreamEvaluator valueEvaluator = getEvaluatorParameter("value", expression, factory);
    String valueType = getStringParameter("valueType", expression, factory);

    StreamEvaluator partitionEvaluator = getEvaluatorParameter("partition", expression, factory);

    Map<String,String> otherParams = new HashMap<>();
    Set<String> ignoreParams = new HashSet<String>() {
      {
        add("bootstrapServers");
        add("keyType");
        add("valueType");
        add("topic");
        add("key");
        add("value");
        add("partition");
      }
    };
    for(StreamExpressionNamedParameter param : factory.getNamedOperands(expression).stream().filter(item -> !ignoreParams.contains(item.getName())).collect(Collectors.toList())) {
      if(param.getParameter() instanceof StreamExpressionValue) {
        otherParams.put(param.getName(), ((StreamExpressionValue)param.getParameter()).getValue());
      }
    }

    if(1 != streamParams.size()) {
      throw new IOException(String.format(Locale.ROOT, "Invalid %s expressions '%s' - expecting exactly 1 incoming stream but found %d", factory.getFunctionName(getClass()), expression, streamParams.size()));
    }

    if(null == bootstrapServers) {
      throw new IOException(String.format(Locale.ROOT, "Invalid %s expressions '%s' - expecting a list of bootstrapServers but found none", factory.getFunctionName(getClass()), expression));
    }

    if(null != keyEvaluator) {
      if(null == keyType) {
        throw new IOException(String.format(Locale.ROOT, "Invalid %s expressions '%s' - failed to determine keyType for key '%s' (string,int,long,boolean,double are all accepted)", factory.getFunctionName(getClass()), expression, keyEvaluator.toExpression(factory)));
      }
    }

    if(null != valueEvaluator) {
      if(null == valueType) {
        throw new IOException(String.format(Locale.ROOT, "Invalid %s expressions '%s' - failed to determine valueType for value '%s' (string,int,long,boolean,double are all accepted)", factory.getFunctionName(getClass()), expression, valueEvaluator.toExpression(factory)));
      }
    }
    else {
      valueType = String.class.getName();
    }

    this.incomingStream = factory.constructStream(streamParams.get(0));
    this.bootstrapServers = bootstrapServers;
    this.topicEvaluator = topicEvaluator;
    this.keyEvaluator = keyEvaluator;
    this.keyType = keyType;
    this.valueEvaluator = valueEvaluator;
    this.valueType = valueType;
    this.partitionEvaluator = partitionEvaluator;
    this.otherProducerParams = otherParams;
  }

  @Override
  public void open() throws IOException {
    // producerClient = new
  }

  @Override
  public Tuple read() throws IOException {
    return null;
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

  private StreamEvaluator getEvaluatorParameter(String paramName, StreamExpression expression, StreamFactory factory) throws IOException {
    StreamExpressionNamedParameter param = factory.getNamedOperand(expression, paramName);
    if(null != param) {
      if(param.getParameter() instanceof StreamExpression) {
        if(factory.doesRepresentTypes((StreamExpression)param.getParameter(), StreamEvaluator.class)) {
          return factory.constructEvaluator((StreamExpression)param.getParameter());
        }
      }
    }

    return null;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return toExpression(factory, true);
  }

  public StreamExpressionParameter toExpression(StreamFactory factory, boolean includeStream) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(getClass()));

    if(includeStream){
      if(incomingStream instanceof Expressible){
        expression.addParameter(((Expressible)incomingStream).toExpression(factory));
      }
      else{
        throw new IOException("Failed to create expression - incoming stream is not Expressible");
      }
    }

    expression.addParameter(new StreamExpressionNamedParameter("bootstrapServers", bootstrapServers));
    if(null != topicEvaluator){
      expression.addParameter(new StreamExpressionNamedParameter("topic", topicEvaluator.toExpression(factory )));
    }

    if(null != keyEvaluator){
      expression.addParameter(new StreamExpressionNamedParameter("key", keyEvaluator.toExpression(factory)));
      if(null != keyType){
        expression.addParameter(new StreamExpressionNamedParameter("keyType", keyType));
      }
    }

    if(null != valueEvaluator){
      expression.addParameter(new StreamExpressionNamedParameter("value", valueEvaluator.toExpression(factory)));
      if(null != valueType){
        expression.addParameter(new StreamExpressionNamedParameter("valueType", valueType));
      }
    }

    if(null != partitionEvaluator){
      expression.addParameter(new StreamExpressionNamedParameter("partition", partitionEvaluator.toExpression(factory)));
    }

    for(Entry<String,String> entry : otherProducerParams.entrySet()){
      expression.addParameter(new StreamExpressionNamedParameter(entry.getKey(), entry.getValue()));
    }

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    // A producer stream is backward wrt the order in the explanation. This stream is the "child"
    // while the collection we're updating is the parent.

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId() + "-kafka-topic");

    explanation.setFunctionName(String.format(Locale.ROOT, "kafka (%s)", topicEvaluator.toExpression(factory)));
    explanation.setImplementingClass("Kafka");
    explanation.setExpressionType(ExpressionType.DATASTORE);
    explanation.setExpression("Produce into " + bootstrapServers);

    // child is a datastore so add it at this point
    StreamExplanation child = new StreamExplanation(getStreamNodeId().toString());
    child.setFunctionName(String.format(Locale.ROOT, factory.getFunctionName(getClass())));
    child.setImplementingClass(getClass().getName());
    child.setExpressionType(ExpressionType.STREAM_DECORATOR);
    child.setExpression(toExpression(factory, false).toString());
    child.addChild(incomingStream.toExplanation(factory));

    explanation.addChild(child);

    return explanation;
  }

  @Override
  public void close() throws IOException {
    if(null != producerClient) {
      producerClient.close();
    }
  }

  @Override
  public StreamComparator getStreamSort() {
    return incomingStream.getStreamSort();
  }

  @Override
  public void setStreamContext(StreamContext context) {
    this.context = context;
  }

  @Override
  public List<TupleStream> children() {
    List<TupleStream> l = new ArrayList<>();
    l.add(incomingStream);
    return l;
  }

}
