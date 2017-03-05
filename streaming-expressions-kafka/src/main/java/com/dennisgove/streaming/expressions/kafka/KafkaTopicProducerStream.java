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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.AddEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTopicProducerStream extends TupleStream implements Expressible {
  private Logger log = LoggerFactory.getLogger(getClass());
  
  private StreamContext context;

  private KafkaProducer<?,?> producerClient;
  
  public KafkaTopicProducerStream(StreamExpression expression, StreamFactory factory) throws IOException{
    /*
     *  kafkaProducer(
     *    <incoming stream>, 
     *    topic=<evaluator>, 
     *    bootstrapServers=<kafka servers>,
     *    key=<evaluator>, // optional, if not provided then no key
     *    keyType=[string,int,long,double,boolean], // only required if using key and can't figure out from key param
     *    value=<evaluator> // optional, if not provided then whole tuple as json string is used
     *    valueType=[string,int,long,double,boolean], // only required if can't figure out from value param
     *    partition=<evaluator>, // optional, if not provided then no partition used when sending record
     *  )
     *  
     *  Anything that is <evaluator> is allowed to be any valid StreamEvaluator, such as
     *  raw("foo") // raw value "foo"
     *  "foo" // value of field foo
     *  add(foo, bar) // value of foo + value of bar
     */
    
    List<StreamExpression> streamParams = factory.getExpressionOperandsRepresentingTypes(expression, TupleStream.class, Expressible.class);
    
    String bootstrapServers = getStringParameter("bootstrapServers", expression, factory);
    String keyType = getStringParameter("keyType", expression, factory);
    String valueType = getStringParameter("valueType", expression, factory);
    
    StreamEvaluator topic = getEvaluatorParameter("topic", expression, factory);
    StreamEvaluator key = getEvaluatorParameter("key", expression, factory);
    StreamEvaluator value = getEvaluatorParameter("value", expression, factory);
    StreamEvaluator partition = getEvaluatorParameter("partition", expression, factory);    
    
    Map<String,String> otherParams = new HashMap<String,String>();
    Set<String> ignoreParams = new HashSet<String>(){ { add("bootstrapServers"); add("keyType"); add("valueType"); add("topic"); add("key"); add("value"); add("partition"); } };
    for(StreamExpressionNamedParameter param : factory.getNamedOperands(expression).stream().filter(item -> !ignoreParams.contains(item.getName())).collect(Collectors.toList())){
      if(param.getParameter() instanceof StreamExpressionValue){
        otherParams.put(param.getName(), ((StreamExpressionValue)param.getParameter()).getValue());
      }
    }
  }
  
  private String getStringParameter(String paramName, StreamExpression expression, StreamFactory factory){
    StreamExpressionNamedParameter param = factory.getNamedOperand(expression, paramName);
    if(null != param){
      if(param.getParameter() instanceof StreamExpressionValue){
        return ((StreamExpressionValue)param.getParameter()).getValue();
      }
    }
    
    return null;
  }

  private StreamEvaluator getEvaluatorParameter(String paramName, StreamExpression expression, StreamFactory factory) throws IOException{
    StreamExpressionNamedParameter param = factory.getNamedOperand(expression, paramName);
    if(null != param){
      if(param.getParameter() instanceof StreamExpression){
        if(factory.doesRepresentTypes((StreamExpression)param.getParameter(), StreamEvaluator.class)){
          return factory.constructEvaluator((StreamExpression)param.getParameter());
        }
      }
    }
    
    return null;
  }


  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return null;
  }

  @Override
  public void setStreamContext(StreamContext context) {
    this.context = context;
  }

  @Override
  public List<TupleStream> children() {
    return null;
  }

  @Override
  public void open() throws IOException {
  }

  @Override
  public void close() throws IOException {
    if(null != producerClient){
      producerClient.close();
    }
  }

  @Override
  public Tuple read() throws IOException {
    return null;
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return null;
  }

}
