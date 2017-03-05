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
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTopicProducerStream extends TupleStream implements Expressible {
  private Logger log = LoggerFactory.getLogger(getClass());
  
  private StreamContext context;

  private KafkaProducer<?,?> producerClient;
  
  public KafkaTopicProducerStream(StreamExpression expression, StreamFactory factory){
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
