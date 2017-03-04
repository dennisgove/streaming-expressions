/**
 * Copyright 2017 Dennis Gove
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStream extends TupleStream implements Expressible {
  private Logger log = LoggerFactory.getLogger(getClass());

  /* (non-Javadoc)
   * @see org.apache.solr.common.MapSerializable#toMap(java.util.Map)
   */
  public Map toMap(Map<String,Object> map) {
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.solr.client.solrj.io.stream.expr.Expressible#toExpression(org.apache.solr.client.solrj.io.stream.expr.StreamFactory)
   */
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.solr.client.solrj.io.stream.TupleStream#setStreamContext(org.apache.solr.client.solrj.io.stream.StreamContext)
   */
  @Override
  public void setStreamContext(StreamContext context) {
  }

  /* (non-Javadoc)
   * @see org.apache.solr.client.solrj.io.stream.TupleStream#children()
   */
  @Override
  public List<TupleStream> children() {
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.solr.client.solrj.io.stream.TupleStream#open()
   */
  @Override
  public void open() throws IOException {
  }

  /* (non-Javadoc)
   * @see org.apache.solr.client.solrj.io.stream.TupleStream#close()
   */
  @Override
  public void close() throws IOException {
  }

  /* (non-Javadoc)
   * @see org.apache.solr.client.solrj.io.stream.TupleStream#read()
   */
  @Override
  public Tuple read() throws IOException {
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.solr.client.solrj.io.stream.TupleStream#getStreamSort()
   */
  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.solr.client.solrj.io.stream.TupleStream#toExplanation(org.apache.solr.client.solrj.io.stream.expr.StreamFactory)
   */
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return null;
  }

}
