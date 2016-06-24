/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.ingestion.sink.elasticsearch;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Basic serializer that serializes the event body and header fields into
 * individual fields</p>
 *
 * A best effort will be used to determine the content-type, if it cannot be
 * determined fields will be indexed as Strings
 */
public class ElasticSearchDynamicSerializer implements
    ElasticSearchEventSerializer {

  @Override
  public void configure(Context context) {
    // NO-OP...
  }

  @Override
  public void configure(ComponentConfiguration conf) {
    // NO-OP...
  }

  @Override
  public XContentBuilder getContentBuilder(Event event) throws IOException {
    XContentBuilder builder = jsonBuilder().startObject();
    appendBody(builder, event);
    appendHeaders(builder, event);
    return builder;
  }

  private void appendBody(XContentBuilder builder, Event event)
      throws IOException {
    ContentBuilderUtil.appendField(builder, "body", event.getBody());
  }

  private void appendHeaders(XContentBuilder builder, Event event)
      throws IOException {
    Map<String, String> headers = event.getHeaders();
    for (String key : headers.keySet()) {
      ContentBuilderUtil.appendField(builder, key,
          headers.get(key).getBytes(charset));
    }
  }

}
