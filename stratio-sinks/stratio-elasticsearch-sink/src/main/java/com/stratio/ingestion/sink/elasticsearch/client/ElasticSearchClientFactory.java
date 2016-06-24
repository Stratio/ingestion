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
package com.stratio.ingestion.sink.elasticsearch.client;

import com.stratio.ingestion.sink.elasticsearch.ElasticSearchEventSerializer;
import com.stratio.ingestion.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;

/**
 * Internal ElasticSearch client factory. Responsible for creating instance
 * of ElasticSearch clients.
 */
public class ElasticSearchClientFactory {
  public static final String TransportClient = "transport";
  public static final String RestClient = "rest";

  /**
   *
   * @param clientType
   *    String representation of client type
   * @param hostNames
   *    Array of strings that represents hosntames with ports (hostname:port)
   * @param clusterName
   *    Elasticsearch cluster name used only by Transport Client
   * @param serializer
   *    Serializer of flume events to elasticsearch documents
   * @return
   */
  public ElasticSearchClient getClient(String clientType, String[] hostNames,
      String clusterName, ElasticSearchEventSerializer serializer,
      ElasticSearchIndexRequestBuilderFactory indexBuilder) throws NoSuchClientTypeException {
    if (clientType.equalsIgnoreCase(TransportClient) && serializer != null) {
      return new ElasticSearchTransportClient(hostNames, clusterName, serializer);
    } else if (clientType.equalsIgnoreCase(TransportClient) && indexBuilder != null) { 
      return new ElasticSearchTransportClient(hostNames, clusterName, indexBuilder);
    } else if (clientType.equalsIgnoreCase(RestClient) && serializer != null) {
      return new ElasticSearchRestClient(hostNames, serializer);
    }
    throw new NoSuchClientTypeException();
  }

  /**
   * Used for tests only. Creates local elasticsearch instance client.
   *
   * @param clientType Name of client to use
   * @param serializer Serializer for the event
   * @param indexBuilder Index builder factory
   *
   * @return Local elastic search instance client
   */
  public ElasticSearchClient getLocalClient(String clientType, ElasticSearchEventSerializer serializer,
          ElasticSearchIndexRequestBuilderFactory indexBuilder) throws NoSuchClientTypeException {
    if (clientType.equalsIgnoreCase(TransportClient) && serializer != null) {
      return new ElasticSearchTransportClient(serializer);
    } else if (clientType.equalsIgnoreCase(TransportClient) && indexBuilder != null)  {
      return new ElasticSearchTransportClient(indexBuilder);
    } else if (clientType.equalsIgnoreCase(RestClient)) {
    }
    throw new NoSuchClientTypeException();
  }
}
