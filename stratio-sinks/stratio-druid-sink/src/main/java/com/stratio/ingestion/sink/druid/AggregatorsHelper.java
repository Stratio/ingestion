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
package com.stratio.ingestion.sink.druid;

import java.util.ArrayList;
import java.util.List;

import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;

/**
 * Created by eambrosio on 30/03/15.
 */
public class AggregatorsHelper {

    public static List<AggregatorFactory> build(String rawAggregators) {
//        final List<String> splittedAggregators = Arrays.asList(rawAggregators.split(","));
        //        List<AggregatorFactory> list = new ArrayList<AggregatorFactory>();
        //        for (String aggregator : aggregators) {
        //            list.add(null);
        //        }
        //TODO pending implementation
        List<AggregatorFactory> aggregators = new ArrayList<AggregatorFactory>();
        aggregators.add(new CountAggregatorFactory("count"));
        return aggregators;

    }

}
