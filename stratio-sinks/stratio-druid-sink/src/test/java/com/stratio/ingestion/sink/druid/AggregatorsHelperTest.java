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

import org.fest.assertions.Assertions;
import org.junit.Test;

import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;

/**
 * Created by eambrosio on 29/04/15.
 */
public class AggregatorsHelperTest {
    private final List<AggregatorFactory> expectedList = buildExpectedList();

    @Test
    public void buildWithEmptyList() {
        String emptyList = "";
        Assertions.assertThat(AggregatorsHelper.build(emptyList)).isEqualTo(expectedList);
    }

    @Test
    public void buildWithNoValidList() {
        String emptyList = ",,21$%";
        Assertions.assertThat(AggregatorsHelper.build(emptyList)).isEqualTo(expectedList);
    }

    @Test
    public void buildWithValidList() {
        String emptyList = "count,longSum,doubleSum";
        Assertions.assertThat(AggregatorsHelper.build(emptyList)).isEqualTo(expectedList);
    }

    private List< AggregatorFactory >  buildExpectedList() {
        List< AggregatorFactory > expectedAggregators= new ArrayList<AggregatorFactory>();
        expectedAggregators.add(new CountAggregatorFactory("count"));
        return expectedAggregators;
    }

}