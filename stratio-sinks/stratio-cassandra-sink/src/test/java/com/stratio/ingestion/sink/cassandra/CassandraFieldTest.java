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
package com.stratio.ingestion.sink.cassandra;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.fest.assertions.Assertions.*;

@RunWith(JUnit4.class)
public class CassandraFieldTest {

    @Test
    public void testHashCodeAndEquals() {
        String[][] pairs = new String[][]{
                new String[]{"foo", "bar"},
                new String[]{"foo", null},
                new String[]{null, "bar"},
                new String[]{null, null},
        };
        for (String[] pair : pairs) {
            final CassandraField<String> one = new CassandraField<String>(pair[0], pair[1]);
            final CassandraField<String> other = new CassandraField<String>(pair[0], pair[1]);
            assertThat(one).isEqualTo(one);
            assertThat(one).isEqualTo(other);
            assertThat(one.hashCode()).isEqualTo(other.hashCode());
        }
        for (String[] onePair : pairs) {
            for (String[] otherPair : pairs) {
                if (onePair == otherPair) {
                    continue;
                }
                final CassandraField<String> one = new CassandraField<String>(onePair[0], onePair[1]);
                final CassandraField<String> other = new CassandraField<String>(otherPair[0], otherPair[1]);
                assertThat(one).isNotEqualTo(other);
                assertThat(one.hashCode()).isNotEqualTo(other.hashCode());
            }
        }
    }

}
