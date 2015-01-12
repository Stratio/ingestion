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
package com.stratio.ingestion.source.batch.rest.checkpoint;

import java.util.Map;

import org.apache.flume.Context;

/**
 * Created by eambrosio on 7/01/15.
 */
public interface CheckpointType {

    String populateCheckpoint(Object checkpoint, Context context);

    String populateDefaultCheckpoint(Context context);

    Boolean isValid(Object lastCheckpoint, Object currentCheckpoint,Map<String,String> properties);
}
