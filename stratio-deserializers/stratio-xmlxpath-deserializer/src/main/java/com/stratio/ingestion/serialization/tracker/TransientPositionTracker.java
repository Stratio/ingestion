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
package com.stratio.ingestion.serialization.tracker;

import java.io.IOException;

import org.apache.flume.serialization.PositionTracker;

public class TransientPositionTracker implements PositionTracker {

  private final String target;
  private long position = 0;

  public TransientPositionTracker(String target) {
    this.target = target;
  }

  @Override
  public void storePosition(long position) throws IOException {
    this.position = position;
  }

  @Override
  public long getPosition() {
    return position;
  }

  @Override
  public String getTarget() {
    return target;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
