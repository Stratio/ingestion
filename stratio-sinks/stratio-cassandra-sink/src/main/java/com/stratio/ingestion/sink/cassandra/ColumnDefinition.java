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

import java.io.Serializable;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

class ColumnDefinition implements Serializable {

	private static final long serialVersionUID = 1L;

	@JsonProperty("fieldDefinitions")
	private List<FieldDefinition> fields;

	public List<FieldDefinition> getFields() {
		return this.fields;
	}

	public void setFields(List<FieldDefinition> fields) {
		this.fields = fields;
	}

	public FieldDefinition getFieldDefinitionByName(String name) {
		for (FieldDefinition fd : this.fields)
			if (fd.getColumnName().equals(name))
				return fd;
		return null;
	}

}