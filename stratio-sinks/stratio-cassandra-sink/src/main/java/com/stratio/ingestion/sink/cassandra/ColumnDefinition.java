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