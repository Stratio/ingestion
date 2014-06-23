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
package com.stratio.ingestion.morphline.nlp;

import java.util.Collection;
import java.util.Collections;

import org.apache.tika.language.LanguageIdentifier;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

public class LanguageExtractorBuilder implements CommandBuilder {

	private final static String COMMAND_NAME = "languageExtractor";

	@Override
	public Collection<String> getNames() {
		return Collections.singletonList(COMMAND_NAME);
	}

	@Override
	public Command build(Config config, Command parent, Command child,
			MorphlineContext context) {
		return new LanguageExtractor(this, config, parent, child, context);
	}

	private static final class LanguageExtractor extends AbstractCommand {
		private final static String INPUT_FIELD = "input";
		private final static String OUTPUT_FIELD = "output";

		private final String inputFieldName;
		private final String outputFieldName;

		public LanguageExtractor(CommandBuilder builder, Config config,
				Command parent, Command child, final MorphlineContext context) {

			super(builder, config, parent, child, context);
			this.inputFieldName = getConfigs().getString(config, INPUT_FIELD);
			this.outputFieldName = getConfigs().getString(config, OUTPUT_FIELD);
			validateArguments();
		}

		@Override
		protected boolean doProcess(Record record) {
			Object value = record.get(inputFieldName).get(0);

			LanguageIdentifier li = new LanguageIdentifier((String) value);
			record.put(outputFieldName, li.getLanguage());

			// pass record to next command in chain:
			return super.doProcess(record);
		}

	}

}
