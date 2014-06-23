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
package com.stratio.ingestion.morphline.wikipediacleaner;

import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;

import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

public class WikipediaCleanerBuilder implements CommandBuilder {

	private final static String COMMAND_NAME = "wikipediaCleaner";
	private final static String TEMPLATE_PATTERN = "\\{\\{[^{}]*\\}\\}";

	@Override
	public Collection<String> getNames() {
		return Collections.singletonList(COMMAND_NAME);
	}

	@Override
	public Command build(Config config, Command parent, Command child,
			MorphlineContext context) {
		return new WikipediaCleaner(this, config, parent, child, context);
	}

	private static final class WikipediaCleaner extends AbstractCommand {

		private final static String INPUT_FIELD = "input";
		private final static String OUTPUT_FIELD = "output";

		private String inputFieldName;
		private String outputFieldName;

		public WikipediaCleaner(CommandBuilder builder, Config config,
				Command parent, Command child, final MorphlineContext context) {

			super(builder, config, parent, child, context);
			this.inputFieldName = getConfigs().getString(config, INPUT_FIELD);
			this.outputFieldName = getConfigs().getString(config, OUTPUT_FIELD);
			validateArguments();
		}

		@Override
		protected boolean doProcess(Record record) {
			Object value = record.get(inputFieldName).get(0);

			WikiModel wikiModel = new WikiModel(
					"http://www.mywiki.com/wiki/${image}",
					"http://www.mywiki.com/wiki/${title}");

			String cleanText = wikiModel.render(new PlainTextConverter(),
					value.toString());

			Pattern p = Pattern.compile(TEMPLATE_PATTERN);
			Matcher m = p.matcher(cleanText);
			while (m.find()) {
				cleanText = m.replaceAll("");
				m = p.matcher(cleanText);
			}

			record.put(outputFieldName, cleanText.trim());

			// pass record to next command in chain:
			return super.doProcess(record);
		}
	}

}
