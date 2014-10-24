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
import java.util.Locale;
import java.util.Map;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;

import cue.lang.Counter;
import cue.lang.NGramIterator;
import cue.lang.stop.StopWords;

public class TopWordsBuilder implements CommandBuilder {

	private final static String COMMAND_NAME = "topWords";
	
	@Override
	public Collection<String> getNames() {
		return Collections.singletonList(COMMAND_NAME);
	}

	@Override
	public Command build(Config config, Command parent, Command child,
			MorphlineContext context) {
		return new TopWords(this, config, parent, child, context);
	}

	private static final class TopWords extends AbstractCommand {
		private final static String LANGUAGE_FIELD = "language";
		private final static String INPUT_FIELD = "input";
		private final static String OUTPUT_FIELD = "output";
		
		private final String languageFieldName;
		private final String inputFieldName;
		private final String outputFieldName;


		public TopWords(CommandBuilder builder, Config config, Command parent, Command child,
				final MorphlineContext context) {

			super(builder, config, parent, child, context);
			this.languageFieldName = getConfigs().getString(config, LANGUAGE_FIELD);
			this.inputFieldName = getConfigs().getString(config, INPUT_FIELD);
			this.outputFieldName = getConfigs().getString(config, OUTPUT_FIELD);
			validateArguments();
		}

		@Override
		protected boolean doProcess(Record record) {
			String language = (String) record.get(languageFieldName).get(0);
			String inputText = (String) record.get(inputFieldName).get(0);
			Locale locale = new Locale(language);
			StopWords stopwords = StopWordsFinder.find(language);

			final Counter<String> ngrams = new Counter<String>();
			for (final String ngram : new NGramIterator(1, inputText, locale, stopwords)) {
			    ngrams.note(ngram.toLowerCase(locale));
			}
			if (!ngrams.getMostFrequent(1).isEmpty())
				record.put(outputFieldName, ngrams.getMostFrequent(1).get(0));

			// pass record to next command in chain:
			return super.doProcess(record);
		}

	}
	
	private final static class StopWordsFinder {

    private static Map<String,StopWords> stopWordsMap = ImmutableMap.<String, StopWords>builder()
        .put("es", StopWords.Spanish)
        .put("en", StopWords.English)
        .put("ca", StopWords.Catalan)
        .put("de", StopWords.German)
        .put("ar", StopWords.Arabic)
        .put("hr", StopWords.Croatian)
			  .put("cs", StopWords.Czech)
        .put("nl", StopWords.Dutch)
        .put("da", StopWords.Danish)
        .put("eo", StopWords.Esperanto)
        .put("fi", StopWords.Finnish)
			  .put("fr", StopWords.French)
        .put("el", StopWords.Greek)
        .put("hi", StopWords.Hindi)
        .put("hu", StopWords.Hungarian)
        .put("it", StopWords.Italian)
			  .put("la", StopWords.Latin)
        .put("no", StopWords.Norwegian)
        .put("pl", StopWords.Polish)
        .put("pt", StopWords.Portuguese)
        .put("ro", StopWords.Romanian)
        .put("ru", StopWords.Russian)
			  .put("sl", StopWords.Slovenian)
			  .put("sk", StopWords.Slovak)
			  .put("sv", StopWords.Swedish)
			  .put("he", StopWords.Hebrew)
			  .put("tk", StopWords.Turkish)
        .build();

        public static StopWords find(String language) {
          StopWords result = stopWordsMap.get(language);
          if (result == null) {
            return StopWords.Custom;
          }
          return result;
        }

		}

}
