package com.stratio.ingestion.morphline.nlp;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

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
		
		public static StopWords find(String language) {
			switch (language) {
			case "es":
				return StopWords.Spanish;
			case "en":
				return StopWords.English;
			case "ca":
				return StopWords.Catalan;
			case "de":
				return StopWords.German;
			case "ar":
				return StopWords.Arabic;
			case "hr":
				return StopWords.Croatian;
			case "cs":
				return StopWords.Czech;
			case "nl":
				return StopWords.Dutch;
			case "da":
				return StopWords.Danish;
			case "eo":
				return StopWords.Esperanto;
			case "fi":
				return StopWords.Finnish;
			case "fr":
				return StopWords.French;
			case "el":
				return StopWords.Greek;
			case "hi":
				return StopWords.Hindi;
			case "hu":
				return StopWords.Hungarian;
			case "it":
				return StopWords.Italian;
			case "la":
				return StopWords.Latin;
			case "no":
				return StopWords.Norwegian;
			case "pl":
				return StopWords.Polish;
			case "pt":
				return StopWords.Portuguese;
			case "ro":
				return StopWords.Romanian;
			case "ru":
				return StopWords.Russian;
			case "sl":
				return StopWords.Slovenian;
			case "sk":
				return StopWords.Slovak;
			case "sv":
				return StopWords.Swedish;
			case "he":
				return StopWords.Hebrew;
			case "tk":
				return StopWords.Turkish;
			default:
				return StopWords.Custom;
			}
		}
	}
	
}
