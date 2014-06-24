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

import com.typesafe.config.Config;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;

public class TokenizerBuilder implements CommandBuilder {

	private final static String COMMAND_NAME = "tokenize";
	
	@Override
	public Collection<String> getNames() {
		return Collections.singletonList(COMMAND_NAME);
	}

	@Override
	public Command build(Config config, Command parent, Command child,
			MorphlineContext context) {
		return new Tokenize(this, config, parent, child, context);
	}

	private static final class Tokenize extends AbstractCommand {
        private final Analyzer analyzer;

        public Tokenize(CommandBuilder builder, Config config, Command parent, Command child,
                        final MorphlineContext context) {

            super(builder, config, parent, child, context);

            this.analyzer = new StandardAnalyzer(Version.LUCENE_47);

            validateArguments();
        }

        @Override
        protected boolean doProcess(Record record) {
            final String body = (String) record.getFirstValue("message");
            try (TokenStream stream = analyzer.tokenStream("body", new StringReader(body))) {
                CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
                stream.reset();
                while (stream.incrementToken()) {
                    final Record newRecord = new Record();
                    newRecord.put("message", termAtt.toString());
                    super.doProcess(newRecord);
                }
                stream.end();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            return true;
        }

    }
	
}
