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
package formatter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.stratio.ingestion.model.AgentComponent;
import com.stratio.ingestion.model.Attribute;
import com.stratio.ingestion.model.ModelValidator;
import com.stratio.ingestion.model.channel.Channel;
import com.stratio.ingestion.model.sink.Sink;
import com.stratio.ingestion.model.source.Source;

/**
 * Created by miguelsegura on 22/09/15.
 */
public class ModelFormatter {



//    private static final Logger log = LoggerFactory.getLogger(PropertiesFormatter.class);

    private String element;
    private ModelValidator modelValidator = new ModelValidator();

    public ModelFormatter(AgentComponent agentComponent)
            throws IOException {

        try {

            String ruta = "src/test"
                    + "/resources/prop2.properties";
            File archivo = new File(ruta);
            BufferedWriter bw;

            bw = new BufferedWriter(new FileWriter(archivo));


            List<Source> sources = agentComponent.getSources();
            List<Sink> sinks = agentComponent.getSinks();
            List<Channel> channels = agentComponent.getChannels();

            bw.write("#Name the components on this agent");
            bw.newLine();
            bw.newLine();

            /*** Create Sources ***/
            element = "sources";
            bw.write("##### " + element.toUpperCase() + " #####");
            bw.newLine();
            bw.newLine();
            writeSources(sources, bw, element);

            /*** Create Sinks ***/
            element = "sinks";
            bw.write("##### " + element.toUpperCase() + " #####");
            bw.newLine();
            bw.newLine();
            writeSinks(sinks, bw, element);

            /*** Create Channels ***/
            element = "channels";
            bw.write("##### " + element.toUpperCase() + " #####");
            bw.newLine();
            bw.newLine();
            writeChannels(channels, bw, element);

            /*** Join Elements ***/
            bw.write("##### UNION #####");
            bw.newLine();
            bw.newLine();
            element = "sources";
            writeSourcesConnections(sources, bw, element);
            element = "sinks";
            writeSinksConnections(sinks, bw, element);



            bw.close();


            return;
        } catch (Exception e) {
            throw new IOException("Cannot serialize JSON", e);
        }
    }

    private void writeSources(List<Source> sources, BufferedWriter bw, String elements) throws IOException {
        try {
            String idSources = "";
            for (Source source : sources) {
                idSources = idSources + " " + source.getId();
            }
            bw.write("a1." + elements + "=" + idSources);
            bw.newLine();
            bw.newLine();

            for (Source source : sources) {
                bw.write("a1." + elements + "." + source.getId() + ".type=" + source.getType());
                bw.newLine();

                List<Attribute> attributes = source.getSettings();

                writeAttributes(attributes, bw, elements, source.getId());

                bw.newLine();
            }
        }catch (Exception e) {
            throw new IOException("Cannot serialize JSON sources", e);
        }
    }

    private void writeSinks(List<Sink> sinks, BufferedWriter bw, String elements) throws IOException {
        try {
            String idSinks = "";
            for (Sink sink : sinks) {
                idSinks = idSinks + " " + sink.getId();
            }
            bw.write("a1." + elements + "=" + idSinks);
            bw.newLine();
            bw.newLine();

            for (Sink sink : sinks) {
                bw.write("a1." + elements + "." + sink.getId() + ".type=" + sink.getType());
                bw.newLine();

                List<Attribute> attributes = sink.getSettings();

                writeAttributes(attributes, bw, elements, sink.getId());
//                for (Attribute atrib : attributes) {
//                    String type = atrib.getType();
//
//                    if (type.equals("string") && atrib.getValueString() != null) {
//                        bw.write("a1." + elements + "." + sink.getId() + "." + atrib.getId() + "=" + atrib
//                                .getValueString());
//                        bw.newLine();
//                    }
//                    if (type.equals("integer")) {
//                        bw.write("a1." + elements + "." + sink.getId() + "." + atrib.getId() + "=" + atrib
//                                .getValueInteger());
//                        bw.newLine();
//                    }
//                    if (type.equals("boolean")) {
//                        bw.write("a1." + elements + "." + sink.getId() + "." + atrib.getId() + "=" + atrib
//                                .getValueBoolean());
//                        bw.newLine();
//                    }
//                }
                bw.newLine();
            }
        }catch (Exception e) {
            throw new IOException("Cannot serialize JSON sinks", e);
        }
    }

    private void writeChannels(List<Channel> channels, BufferedWriter bw, String elements) throws IOException {
        try {
            String idChannels = "";
            for (Channel channel : channels) {
                idChannels = idChannels + " " + channel.getId();
            }
            bw.write("a1." + elements + "=" + idChannels);
            bw.newLine();
            bw.newLine();

            for (Channel channel : channels) {
                bw.write("a1." + elements + "." + channel.getId() + ".type=" + channel.getType());
                bw.newLine();

                List<Attribute> attributes = channel.getSettings();

                writeAttributes(attributes, bw, elements, channel.getId());
//                for (Attribute atrib : attributes) {
//                    String type = atrib.getType();
//
//                    if (type.equals("string") && atrib.getValueString() != null) {
//                        bw.write("a1." + elements + "." + channel.getId() + "." + atrib.getId() + "=" + atrib
//                                .getValueString());
//                        bw.newLine();
//                    }
//                    if (type.equals("integer")) {
//                        bw.write("a1." + elements + "." + channel.getId() + "." + atrib.getId() + "=" + atrib
//                                .getValueInteger());
//                        bw.newLine();
//                    }
//                    if (type.equals("boolean")) {
//                        bw.write("a1." + elements + "." + channel.getId() + "." + atrib.getId() + "=" + atrib
//                                .getValueBoolean());
//                        bw.newLine();
//                    }
//                }
                bw.newLine();
            }
        }catch (Exception e) {
            throw new IOException("Cannot serialize JSON channels", e);
        }
    }

    private void writeSourcesConnections(List<Source> sources, BufferedWriter bw, String elements) throws IOException {
        try {
            for (Source source : sources) {
                String channels = source.getChannels();

                modelValidator.checkChannelSource(source);

                bw.write("a1." + elements + "." + source.getId() + ".channels=" + channels);
                bw.newLine();

            }
        }catch (Exception e) {
            throw new IOException("Cannot connect sources", e);
        }
    }

    private void writeSinksConnections(List<Sink> sinks, BufferedWriter bw, String elements) throws IOException {
        try {
            for (Sink sink : sinks) {
                String channels = sink.getChannels();

                modelValidator.checkChannelSink(sink);

                bw.write("a1." + elements + "." + sink.getId() + ".channels=" + channels);
                bw.newLine();


            }
        }catch (Exception e) {
            throw new IOException("Cannot connect sinks", e);
        }
    }

    private void writeAttributes(List<Attribute> attributes, BufferedWriter bw, String element, String elementId)
            throws IOException {
        try {
            for (Attribute atrib : attributes) {
                String type = atrib.getType();

                modelValidator.checkAttributes(atrib);
//                boolean required = atrib.getRequired();

//                if(required && atrib.getValueString().equals(null) && atrib.getValueInteger().equals(null) && atrib
//                        .getValueBoolean().equals(null)) {
//                    throw new IOException("Cannot write attributes, required field without value");
//                }

                if (type.equals("string") && atrib.getValueString() != null) {
                    bw.write("a1." + element + "." + elementId + "." + atrib.getId() + "=" + atrib
                            .getValueString());
                    bw.newLine();
                }
                if (type.equals("integer") && atrib.getValueInteger() != null) {
                    bw.write("a1." + element + "." + elementId + "." + atrib.getId() + "=" + atrib
                            .getValueInteger());
                    bw.newLine();
                }
                if (type.equals("boolean") && atrib.getValueBoolean() != null) {
                    bw.write("a1." + element + "." + elementId + "." + atrib.getId() + "=" + atrib
                            .getValueBoolean());
                    bw.newLine();
                }

            }
        }catch(Exception e) {
            throw new IOException("Can't write attributes, required field not found", e);
        }
    }
}
