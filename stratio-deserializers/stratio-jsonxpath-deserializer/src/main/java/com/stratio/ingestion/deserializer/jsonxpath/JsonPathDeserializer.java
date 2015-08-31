package com.stratio.ingestion.deserializer.jsonxpath;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import net.minidev.json.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.apache.flume.serialization.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;


//@formatter:off
/**
 * <p>XML XPath Deserializer. Read InputStream as XML compile a XPathExpression and create event for each element
 * result of apply that expression to the xml in headers. Maintain whole xml in body.</p>.
 * <ul>
 * <li><em>outputField</em>: Output Field in header where put events. Default: element.</li>
 * <li><em>expression</em>: XPath expression. </li>
 * </ul>
 *
 * <p>A special option is the chance to evaluate xpath expression for each event and add result in a header. For example:</p>
 * <code>
 * <li>headers.author= <XPathExpression> will put result of expression in author field of header.</li>
 * </code>
 */
//@formatter:on
public class JsonPathDeserializer implements EventDeserializer {

    private static final Logger log = LoggerFactory.getLogger(JsonPathDeserializer.class);

    private static final String CONF_XPATH_EXPRESSION = "expression";
    private static final String CONF_OUTPUT_HEADER = "outputHeader";
    private static final String CONF_OUTPUT_BODY = "outputBody";

    private static final boolean DEFAULT_OUTPUT_BODY = true;

    private boolean isOpen;
    private String outputHeader;
    private boolean outputBody;
    private ReadContext ctx;
    private String body;
    private List list = null;
    private ListIterator markIt, currentIt;

    JsonPathDeserializer(Context context, ResettableInputStream in) throws IOException {
        try {
            final String expression = context.getString(CONF_XPATH_EXPRESSION);
            outputBody = context.getBoolean(CONF_OUTPUT_BODY, DEFAULT_OUTPUT_BODY);
            if (!outputBody) {
                if (!context.containsKey(CONF_OUTPUT_HEADER)) {
                    throw new ConfigurationException(
                            String.format("Either %s must be false or %s must be defined", CONF_OUTPUT_BODY, CONF_OUTPUT_HEADER));
                }
                outputHeader = context.getString(CONF_OUTPUT_HEADER);
            }

            ctx = JsonPath.parse(new ResettableInputStreamInputStream(in));

            if (ctx != null) {
                isOpen = true;
            }

            body = ctx.jsonString();
            list = ctx.read(expression);

            markIt = list.listIterator();
            currentIt = list.listIterator();

        } catch (Exception e) {
           throw new IOException("Cannot serialize JSON", e);

        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                log.warn("Error while closing input stream");
            }
        }
    }

    @Override
    public Event readEvent() throws IOException {
        ensureOpen();
        if (!currentIt.hasNext()) {
            return null;
        } else {
            Object result = currentIt.next();
            String node;
            if(result instanceof String) {
                node = (String)result;
            } else {
                node = new JSONObject((Map)result).toJSONString();
            }
            if (outputBody) {
                return EventBuilder.withBody(node, Charsets.UTF_8);
            } else {
                final Event event = EventBuilder.withBody(body, Charsets.UTF_8);
                event.getHeaders().put(outputHeader, node);
                return event;
            }
        }
    }

    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        ensureOpen();
        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent();
            if (event != null) {
                events.add(event);
            }
        }
        return events;
    }

    @Override
    public void mark() throws IOException {
        ensureOpen();
        int index = currentIt.previousIndex();
        markIt = index >= 0 ? list.listIterator(currentIt.previousIndex()) : list.listIterator(0);
        if (markIt.hasNext()) {
            markIt.next();
        }
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        int index = markIt.previousIndex();
        currentIt = index >= 0 ? list.listIterator(markIt.previousIndex()) : list.listIterator(0);
        if (currentIt.hasNext()) {
            currentIt.next();
        }
    }

    @Override
    public void close() throws IOException {
        if (isOpen) {
            isOpen = false;
        }
    }

    public String documentToString() throws TransformerException {
        return ctx.jsonString();
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw new IllegalStateException("Serializer has been closed");
        }
    }

    public static class Builder implements EventDeserializer.Builder {

        @Override
        public EventDeserializer build(Context context, ResettableInputStream in) {
            if (!(in instanceof Seekable)) {
                throw new IllegalArgumentException(
                        "Cannot use this deserializer without a Seekable input stream");
            }
            try {
                return new JsonPathDeserializer(context, in);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
