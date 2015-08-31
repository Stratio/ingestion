package com.stratio.ingestion.deserializer.jsonxpath;

import com.stratio.ingestion.serialization.tracker.TransientPositionTracker;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.apache.flume.serialization.ResettableInputStream;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class JsonPathDeserializerTest {

    private ResettableInputStream getTestInputStream() throws Exception {
        return getTestInputStream("books.json");
    }

    private ResettableInputStream getTestInputStream(final String path) throws Exception {
        File f = new File(this.getClass().getClassLoader().getResource(path).toURI());
        return new ResettableFileInputStream(f, new TransientPositionTracker("dummy"));
    }

    @Test
    public void testReadsAndMark() throws Exception {
        Context context = new Context();
        context.put("expression", "$.store.book[*]");
        EventDeserializer des = new JsonPathDeserializer.Builder().build(context, getTestInputStream());
        validateReadAndMark(des);
    }

    @Test
    public void testReset() throws Exception {
        Context context = new Context();
        context.put("expression", "$.store.book[*].title");
        EventDeserializer des = new JsonPathDeserializer.Builder().build(context, getTestInputStream());
        validateReset(des);
    }

    @Test
    public void testHeader() throws Exception {
        Context context = new Context();
        context.put("expression", "$.store.book[*]");
        context.put("outputHeader", "myHeader");
        context.put("outputBody", "false");
        EventDeserializer des = new JsonPathDeserializer.Builder().build(context, getTestInputStream());
        validateReadAndMarkWithHeader(des);
    }

    @Test(expected = RuntimeException.class)
    public void testBadJSON() throws Exception {
        new JsonPathDeserializer.Builder().build(new Context(), getTestInputStream("bad.json"));
    }

    private void validateReset(EventDeserializer des) throws IOException {
        Event evt = des.readEvent();
        assertEquals("Sayings of the Century", new String(evt.getBody()));
        des.mark();

        List<Event> events = des.readEvents(3);
        assertEquals(3, events.size());
        assertEquals("Sword of Honour", new String(events.get(0).getBody()));
        assertEquals("Moby Dick", new String(events.get(1).getBody()));
        assertEquals("The Lord of the Rings", new String(events.get(2).getBody()));

        des.reset(); // reset!

        events = des.readEvents(3);
        assertEquals(3, events.size());
        assertEquals("Sword of Honour", new String(events.get(0).getBody()));
        assertEquals("Moby Dick", new String(events.get(1).getBody()));
        assertEquals("The Lord of the Rings", new String(events.get(2).getBody()));

        evt = des.readEvent();
        Assert.assertNull("Event should be null because there are no more books " + "left to read", evt);

    }

    private void validateHeaders(EventDeserializer des) throws IOException {
        List<Event> events = des.readEvents(4);
        Assert.assertTrue(events.size() == 4);

        for (Event evt : events) {
            Assert.assertEquals(evt.getHeaders().get("author"), "J K. Rowling");
        }
    }

    private void validateReadAndMark(EventDeserializer des) throws IOException {
        Event evt;

        evt = des.readEvent();
        assertTrue(new String(evt.getBody()).contains("Nigel Rees"));
        des.mark();

        evt = des.readEvent();
        assertTrue(new String(evt.getBody()).contains("Evelyn Waugh"));
        des.mark(); // reset!

        List<Event> readEvents = des.readEvents(2);
        assertEquals(2, readEvents.size());

        evt = des.readEvent();
        assertNull("Event should be null because there are no more books " + "left to read", evt);

        des.mark();
        des.mark();
        des.close();
    }

    private void validateReadAndMarkWithHeader(EventDeserializer des) throws IOException {
        Event evt;

        evt = des.readEvent();
        System.out.println(evt.getHeaders().get("myHeader"));
        assertTrue(evt.getHeaders().get("myHeader").contains("Nigel Rees"));
        des.mark();

        evt = des.readEvent();
        assertTrue(evt.getHeaders().get("myHeader").contains("Evelyn Waugh"));
        des.mark(); // reset!

        List<Event> readEvents = des.readEvents(2);
        assertEquals(2, readEvents.size());

        evt = des.readEvent();
        assertNull("Event should be null because there are no more books " + "left to read", evt);

        des.mark();
        des.mark();
        des.close();
    }


}
