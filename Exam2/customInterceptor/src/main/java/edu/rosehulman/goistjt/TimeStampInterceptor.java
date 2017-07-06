package edu.rosehulman.goistjt;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

public class TimeStampInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        LocalDateTime localDateTime = LocalDateTime.now();
        try {
            String body = new String(event.getBody(), "UTF-8");
            return EventBuilder.withBody("Parse Time: " + localDateTime.toString() + "\t" + body, Charset.forName("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> eventList = new LinkedList<>();
        list.forEach(event -> {
            Event out = intercept(event);
            if (out != null) {
                eventList.add(out);
            }
        });
        return eventList;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
