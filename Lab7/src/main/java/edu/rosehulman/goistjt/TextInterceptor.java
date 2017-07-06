package edu.rosehulman.goistjt;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

public class TextInterceptor implements Interceptor {
    private String hostname;

    public TextInterceptor() {

    }

    public void initialize() {
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new FlumeException("Cannot get Hostname", e);
        }
    }

    public Event intercept(Event event) {
        try {
            String body = new String(event.getBody(), "UTF-8");
            if (body.contains("call returned (0, '')") || body.contains("Heartbeat response received") || body.contains("No commands sent from the Server")) {
                return null;
            }
            return EventBuilder.withBody(hostname + " " + body, Charset.forName("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

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

    public void close() {
    }

    public static class Builder implements Interceptor.Builder {
        public Interceptor build() {
            return new TextInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
