package com.gautam.mantra.spark;


import java.sql.Timestamp;

public class Event {
    String eventId;
    Timestamp eventTs;


    /** Constructor
     * @param s Event id
     * @param timestamp timestamp
     */
    public Event(String s, java.sql.Timestamp timestamp) {
        this.setEventId(s);
        this.setEventTs(timestamp);
    }

    /**
     * default constructor
     */
    public Event() {

    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Timestamp getEventTs() {
        return eventTs;
    }

    public void setEventTs(Timestamp eventTs) {
        this.eventTs = eventTs;
    }

    @Override
    public String toString() {
        return "Events{" +
                "eventId='" + eventId + '\'' +
                ", eventTs=" + eventTs +
                '}';
    }
}
