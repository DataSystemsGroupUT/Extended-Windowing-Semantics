package org.apache.beam.examples;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

@DefaultCoder(AvroCoder.class)
public class Event {
    @Nullable String user;
    @Nullable String team;
    @Nullable Integer score;
    @Nullable Instant timestamp;

    public Event() {}

    public Event(String user, String team, Integer score, Instant timestamp) {
        this.user = user;
        this.team = team;
        this.score = score;
        this.timestamp = timestamp;
    }

    public String getUser() {
        return this.user;
    }

    public String getTeam() {
        return this.team;
    }

    public Integer getScore() {
        return this.score;
    }

    public String getKey(String keyname) {
        if ("team".equals(keyname)) {
            return this.team;
        } else { // return username as default
            return this.user;
        }
    }

    public Instant getTimestamp() {
        return this.timestamp;
    }
}