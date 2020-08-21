package caper.pronto;

import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.Date;

/**
 * TODO: Move into separate library
 */
public class TimestampUtil {
    public static boolean isTimestampEmpty(Timestamp timestamp) {
        return timestamp.getSeconds() == 0 && timestamp.getNanos() == 0;
    }

    public static Timestamp fromInstant(Instant instant) {
        if (instant == null) {
            return null;
        }
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    public static Instant toInstant(Timestamp timestamp) {
        if (timestamp == null) {
            return null;
        }
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    public static Date toDate(Timestamp created) {
        Instant instant = toInstant(created);
        if (instant == null) {
            return null;
        }
        return Date.from(instant);
    }
}