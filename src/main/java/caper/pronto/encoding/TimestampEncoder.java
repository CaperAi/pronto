package caper.pronto.encoding;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;

import org.bson.BsonReader;
import org.bson.BsonWriter;

import java.time.Instant;

import caper.pronto.TimestampUtil;

public class TimestampEncoder extends Encoder {
    public TimestampEncoder(Descriptors.FieldDescriptor field) {
        super(field);

        if (!field.getMessageType().getFullName().equals(Timestamp.getDescriptor().getFullName())) {
            throw new RuntimeException("Must provide a Timestamp into a TimestampEncoder");
        }
    }

    @Override
    public void encode(BsonWriter writer, Object value) {
        final Timestamp timestamp = (Timestamp) value;
        final Instant instant = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
        writer.writeDateTime(instant.toEpochMilli());
    }

    @Override
    public Object decode(BsonReader reader) {
        final long value = reader.readDateTime();
        return fromMongo(value);
    }

    @Override
    public boolean isEmpty(Object value) {
        return TimestampUtil.isTimestampEmpty(((Timestamp) value));
    }

    public static long toMongo(Timestamp timestamp) {
        final Instant instant = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
        return instant.toEpochMilli();
    }

    public static Timestamp fromMongo(long value) {
        final Instant instant = Instant.ofEpochMilli(value);
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
