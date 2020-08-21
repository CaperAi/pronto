package caper.pronto.encoding;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;

import java.util.ArrayList;
import java.util.List;

public class RepeatedEncoder extends Encoder {
    private final Encoder internal;

    public RepeatedEncoder(Descriptors.FieldDescriptor field, Encoder internal) {
        super(field);
        this.internal = internal;
    }

    @Override
    public void encode(BsonWriter writer, Object value) {
        writer.writeStartArray();
        for (Object item : (List<Object>) value) {
            this.internal.encode(writer, item);
        }
        writer.writeEndArray();
    }

    private List<Object> decodeArray(BsonReader reader) {
        List<Object> values = new ArrayList<>();
        reader.readStartArray();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            values.add(this.internal.decode(reader));
        }
        reader.readEndArray();
        return values;
    }


    private List<Object> decodeSingle(BsonReader reader) {
        List<Object> values = new ArrayList<>();
        values.add(this.internal.decode(reader));
        return values;
    }

    @Override
    public Object decode(BsonReader reader) {
        if (reader.getCurrentBsonType() == BsonType.ARRAY) {
            return decodeArray(reader);
        } else {
            return decodeSingle(reader);
        }
    }

    @Override
    public boolean isEmpty(Object value) {
        return ((List<?>) value).isEmpty();
    }

    public <T> T value(int index, Message.Builder builder) {
        return (T) builder.getRepeatedField(getField(), index);
    }
}
