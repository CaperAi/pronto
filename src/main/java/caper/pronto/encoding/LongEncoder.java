package caper.pronto.encoding;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.bson.BsonReader;
import org.bson.BsonWriter;

public class LongEncoder extends Encoder {
    public LongEncoder(Descriptors.FieldDescriptor field) {
        super(field);
    }

    @Override
    public void encode(BsonWriter writer, Object value) {
        writer.writeInt64((long) value);
    }

    @Override
    public Object decode(BsonReader reader) {
        return reader.readInt64();
    }

    @Override
    public boolean isEmpty(Object value) {
        return ((long) value) == 0;
    }
}
