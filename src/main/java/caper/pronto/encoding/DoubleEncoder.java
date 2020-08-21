package caper.pronto.encoding;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.bson.BsonReader;
import org.bson.BsonWriter;

public class DoubleEncoder extends Encoder {
    public DoubleEncoder(Descriptors.FieldDescriptor field) {
        super(field);
    }

    @Override
    public void encode(BsonWriter writer, Object value) {
        writer.writeDouble((double) value);
    }

    @Override
    public Object decode(BsonReader reader) {
        return reader.readDouble();
    }

    @Override
    public boolean isEmpty(Object value) {
        return ((double) value) == 0;
    }
}
