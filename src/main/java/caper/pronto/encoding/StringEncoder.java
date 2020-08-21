package caper.pronto.encoding;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.bson.BsonReader;
import org.bson.BsonWriter;

public class StringEncoder extends Encoder {
    public StringEncoder(Descriptors.FieldDescriptor field) {
        super(field);
    }

    @Override
    public void encode(BsonWriter writer, Object value) {
        writer.writeString((String) value);
    }

    @Override
    public Object decode(BsonReader reader) {
        return reader.readString();
    }

    @Override
    public boolean isEmpty(Object value) {
        return ((String) value).isEmpty();
    }
}
