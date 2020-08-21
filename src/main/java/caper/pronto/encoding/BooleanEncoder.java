package caper.pronto.encoding;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.bson.BsonReader;
import org.bson.BsonWriter;

public class BooleanEncoder extends Encoder {
    public BooleanEncoder(Descriptors.FieldDescriptor field) {
        super(field);
    }

    @Override
    public void encode(BsonWriter writer, Object value) {
        writer.writeBoolean((boolean) value);
    }

    @Override
    public Object decode(BsonReader reader) {
        return reader.readBoolean();
    }

    @Override
    public boolean isEmpty(Object value) {
        return ((Boolean) value).booleanValue() == false;
    }
}
