package caper.pronto.encoding;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.bson.BsonReader;
import org.bson.BsonWriter;

public class IntEncoder extends Encoder {
    public IntEncoder(Descriptors.FieldDescriptor field) {
        super(field);
    }

    @Override
    public void encode(BsonWriter writer, Object value) {
        writer.writeInt32((int) value);
    }

    @Override
    public Object decode(BsonReader reader) {
        return reader.readInt32();
    }

    @Override
    public boolean isEmpty(Object value) {
        return ((int) value) == 0;
    }
}
