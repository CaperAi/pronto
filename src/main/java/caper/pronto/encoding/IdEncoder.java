package caper.pronto.encoding;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.types.ObjectId;

public class IdEncoder extends Encoder {
    public IdEncoder(Descriptors.FieldDescriptor field) {
        super(field);
    }

    @Override
    public void encode(BsonWriter writer, Object value) {
        writer.writeObjectId(new ObjectId((String) value));
    }

    @Override
    public Object decode(BsonReader reader) {
        ObjectId value = reader.readObjectId();
        return value.toHexString();
    }

    @Override
    public String name() {
        return "_id";
    }

    @Override
    public boolean wants(Message.Builder builder) {
        final String value = value(builder);
        return !value.isEmpty();
    }

    @Override
    public boolean isEmpty(Object value) {
        return ((String) value).isEmpty();
    }
}
