package caper.pronto.encoding;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;

import org.bson.BsonBinary;
import org.bson.BsonReader;
import org.bson.BsonWriter;

public class BinaryEncoder extends Encoder {
    public BinaryEncoder(Descriptors.FieldDescriptor field) {
        super(field);
    }

    @Override
    public void encode(BsonWriter writer, Object value) {
        final ByteString bytes = (ByteString) value;
        writer.writeBinaryData(new BsonBinary(bytes.toByteArray()));
    }

    @Override
    public Object decode(BsonReader reader) {
        final BsonBinary value = reader.readBinaryData();
        return ByteString.copyFrom(value.getData());
    }

    @Override
    public boolean isEmpty(Object value) {
        return ((ByteString) value).isEmpty();
    }
}
