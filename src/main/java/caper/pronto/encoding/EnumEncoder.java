package caper.pronto.encoding;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Message;

import org.bson.BsonReader;
import org.bson.BsonWriter;

public class EnumEncoder extends Encoder {
    private final EnumDescriptor descriptor;

    public EnumEncoder(Descriptors.FieldDescriptor field) {
        super(field);
        descriptor = getField().getEnumType();
    }


    @Override
    public void encode(BsonWriter writer, Object value) {
        EnumValueDescriptor corrected = (EnumValueDescriptor) value;
        writer.writeInt32(corrected.getNumber());
    }

    @Override
    public Object decode(BsonReader reader) {
        return descriptor.findValueByNumber(reader.readInt32());
    }

    @Override
    public boolean isEmpty(Object value) {
        return ((EnumValueDescriptor) value).getIndex() == 0;
    }
}
