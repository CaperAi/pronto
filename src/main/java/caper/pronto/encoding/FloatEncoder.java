package caper.pronto.encoding;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.bson.BsonReader;
import org.bson.BsonWriter;

public class FloatEncoder extends Encoder {
    public FloatEncoder(Descriptors.FieldDescriptor field) {
        super(field);
    }

    @Override
    public void encode(BsonWriter writer, Object value) {
        // This has to be here for casting magic.
        final float f = (float) value;
        writer.writeDouble(f);
    }

    @Override
    public Object decode(BsonReader reader) {
        // This has to be here for casting magic.
        final double d = reader.readDouble();
        final float f = (float) d;
        return f;
    }

    @Override
    public boolean isEmpty(Object value) {
        return ((float) value) == 0;
    }
}
