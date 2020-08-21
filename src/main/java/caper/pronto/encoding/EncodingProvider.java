package caper.pronto.encoding;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Timestamp;

import org.bson.codecs.configuration.CodecRegistry;

public class EncodingProvider {
    private static Encoder getEncoderForMessageField(final FieldDescriptor field, final CodecRegistry registry) {
        final Descriptor descriptor = field.getMessageType();
        final String typeName = descriptor.getFullName();

        // Custom encoders for Mongo times. This allows other BI tools to understand the values we
        // are storing rather than having to only pull through our tooling.
        if (typeName.equals(Timestamp.getDescriptor().getFullName())) {
            return new TimestampEncoder(field);

        }

        return new DefaultEncoder(field, registry);
    }

    public static Encoder getEncoderForField(final FieldDescriptor field, final CodecRegistry registry) {
        Encoder result = null;

        if (field.getName().equals("id")) {
            result = new IdEncoder(field);
        } else {
            switch (field.getJavaType()) {
                case INT:
                    result = new IntEncoder(field);
                    break;
                case LONG:
                    result = new LongEncoder(field);
                    break;
                case BOOLEAN:
                    result = new BooleanEncoder(field);
                    break;
                case STRING:
                    result = new StringEncoder(field);
                    break;
                case FLOAT:
                    result = new FloatEncoder(field);
                    break;
                case DOUBLE:
                    result = new DoubleEncoder(field);
                    break;
                case ENUM:
                    result = new EnumEncoder(field);
                    break;
                case BYTE_STRING:
                    result = new BinaryEncoder(field);
                    break;
                case MESSAGE:
                    result = getEncoderForMessageField(field, registry);
                    break;
            }
        }

        if (field.isRepeated()) {
            result = new RepeatedEncoder(field, result);
        }

        return result;
    }
}
