package caper.pronto.encoding;

import com.google.api.gax.protobuf.ProtoReflectionUtil;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Map;

public class DefaultEncoder extends Encoder {
    private final Codec<Message> codec;

    // TODO(josh): Manually recurse through the proto rather than relying on the CodecRegistry. This will allow us to
    //             more easily support other storage backends AND fix the Nested IDs Must Be ObjectIDs issue.

    public DefaultEncoder(FieldDescriptor field, CodecRegistry registry) {
        super(field);
        final Descriptor descriptor = field.getMessageType();
        final Message defaultInstance = ProtoReflectionUtil.getDefaultInstance(descriptor);
        codec = (Codec<Message>) registry.get(defaultInstance.getClass());
    }

    @Override
    public void encode(BsonWriter writer, Object value) {
        codec.encode(writer, (Message) value, EncoderContext.builder().build());
    }

    @Override
    public Object decode(BsonReader reader) {
        final Message decode = codec.decode(reader, DecoderContext.builder().build());
        return decode;
    }

    @Override
    public boolean isEmpty(Object value) {
        final Message m = (Message) value;
        for (Map.Entry<FieldDescriptor, Object> field : m.getAllFields().entrySet()) {
            FieldDescriptor descriptor = field.getKey();

            if (descriptor.isRepeated()) {
                return m.getRepeatedFieldCount(descriptor) == 0;
            } else {
                if (m.hasField(field.getKey())) {
                    return false;
                }
            }
        }
        return true;
    }
}
