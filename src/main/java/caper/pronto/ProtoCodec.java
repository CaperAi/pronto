package caper.pronto;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.HashMap;
import java.util.Map;

import caper.pronto.encoding.Encoder;
import caper.pronto.encoding.EncodingProvider;

import static org.bson.BsonType.END_OF_DOCUMENT;

public class ProtoCodec<T> implements Codec<T> {
    private final Class<T> type;
    private final Message defaultInstance;
    private final Map<String, Encoder> fields = new HashMap<>();

    public ProtoCodec(CodecRegistry registry, Class<T> type, Message defaultInstance, Descriptor descriptor) {
        this.type = type;
        this.defaultInstance = defaultInstance;

        // Group field names to field descriptors
        for (FieldDescriptor field : descriptor.getFields()) {
            final Encoder encoder = EncodingProvider.getEncoderForField(field, registry);
            fields.put(encoder.name(), encoder);
        }
    }

    @Override
    public T decode(BsonReader reader, DecoderContext decoderContext) {
        final Message.Builder builder = defaultInstance.newBuilderForType();
        reader.readStartDocument();
        while (reader.readBsonType() != END_OF_DOCUMENT) {
            final String name = reader.readName();
            final Encoder encoder = fields.get(name);

            if (encoder == null) {
                reader.skipValue();
                continue;
            }

            final Object value = encoder.decode(reader);

            if (encoder.isEmpty(value)) {
                continue;
            }

            builder.setField(encoder.getField(), value);
        }
        reader.readEndDocument();

        return (T) builder.build();
    }

    public void encode(BsonWriter writer, Message message, EncoderContext encoderContext) {
        final Message.Builder builder = message.toBuilder();
        writer.writeStartDocument();
        for (final String name : fields.keySet()) {
            final Encoder encoder = fields.get(name);
            if (encoder.wants(builder)) {
                writer.writeName(encoder.name());
                encoder.encode(writer, builder.getField(encoder.getField()));
            }
        }
        writer.writeEndDocument();
    }

    @Override
    public void encode(BsonWriter writer, T value, EncoderContext encoderContext) {
        encode(writer, (Message) value, encoderContext);
    }

    @Override
    public Class<T> getEncoderClass() {
        return type;
    }
}
