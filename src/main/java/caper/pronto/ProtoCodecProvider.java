package caper.pronto;

import com.google.api.gax.protobuf.ProtoReflectionUtil;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

public class ProtoCodecProvider implements CodecProvider {
    @Override
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        try {
            final Message instance = ProtoReflectionUtil.getDefaultInstance(clazz.asSubclass(Message.class));
            final Descriptors.Descriptor descriptor = instance.getDescriptorForType();
            return new ProtoCodec<T>(registry, clazz, instance, descriptor);
        } catch (ClassCastException e) {
            return null;
        }
    }
}