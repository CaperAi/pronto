package caper.pronto.utils;

import com.mongodb.MongoClient;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import caper.pronto.ProtoCodecProvider;

/**
 * Created by native on 8/4/20.
 */

public class CodecUtils {

    public static CodecRegistry getRegistry() {
        final CodecRegistry registries = CodecRegistries.fromProviders(
                new ProtoCodecProvider(),
                PojoCodecProvider.builder().automatic(true).build()
        );

        return CodecRegistries.fromRegistries(
                registries,
                MongoClient.getDefaultCodecRegistry()
        );
    }
}
