# Proto + Mongo == Pronto

Pronto is a MongoDB Codec Provider that serializes protos to and from bson documents to be saved
and loaded from MongoDB. This library will allow you to avoid writing code that manually transforms
a proto into a pojo or BsonDocument and then inserting that into Mongo.

# Hiring

Caper is always looking for talented software engineers. We're currently looking for Backend and
Android Frontend engineers. If you want to talk Caper please feel free to shoot an email to any
of the authors of this library. You can find our emails in the commit log.

# License

All code from `src/main/java/caper/...` and `src/test/...` are covered by `LICENSE.md`. The code within
`src/main/java/com/google/api/gax/protobuf/` and `src/main/resources/licenses/GAX_PROTOBUF_LICENSE`
are licensed under the terms within `src/main/resources/licenses/GAX_PROTOBUF_LICENSE`. This license
file is also packed into the jar under `/licenses/GAX_PROTOBUF_LICENSE`, if you are packaging this
for distribution please remember to include this file in your build.

# TODO

1. Create a QueryBuilder?
2. Annotations for choosing/altering field names?
3. Automation of proto migrations?
4. Support other data storage backends?

# Usage

// TODO(josh): Publish to maven registry or document jitpack?

Now you will be able to register the `ProtoCodecProvider` in your MongoDB client. It will look
something like this:

```java
import com.mongodb.MongoClient;
import org.bson.codecs.configuration.CodecRegistry;
import com.mongodb.client.MongoCollection;

class Example {
    private final static CodecRegistry registry = fromRegistries(
        fromProviders(
            // Create a provider for the codec.
            new ProtoCodecProvider(),
            PojoCodecProvider.builder().automatic(true).build()
        )
    );

    private MongoDatabase database(MongoClient client, String name) {
        return client.getDatabase(name)
                // Register it as the codec for your client.
                .withCodecRegistry(registry)
                .withWriteConcern(WriteConcern.ACKNOWLEDGED);
    }
}
```

You can now insert protos using the following APIs:

```java
import com.mongodb.client.MongoCollection;
class Repository {
    private final MongoCollection<BasicMessage> collection = database("basic_messages")
        .getCollection("enums_basic_proto", BasicMessage.class);

    public void saveThings() {
        collection.insertMany(Arrays.asList(
            BasicMessage.newBuilder()
                .setBasicEnum(BasicEnum.A)
                .build(),
            BasicMessage.newBuilder()
                .setBasicEnum(BasicEnum.B)
                .build()
        ));
    }
}
```

# Usage (caper internal)

The only thing that is different between our public and private
release of pronto is how you include it into our build chain. 
Just add the following to your `build.gradle`:

```groovy
dependencies {
  include project(':pronto')
}
```
