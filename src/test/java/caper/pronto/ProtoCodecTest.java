package caper.pronto;


import caper.pronto.Repeated.OldRepeatingParent;
import caper.pronto.Repeated.RepeatingParent;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.util.Timestamps;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.UpdateResult;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

import caper.pronto.utils.CodecUtils;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProtoCodecTest extends TestBase {

    @Test
    public void oneOf() {
        final MongoCollection<OneOfMessage> collection = collection("oneof", "oneof", OneOfMessage.class);

        collection.insertMany(asList(
                OneOfMessage.newBuilder()
                        .setA("A")
                        .build(),
                OneOfMessage.newBuilder()
                        .setB("B")
                        .build(),
                OneOfMessage.newBuilder()
                        .setC("C")
                        .build(),
                OneOfMessage.newBuilder()
                        .setComplex(Complex.newBuilder()
                                .setNested(NestedComplex.newBuilder()
                                        .setA(10)
                                )
                        )
                        .build()
        ));

        final BsonDocument filterA = new BsonDocument()
                .append("a", new BsonString("A"));
        final BsonDocument filterB = new BsonDocument()
                .append("b", new BsonString("B"));
        final BsonDocument filterC = new BsonDocument()
                .append("c", new BsonString("C"));
        final BsonDocument filterComplex = new BsonDocument()
                .append("complex.nested.a", new BsonInt32(10));

        OneOfMessage a = collection.find(OneOfMessage.class)
                .filter(filterA)
                .limit(1)
                .first();
        OneOfMessage b = collection.find(OneOfMessage.class)
                .filter(filterB)
                .limit(1)
                .first();
        OneOfMessage c = collection.find(OneOfMessage.class)
                .filter(filterC)
                .limit(1)
                .first();
        OneOfMessage complex = collection.find()
                .filter(filterComplex)
                .limit(1)
                .first();

        assertEquals(OneOfMessage.SomethingCase.A, a.getSomethingCase());
        assertEquals(OneOfMessage.SomethingCase.B, b.getSomethingCase());
        assertEquals(OneOfMessage.SomethingCase.C, c.getSomethingCase());
        assertEquals(OneOfMessage.SomethingCase.COMPLEX, complex.getSomethingCase());
    }

    @Test
    public void repeatingMigrationProto() {
        final MongoCollection<OldRepeatingParent> collection = collection("repeated", "repeating_proto", OldRepeatingParent.class);

        collection.insertOne(
                OldRepeatingParent.newBuilder()
                        .addChildren(Repeated.RepeatingChild.newBuilder()
                                .setChildId("goodbyworld")
                                .build())
                        .addChildren(Repeated.RepeatingChild.newBuilder()
                                .setChildId("something")
                                .build())
                        .setPrimitive("value")
                        .setNotRepeatedChild(Repeated.RepeatingChild.newBuilder()
                                .setChildId("notrepeating")
                                .build())
                        .build()
        );

        final MongoCollection<RepeatingParent> upgraded = collection("repeated", "repeating_proto", RepeatingParent.class);

        RepeatingParent stored = upgraded.find().first();
        assertEquals(1, stored.getPrimitiveCount());
        assertEquals("value", stored.getPrimitive(0));
    }

    @Test
    public void repeatingProto() {
        final MongoCollection<RepeatingParent> collection = collection("repeated", "repeating_proto", RepeatingParent.class);

        collection.insertOne(
                RepeatingParent.newBuilder()
                        .build()
        );

        RepeatingParent stored = collection.find().first();

        assertEquals(0, stored.getChildrenCount());
        assertEquals(0, stored.getPrimitiveCount());
        assertEquals("", stored.getNotRepeatedChild().getChildId());
    }

    @Test
    public void repeatingOneofProto() {
        final MongoCollection<OldRepeatingParent> collection = collection("repeated", "repeating_proto", OldRepeatingParent.class);

        collection.insertMany(asList(
                OldRepeatingParent.newBuilder()
                        .setPrimitive("A")
                        .addChildren(Repeated.RepeatingChild.newBuilder()
                                .setName("A")
                        )
                        .build(),
                OldRepeatingParent.newBuilder()
                        .setPrimitive("B")
                        .addChildren(Repeated.RepeatingChild.newBuilder()
                                .setPreferredName("B")
                        )
                        .build()
        ));

        OldRepeatingParent a = collection.find()
                .filter(new BsonDocument()
                        .append("primitive", new BsonString("A")))
                .first();

        OldRepeatingParent b = collection.find()
                .filter(new BsonDocument()
                        .append("primitive", new BsonString("B")))
                .first();

        assertEquals(Repeated.RepeatingChild.ValueCase.NAME, a.getChildren(0).getValueCase());
        assertEquals(Repeated.RepeatingChild.ValueCase.PREFERRED_NAME, b.getChildren(0).getValueCase());
    }

    @Test
    public void emptyRepeatingProto() {
        final MongoCollection<RepeatingParent> collection = collection("repeated", "repeating_proto", RepeatingParent.class);

        collection.insertOne(
                RepeatingParent.newBuilder()
                        .addChildren(Repeated.RepeatingChild.newBuilder()
                                .setChildId("goodbyworld")
                                .build())
                        .addChildren(Repeated.RepeatingChild.newBuilder()
                                .setChildId("something")
                                .build())
                        .addPrimitive("foobar")
                        .addPrimitive("Another")
                        .addPrimitive("a third")
                        .setNotRepeatedChild(Repeated.RepeatingChild.newBuilder()
                                .setChildId("notrepeating")
                                .build())
                        .build()
        );

        RepeatingParent stored = collection.find().first();

        assertEquals(2, stored.getChildrenCount());
        assertEquals(3, stored.getPrimitiveCount());
        assertEquals("notrepeating", stored.getNotRepeatedChild().getChildId());
    }

    @Test
    public void nestedProto() {
        final MongoCollection<Nested.Parent> collection = collection("nested", "nested_proto", Nested.Parent.class);
        final ReplaceOptions policy = new ReplaceOptions().upsert(true);


        collection.insertMany(asList(
                Nested.Parent.newBuilder()
                        .setOur(Nested.Child.newBuilder()
                                .setChildId("goodby"))
                        .setExternal(Timestamps.fromSeconds(20))
                        .build(),
                Nested.Parent.newBuilder()
                        .setOur(Nested.Child.newBuilder()
                                .setChildId("hello"))
                        .setExternal(Timestamps.fromSeconds(30))
                        .build()
        ));

        UpdateResult result = collection.replaceOne(new BsonDocument()
                .append("our.child_id", new BsonString("foobar")), Nested.Parent.newBuilder()
                .setOur(Nested.Child.newBuilder()
                        .setChildId("foobar"))
                .setExternal(Timestamps.fromSeconds(10))
                .build(), policy);
        final ObjectId id = result.getUpsertedId().asObjectId().getValue();

        assertEquals(3, collection.countDocuments());

        Nested.Parent stored = collection.find(new BsonDocument()
                .append("_id", new BsonObjectId(id)))
                .first();

        assertEquals(10, stored.getExternal().getSeconds());
        assertEquals("foobar", stored.getOur().getChildId());
        assertEquals(id.toHexString(), stored.getId());
    }

    @Test
    public void enumsBasicProto() {
        final MongoCollection<BasicMessage> collection = collection("basic_messages", "enums_basic_proto", BasicMessage.class);

        collection.insertMany(asList(
                BasicMessage.newBuilder()
                        .setBasicEnum(BasicEnum.A)
                        .build(),
                BasicMessage.newBuilder()
                        .setBasicEnum(BasicEnum.B)
                        .build()
        ));


        final BsonDocument filterA = new BsonDocument()
                .append("basic_enum", new BsonInt32(BasicEnum.A_VALUE));
        BasicMessage hopefullyA = collection.find(filterA, BasicMessage.class)
                .first();
        assertEquals(BasicEnum.A, hopefullyA.getBasicEnum());
        assertFalse("A's ID was set", hopefullyA.getId().isEmpty());

        final BsonDocument filterB = new BsonDocument()
                .append("basic_enum", new BsonInt32(BasicEnum.B_VALUE));
        BasicMessage hopefullyB = collection.find(BasicMessage.class)
                .filter(filterB)
                .limit(1)
                .first();
        assertEquals(BasicEnum.B, hopefullyB.getBasicEnum());
        assertFalse("B's ID was set", hopefullyB.getId().isEmpty());
    }


    @Test
    public void updateBasicProto() {
        final MongoCollection<BasicMessage> collection = collection("basic_messages", "update_basic_proto", BasicMessage.class);
        final ReplaceOptions policy = new ReplaceOptions().upsert(true);

        final BsonDocument filter = new BsonDocument()
                .append("basic_bool", new BsonBoolean(false));

        final UpdateResult result = collection
                .replaceOne(filter, BasicMessage.newBuilder().build(), policy);


        assertNotNull(result);
        assertTrue(result.wasAcknowledged());

        assertTrue(result.getUpsertedId().asObjectId().getValue().toHexString().length() > 0);
    }

    @Test
    public void persistBytes() {
        // This does work but from bloomrpc it appears this does not work. There is an existing
        // issue open on BloomRPC: https://github.com/uw-labs/bloomrpc/issues/101
        final byte[] data = new byte[]{
                1, 2, 3, 4
        };

        final BasicMessage message = BasicMessage.newBuilder()
                .setBasicBytes(ByteString.copyFrom(data))
                .build();
        final MongoCollection<BasicMessage> collection = collection("basic_messages", "bytes_basic_proto", BasicMessage.class);

        collection.insertOne(message);

        final byte[] read = collection.find(BasicMessage.class)
                .limit(1)
                .first()
                .getBasicBytes()
                .toByteArray();

        assertArrayEquals(data, read);
    }

    @Test
    public void persistBasicProto() {
        final BasicMessage message = BasicMessage.newBuilder()
                .setBasicString("Hello world")
                .setBasicBool(true)
                .setBasicBytes(ByteString.copyFromUtf8("Hello World"))
                .setBasicDouble(0.4d)
                .setBasicFloat(0.4f)
                .setBasicEnum(BasicEnum.A)
                .setBasicFixed32(1)
                .setBasicFixed64(2)
                .setBasicInt32(1)
                .setBasicInt64(2)
                .setBasicSignedInt32(1)
                .setBasicSignedInt64(2)
                .setBasicString("Hello world")
                .setBasicUnsignedFixed32(1)
                .setBasicUnsignedFixed64(2)
                .setBasicUnsignedInt32(1)
                .setBasicUnsignedInt64(3)
                .build();
        final MongoCollection<BasicMessage> collection = collection("basic_messages", "persist_basic_proto", BasicMessage.class);

        collection.insertOne(BasicMessage.newBuilder()
                .setBasicString("goodby world")
                .build());
        collection.insertOne(message);

        final BasicMessage read = collection.find(BasicMessage.class)
                .filter(new BsonDocument()
                        .append("basic_string", new BsonString("Hello world")))
                .limit(1)
                .first();
        assertNotNull(read);
        assertEquals("Hello world", read.getBasicString());

        for (final FieldDescriptor descriptor : message.getDescriptorForType().getFields()) {
            if (message.hasField(descriptor)) {
                assertEquals(message.getField(descriptor), read.getField(descriptor));
            }
        }
    }
}