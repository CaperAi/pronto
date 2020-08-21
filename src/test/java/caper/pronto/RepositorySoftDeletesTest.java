package caper.pronto;

import com.google.protobuf.Timestamp;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.sun.org.apache.xml.internal.utils.res.XResourceBundle;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.util.List;

import caper.pronto.utils.CodecUtils;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

import static com.google.common.collect.ImmutableList.copyOf;
import static org.junit.Assert.assertEquals;

public class RepositorySoftDeletesTest extends TestBase {
    @Test
    public void softDeletion() {
        final MongoCollection<SoftDelete> collection = collection("sd", "sd", SoftDelete.class);
        final MongoRepository<SoftDelete> repository = repository(collection);
        SoftDelete result = repository.set(SoftDelete.newBuilder()
                .setWord("hello")
                .build());
        assertEquals(1, collection.countDocuments());


        repository.delete(result.getId());
        assertEquals(1, collection.countDocuments());

        List<SoftDelete> words = copyOf(repository.list());
        assertEquals(0, words.size());
    }

    @Test
    public void listingDeletion() {
        final MongoCollection<SoftDelete> collection = collection("sd", "sd", SoftDelete.class);
        final MongoRepository<SoftDelete> repository = repository(collection);
        SoftDelete result = repository.set(SoftDelete.newBuilder()
                .setWord("hello")
                .build());
        assertEquals(1, collection.countDocuments());
        repository.delete(result.getId());
        List<SoftDelete> words = copyOf(repository.listWithDeletes());
        assertEquals(1, words.size());
    }


    @Test
    public void listingDeletionsAndNonDeletions() {
        final MongoCollection<SoftDelete> collection = collection("sd", "sd", SoftDelete.class);
        final MongoRepository<SoftDelete> repository = repository(collection);
        SoftDelete result = repository.set(SoftDelete.newBuilder()
                .setWord("hello")
                .build());
        repository.set(SoftDelete.newBuilder()
                .setWord("world")
                .build());

        repository.delete(result.getId());

        List<SoftDelete> words = copyOf(repository.listWithDeletes());
        assertEquals(2, words.size());
    }

    @Test
    public void listingSinceWithFilters() {
        final MongoCollection<SoftDelete> collection = collection("sd", "sd", SoftDelete.class);
        final MongoRepository<SoftDelete> repository = repository(collection);
        Timestamp now = TimestampUtil.fromInstant(Instant.now(Clock.systemUTC()));

        repository.set(SoftDelete.newBuilder()
                .setWord("hello")
                .build());

        repository.set(SoftDelete.newBuilder()
                .setDeleted(true)
                .setWord("goodby")
                .build());

        List<SoftDelete> words = copyOf(repository.list(now));
        assertEquals(1, words.size());

        List<SoftDelete> wordsWithDeletes = copyOf(repository.listWithDeletes(now));
        assertEquals(2, wordsWithDeletes.size());
    }

}
