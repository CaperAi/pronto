package caper.pronto;

import com.google.protobuf.Timestamp;
import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

import static com.google.common.collect.ImmutableList.copyOf;
import static org.junit.Assert.assertEquals;

public class MongoRepositoryTest extends TestBase {
    @Test
    public void insert() {
        final MongoCollection<DictionaryWord> collection = collection("words", "words", DictionaryWord.class);
        final MongoRepository<DictionaryWord> repository = repository(collection);
        repository.set(DictionaryWord.newBuilder()
                .setCount(1)
                .setWord("hello")
                .build());
        assertEquals(1, collection.countDocuments());


        repository.set(DictionaryWord.newBuilder()
                .setCount(2)
                .setWord("goodby")
                .build());
        assertEquals(2, collection.countDocuments());
    }


    @Test
    public void listing() {
        final MongoCollection<DictionaryWord> collection = collection("words", "words", DictionaryWord.class);
        final MongoRepository<DictionaryWord> repository = repository(collection);
        repository.set(DictionaryWord.newBuilder()
                .setCount(1)
                .setWord("hello")
                .build());

        repository.set(DictionaryWord.newBuilder()
                .setCount(2)
                .setWord("goodby")
                .build());

        List<DictionaryWord> words = copyOf(repository.list());

        assertEquals(2, words.size());
    }

    @Test
    public void deleting() {
        final MongoCollection<DictionaryWord> collection = collection("words", "words", DictionaryWord.class);
        final MongoRepository<DictionaryWord> repository = repository(collection);
        DictionaryWord a = repository.set(DictionaryWord.newBuilder()
                .setCount(1)
                .setWord("hello")
                .build());

        DictionaryWord b = repository.set(DictionaryWord.newBuilder()
                .setCount(2)
                .setWord("goodby")
                .build());

        repository.delete(a.getId());

        List<DictionaryWord> words = copyOf(repository.list());

        assertEquals(1, words.size());
        assertEquals(b.getId(), words.get(0).getId());
    }


    @Test
    public void listingSince() {
        final MongoCollection<DictionaryWord> collection = collection("words", "words", DictionaryWord.class);
        final MongoRepository<DictionaryWord> repository = repository(collection);
        Timestamp now = TimestampUtil.fromInstant(Instant.now(Clock.systemUTC()));

        repository.set(DictionaryWord.newBuilder()
                .setCount(1)
                .setWord("hello")
                .build());

        repository.set(DictionaryWord.newBuilder()
                .setCount(2)
                .setWord("goodby")
                .build());

        List<DictionaryWord> words = copyOf(repository.list(now));

        assertEquals(2, words.size());
    }

    @Test
    public void listingSinceWithFilters() {
        final MongoCollection<DictionaryWord> collection = collection("words", "words", DictionaryWord.class);
        final MongoRepository<DictionaryWord> repository = repository(collection);
        Timestamp now = TimestampUtil.fromInstant(Instant.now(Clock.systemUTC()));

        repository.set(DictionaryWord.newBuilder()
                .setCount(1)
                .setWord("hello")
                .build());

        repository.set(DictionaryWord.newBuilder()
                .setCount(2)
                .setWord("goodby")
                .build());

        List<DictionaryWord> words = copyOf(repository.list(now, new BsonDocument("word", new BsonString("hello"))));

        assertEquals(1, words.size());
    }

    @Test
    public void listingSinceAfter() {
        final MongoCollection<DictionaryWord> collection = collection("words", "words", DictionaryWord.class);
        final MongoRepository<DictionaryWord> repository = repository(collection);
        repository.set(DictionaryWord.newBuilder()
                .setCount(1)
                .setWord("hello")
                .build());

        repository.set(DictionaryWord.newBuilder()
                .setCount(2)
                .setWord("goodby")
                .build());

        Timestamp now = TimestampUtil.fromInstant(Instant.now(Clock.systemUTC()));
        List<DictionaryWord> words = copyOf(repository.list(now));

        assertEquals(0, words.size());
    }

    @Test
    public void deleteMany() {
        final MongoCollection<DictionaryWord> collection = collection("words", "words", DictionaryWord.class);
        final MongoRepository<DictionaryWord> repository = repository(collection);
        repository.set(DictionaryWord.newBuilder()
                .setCount(1)
                .setWord("hello")
                .build());
        repository.set(DictionaryWord.newBuilder()
                .setCount(2)
                .setWord("hello")
                .build());
        repository.set(DictionaryWord.newBuilder()
                .setCount(3)
                .setWord("world")
                .build());

        BsonDocument selector = new BsonDocument("word", new BsonString("hello"));
        assertEquals(3, copyOf(repository.list()).size());

        repository.deleteMany(selector);
        assertEquals("None of the removed items should exist anymore", 0, copyOf(repository.list(selector)).size());

        assertEquals("Items we didn't want to delete should remain", 1, copyOf(repository.list()).size());
    }
}
