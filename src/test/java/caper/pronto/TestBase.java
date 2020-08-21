package caper.pronto;

import caper.pronto.utils.CodecUtils;
import com.google.protobuf.Message;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;

public class TestBase {
    public MongoClient client;
    public MongoServer server;

    @Before
    public void setUp() {
        server = new MongoServer(new MemoryBackend());
        InetSocketAddress serverAddress = server.bind();
        client = MongoClients.create("mongodb://" + serverAddress.toString().substring(1) + "/");
    }

    @After
    public void tearDown() {
        client.close();
        server.shutdown();
    }

    public <T extends Message> MongoCollection<T> collection(String databaseName, String collectionName, Class<T> messageClass) {
        return client.getDatabase(databaseName)
                .withCodecRegistry(CodecUtils.getRegistry())
                .withWriteConcern(WriteConcern.JOURNALED)
                .getCollection(collectionName, messageClass);
    }

    public <T extends Message> MongoRepository<T> repository(String databaseName, String collectionName, Class<T> messageClass) {
        return repository(collection(databaseName, collectionName, messageClass));
    }

    public <T extends Message> MongoRepository<T> repository(MongoCollection<T> collection) {
        return new MongoRepository<T>(collection);
    }
}
