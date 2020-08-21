package caper.pronto;

import caper.pronto.encoding.TimestampEncoder;
import com.google.api.gax.protobuf.ProtoReflectionUtil;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.time.Clock;
import java.time.Instant;
import java.util.Iterator;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.BOOLEAN;

public class MongoRepository<T extends Message> {
    // TODO(josh, mike): Make a query builder library that will allow us to build filters, updates,
    //                   and transform operations. Ideally we should have some:
    //                          Filters.like(Message.newBuilder().setA("something"))
    //                   This way engineers never think in terms of mongo. They just think in terms
    //                   of gRPC/protos.
    private final MongoCollection<Message> collection;
    private final Message defaultInstance;
    private final Descriptor descriptor;
    private final FieldDescriptor idField;
    private final FieldDescriptor updatedField;
    private final FieldDescriptor createdField;
    private final FieldDescriptor deletedField;
    private static final FindOneAndReplaceOptions FIND_ONE_REPLACE_UPSERT = new FindOneAndReplaceOptions()
            .upsert(true)
            .returnDocument(ReturnDocument.AFTER);

    public MongoRepository(final MongoCollection<T> collection) {
        this.collection = (MongoCollection<Message>) collection;
        this.defaultInstance = ProtoReflectionUtil.getDefaultInstance(collection.getDocumentClass());
        this.descriptor = this.defaultInstance.getDescriptorForType();
        this.idField = this.descriptor
                .getFields()
                .stream()
                .filter(d -> d.getName().equals("id"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException(
                        "No ID present on proto being saved into collection."
                ));
        this.updatedField = this.descriptor
                .getFields()
                .stream()
                .filter(d -> d.getName().equals("updated"))
                .findFirst()
                .orElse(null);
        this.createdField = this.descriptor
                .getFields()
                .stream()
                .filter(d -> d.getName().equals("created"))
                .findFirst()
                .orElse(null);
        this.deletedField = this.descriptor
                .getFields()
                .stream()
                .filter(d -> d.getName().equals("deleted") && d.getJavaType() == BOOLEAN)
                .findFirst()
                .orElse(null);
    }

    public void delete(String id) throws DeletionFailed {
        if (deletedField == null) {
            DeleteResult result = collection.deleteOne(
                    new BsonDocument("_id", new BsonObjectId(new ObjectId(id)))
            );

            if (!result.wasAcknowledged()) {
                throw new DeletionFailed("Failed to delete " + id + ", not acknowladged by mongo");
            }

            if (result.getDeletedCount() != 1) {
                throw new DeletionFailed("Failed to delete " + id + ", not found in database");
            }
        } else {
            final T old = get(id);

            if (old == null) {
                throw new DeletionFailed("Item not found in database " + id);
            }

            final T value = (T) (old.toBuilder()
                    .setField(deletedField, true)
                    .build());

            set(value);
        }
    }

    /**
     * Delete many records from the database as specified by a filter.
     *
     * @param filter
     */
    public void deleteMany(BsonDocument filter) {
        if (deletedField == null) {
            DeleteResult result = collection.deleteMany(filter);

            if (!result.wasAcknowledged()) {
                throw new DeletionFailed("Failed to delete " + filter + ", not ACKed by mongo");
            }
        } else {
            final String deletedFieldName = deletedField.getName();
            final BsonDocument filterNotDeleted = filter.append(
                    deletedFieldName,
                    new BsonDocument("$ne", new BsonBoolean(true))
            );
            final BsonDocument objectUpdates = new BsonDocument(deletedFieldName, new BsonBoolean(true));

            if (updatedField != null) {
                objectUpdates.append(updatedField.getName(), new BsonDateTime(Instant.now(Clock.systemUTC()).toEpochMilli()));
            }

            UpdateResult result = collection.updateMany(filterNotDeleted, new BsonDocument("$set", objectUpdates));

            if (!result.wasAcknowledged()) {
                throw new DeletionFailed("Failed to soft-delete " + filter + ", not ACKed by mongo");
            }
        }
    }

    /**
     * Replace an object in the MongoDB.
     *
     * @param value
     * @return
     */
    public T set(T value) {
        // Obtain an always-UTC timestamp, no matter the host system's default timestamp.
        final Timestamp timestamp = Timestamps.fromMillis(
                Instant.now(Clock.systemUTC())
                        .toEpochMilli()
        );

        final Message.Builder builder = value.toBuilder();
        setUpdated(builder, timestamp);

        // No ID? Generate one for the object.
        if (!hasId(builder)) {
            // TODO: Set this using the ProtoCodec
            builder.setField(idField, ObjectId.get().toHexString());
            setCreate(builder, timestamp);
        } else {
            // TODO: Load the old object, attempt to find if any changes have been made. If no
            //       changes have been made, just return the existing object from the database.
            //       This will prevent the carts from re-downloading data that has already been
            //       synced and has not been changed on post.

            // TODO: If fields are missing from the provided `T value`, merge in fields from the
            //       existing record in the database. For instance if you have been provided
            //       value = { a: 10 } and the record in the DB has { a: 5, b: 20 } then we should
            //       update `builder` to equal { a: 10, b: 20 } as this is a combination of the
            //       changes from the value and what already existed in the DB.
        }

        return (T) this.collection.findOneAndReplace(
                getFilterToId(getId(builder)),
                builder.build(),
                FIND_ONE_REPLACE_UPSERT
        );
    }

    /**
     * Get an object in this collection by ID.
     *
     * @param id
     * @return
     */
    public T get(String id) {
        return (T) collection.find(getFilterToId(id))
                .comment("Attempting to find " + id)
                .limit(1)
                .first();
    }

    public T get(BsonDocument filter) {
        if (deletedField != null) {
            // Only get with things that haven't been deleted!
            filter.append(deletedField.getName(), new BsonDocument(
                    "$ne", new BsonBoolean(true)
            ));
        }

        return (T) collection.find(filter)
                .comment("Attempting to find single document using filters")
                .limit(1)
                .first();
    }

    private Iterator<T> list(FindIterable<Message> source) {
        return (Iterator<T>) source
                .sort(sorts())
                .iterator();
    }

    /**
     * List all items from the database sorted by updated or creation time if present.
     *
     * @return
     */
    public Iterator<T> list() {
        final FindIterable<Message> cursor;
        if (deletedField != null) {
            cursor = collection.find(new BsonDocument("deleted", new BsonDocument(
                    "$ne", new BsonBoolean(true)
            )));
        } else {
            cursor = collection.find();
        }

        return list(cursor.comment("Listing all objects. No filters provided"));
    }

    /**
     * List all items created or updated since the provided timestamp.
     *
     * @param since - Time to filter all elements by. Only returns things greater than or equal to this timestamp.
     * @return
     * @throws RuntimeException - updated or created must be present on T
     */
    public Iterator<T> list(Timestamp since) {
        return list(since, new BsonDocument());
    }

    /**
     * List all items created or updated since the provided timestamp.
     *
     * @param since   - Time to filter all elements by. Only returns things greater than or equal to this timestamp.
     * @param filters - Filters for more object properties.
     * @return
     * @throws RuntimeException - updated or created must be present on T
     */
    public Iterator<T> list(Timestamp since, BsonDocument filters) {
        final String fieldName;

        if (updatedField != null) {
            fieldName = "updated";
        } else if (createdField != null) {
            fieldName = "created";
        } else {
            // TODO: Fall back to _id
            throw new RuntimeException("This repository cannot be filtered by date! No created/updated!");
        }

        filters.append(fieldName, new BsonDocument(
                "$gte", new BsonDateTime(TimestampEncoder.toMongo(since))
        ));

        // Filter everything since the provided time.
        return list(filters);
    }

    public Iterator<T> list(BsonDocument filters) {
        if (deletedField != null) {
            // Default to only listing records that have not been deleted
            if (!filters.containsKey("deleted")) {
                // This is done because false might not be set when converting from non-soft-deleted
                // to soft-deleted collections.
                // { deleted: { $ne: true } }
                filters.append("deleted", new BsonDocument(
                        "$ne", new BsonBoolean(true)
                ));
            }
        }

        // Filter everything since the provided time.
        return list(collection.find(filters).comment("Listing all objects. No filters provided"));
    }

    public Iterator<T> listWithDeletes() {
        if (deletedField == null) {
            throw new RuntimeException("This collection does not support deletes!");
        }
        return list(collection.find().comment("Listing all objects with deletes."));
    }

    public Iterator<T> listWithDeletes(Timestamp since) {
        if (deletedField == null) {
            throw new RuntimeException("This collection does not support deletes!");
        }
        return listWithDeletes(since, new BsonDocument());
    }

    public Iterator<T> listWithDeletes(Timestamp since, BsonDocument filters) {
        if (deletedField == null) {
            throw new RuntimeException("This collection does not support deletes!");
        }
        final String fieldName;

        if (updatedField != null) {
            fieldName = "updated";
        } else if (createdField != null) {
            fieldName = "created";
        } else {
            // TODO: Fall back to _id
            throw new RuntimeException("This repository cannot be filtered by date! No created/updated!");
        }

        filters.append(fieldName, new BsonDocument(
                "$gte", new BsonDateTime(TimestampEncoder.toMongo(since))
        ));

        // Filter everything since the provided time.
        return list(collection.find(filters).comment("Listing all objects since timestamp with deletes."));
    }

    private BsonDocument getFilterToId(String id) {
        return getFilterToId(new ObjectId(id));
    }

    private BsonDocument getFilterToId(ObjectId id) {
        return new BsonDocument()
                .append("_id", new BsonObjectId(id));
    }

    private void setCreate(Message.Builder builder, Timestamp timestamp) {
        if (createdField != null) {
            builder.setField(createdField, timestamp);
        }
    }

    private void setUpdated(Message.Builder builder, Timestamp timestamp) {
        if (updatedField != null) {
            builder.setField(updatedField, timestamp);
        }
    }

    private String getId(Message.Builder value) {
        return (String) value.getField(idField);
    }

    private boolean hasId(Message.Builder value) {
        return !getId(value).isEmpty();
    }

    private Bson sorts() {
        if (updatedField != null) {
            return Sorts.ascending("updated");
        } else if (createdField != null) {
            return Sorts.ascending("created");
        } else {
            // All objects have an _id field.
            return Sorts.ascending("_id");
        }
    }

    public MongoCollection<Message> getCollection() {
        return collection;
    }
}
