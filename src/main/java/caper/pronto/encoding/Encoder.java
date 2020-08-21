package caper.pronto.encoding;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import org.bson.BsonReader;
import org.bson.BsonWriter;

public abstract class Encoder {
    private final FieldDescriptor field;

    public Encoder(FieldDescriptor field) {
        this.field = field;
    }

    abstract public void encode(BsonWriter writer, Object value);

    abstract public Object decode(BsonReader reader);

    public FieldDescriptor getField() {
        return field;
    }

    /**
     * Name of the encoded field in the mongo database records.
     *
     * @return
     */
    public String name() {
        return getField().getName();
    }

    public <T> T value(Message.Builder builder) {
        return (T) builder.getField(getField());
    }

    public boolean wants(Message.Builder builder) {
        return true;
    }

    public abstract boolean isEmpty(Object value);
}
