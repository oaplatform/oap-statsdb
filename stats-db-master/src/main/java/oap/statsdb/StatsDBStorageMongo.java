package oap.statsdb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import lombok.extern.slf4j.Slf4j;
import oap.reflect.TypeRef;
import oap.storage.mongo.MongoClient;
import org.apache.commons.lang3.mutable.MutableInt;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistries;
import org.joda.time.DateTimeUtils;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by igor.petrenko on 26.03.2019.
 */
@Slf4j
public class StatsDBStorageMongo implements StatsDBStorage, Closeable {
    private static final UpdateOptions UPDATE_OPTIONS_UPSERT = new UpdateOptions().upsert(true);

    private final MongoCollection<MongoNode> collection;
    public int bulkSize = 1000;
    private long lastFsync = -1;

    public StatsDBStorageMongo(MongoClient mongoClient, String table) {
        var ref = new TypeRef<MongoNode>() {
        };

        var codecRegistry = CodecRegistries.fromRegistries(
                CodecRegistries.fromCodecs(new JsonNodeCodec()),
                mongoClient.database.getCodecRegistry()
        );

        this.collection = mongoClient.database
                .getCollection(table, ref.clazz())
                .withCodecRegistry(codecRegistry);

    }

    @Override
    public Map<String, Node> load(NodeSchema schema) {
        log.debug("load {}", schema);
        final Map<String, Node> db = new HashMap<>();

        final Consumer<MongoNode> cons = node -> {
            assert node.n.db.isEmpty();

            var cdb = db;
            for (int i = 0; i < node._id.size() - 1; i++) {
                var nc = schema.get(i);
                var key = node._id.get(nc.key);
                cdb = cdb.computeIfAbsent(key, k -> new Node(nc.newInstance())).db;
            }

            var lastId = node._id.get(schema.get(node._id.size() - 1).key);
            var lastNode = cdb.get(lastId);
            if (lastNode == null) {
                cdb.put(lastId, node.n);
            } else {
                cdb.put(lastId, node.n);
                node.n.db.putAll(lastNode.db);
            }
        };

        collection.find().forEach(cons);

        lastFsync = DateTimeUtils.currentTimeMillis();

        return db;
    }

    @Override
    public void store(NodeSchema schema, Map<String, Node> db) {
        log.debug("store {}", schema);
        var count = 0;

        var now = DateTimeUtils.currentTimeMillis();

        var bulk = new ArrayList<WriteModel<MongoNode>>();
        count += store(schema, 0, new HashMap<>(), db, bulk);
        if (!bulk.isEmpty()) {
            collection.bulkWrite(bulk);
            count += bulk.size();
        }

        lastFsync = now;

        log.info("[{}] fsync modified: {}", collection.getNamespace(), count);
    }

    @Override
    public void removeAll() {
        collection.deleteMany(new BsonDocument());
    }

    private int store(NodeSchema schema, int index, Map<String, String> id,
                      Map<String, Node> db, ArrayList<WriteModel<MongoNode>> bulk) {
        if (db.isEmpty()) return 0;

        assert index <= schema.size();

        var count = new MutableInt();

        db.forEach((key, value) -> {
            var newId = new HashMap<>(id);
            newId.put(schema.get(index).key, key);

            if (value.mt >= lastFsync) {
                bulk.add(new ReplaceOneModel<>(eq("_id", newId), new MongoNode(newId, value), UPDATE_OPTIONS_UPSERT));
                if (bulk.size() >= bulkSize) {
                    collection.bulkWrite(bulk);
                    count.add(bulk.size());
                    bulk.clear();
                }
            }

            count.add(store(schema, index + 1, newId, value.db, bulk));
        });

        return count.intValue();
    }

    @Override
    public void close() {
    }

    public void insertMany(List<MongoNode> stats) {
        collection.insertMany(stats);
    }
}
