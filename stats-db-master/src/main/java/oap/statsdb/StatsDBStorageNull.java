package oap.statsdb;

import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Created by igor.petrenko on 26.03.2019.
 */
public class StatsDBStorageNull implements StatsDBStorage {
    @Override
    public Map<String, Node> load(KeySchema schema) {
        return emptyMap();
    }

    @Override
    public void store(KeySchema schema, Map<String, Node> db) {

    }
}
