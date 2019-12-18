package oap.statsdb;

import oap.json.Binder;
import oap.message.MessageListener;

import java.io.ByteArrayInputStream;

/**
 * Created by igor.petrenko on 2019-12-17.
 */
public class StatsDBMessageListener implements MessageListener {
    private final StatsDBMaster master;

    public StatsDBMessageListener(StatsDBMaster master) {
        this.master = master;
    }

    @Override
    public byte getId() {
        return 1;
    }

    @Override
    public String getInfo() {
        return "stats-db";
    }

    @Override
    public void run(int version, String hostName, int size, byte[] data) {
        var sync = Binder.json.<RemoteStatsDB.Sync>unmarshal(RemoteStatsDB.Sync.class, new ByteArrayInputStream(data));
        master.update(sync, hostName);
    }
}
