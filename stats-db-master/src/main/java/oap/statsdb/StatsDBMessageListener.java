package oap.statsdb;

import oap.json.Binder;
import oap.message.MessageListener;
import oap.message.MessageProtocol;

import java.io.ByteArrayInputStream;

import static oap.statsdb.StatsDBTransportMessage.MESSAGE_TYPE;

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
        return MESSAGE_TYPE;
    }

    @Override
    public String getInfo() {
        return "stats-db";
    }

    @Override
    public short run(int version, String hostName, int size, byte[] data) {
        var sync = Binder.json.unmarshal(RemoteStatsDB.Sync.class, new ByteArrayInputStream(data));
        master.update(sync, hostName);

        return MessageProtocol.STATUS_OK;
    }
}
