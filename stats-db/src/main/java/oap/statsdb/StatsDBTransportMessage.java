package oap.statsdb;

import oap.message.MessageSender;

/**
 * Created by igor.petrenko on 2019-12-17.
 */
public class StatsDBTransportMessage implements StatsDBTransport {
    public final byte MESSAGE_TYPE = 10;

    private final MessageSender sender;

    public StatsDBTransportMessage(MessageSender sender) {
        this.sender = sender;
    }

    @Override
    public boolean send(RemoteStatsDB.Sync sync) {
        return sender.sendJson(MESSAGE_TYPE, sync);
    }
}
