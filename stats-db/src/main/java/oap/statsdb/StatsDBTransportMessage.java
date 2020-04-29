package oap.statsdb;

import oap.message.MessageSender;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Created by igor.petrenko on 2019-12-17.
 */
public class StatsDBTransportMessage implements StatsDBTransport {
    public static final byte MESSAGE_TYPE = 10;

    private final MessageSender sender;

    public StatsDBTransportMessage(MessageSender sender) {
        this.sender = sender;
    }

    @Override
    public Future<?> send(RemoteStatsDB.Sync sync) {
        return sender.sendJson(MESSAGE_TYPE, sync);
    }
}
