package oap.statsdb;

import oap.message.MessageSender;
import oap.message.MessageStatus;

import java.util.concurrent.CompletableFuture;

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
    public CompletableFuture<MessageStatus> send(RemoteStatsDB.Sync sync) {
        return sender.sendJson(MESSAGE_TYPE, sync);
    }
}
