package oap.statsdb;

import java.util.concurrent.CompletableFuture;

/**
 * Created by igor.petrenko on 2019-12-17.
 */
public interface StatsDBTransport {
    CompletableFuture<?> send(RemoteStatsDB.Sync sync);
}
