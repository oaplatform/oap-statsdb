package oap.statsdb;

import java.util.concurrent.Future;

/**
 * Created by igor.petrenko on 2019-12-17.
 */
public interface StatsDBTransport {
    Future<?> send(RemoteStatsDB.Sync sync);
}
