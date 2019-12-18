package oap.statsdb;

/**
 * Created by igor.petrenko on 2019-12-17.
 */
public interface StatsDBTransport {
    boolean send(RemoteStatsDB.Sync sync);
}
