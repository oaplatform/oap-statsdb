package oap.statsdb;

import ch.qos.logback.classic.LoggerContext;
import oap.benchmark.Benchmark;
import oap.concurrent.scheduler.Scheduler;
import oap.message.MessageSender;
import oap.message.MessageServer;
import oap.testng.Env;
import oap.testng.Fixtures;
import oap.testng.SystemTimerFixture;
import oap.testng.TestDirectoryFixture;
import oap.util.Cuid;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by igor.petrenko on 2020-05-13.
 */
public class StatsDBPerformance extends Fixtures {
    private final static int SAMPLES = 10000000;
    private final static int EXPERIMENTS = 5;
    private final static int THREADS = 500;

    {
        fixture(TestDirectoryFixture.FIXTURE);
        fixture(SystemTimerFixture.FIXTURE);
    }

    @Test
    public void testPerf() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        try {
            loggerContext.stop();

            var uid = Cuid.incremental(0);

            var port = Env.port("ver");

            try (var master = new StatsDBMaster(StatsDBTest.schema3, StatsDBStorage.NULL);
                 var messageServer = new MessageServer(TestDirectoryFixture.testPath("mserv"), port, List.of(new StatsDBMessageListener(master)), -1);
                 var messageSender = new MessageSender("localhost", port, TestDirectoryFixture.testPath("msend"));
                 var node = new StatsDBNode(StatsDBTest.schema3, new StatsDBTransportMessage(messageSender), uid);
                 var ignored = Scheduler.scheduleWithFixedDelay(100, TimeUnit.MILLISECONDS, node::sync)) {
                messageServer.start();
                messageSender.start();


                Benchmark.benchmark("statsdb", SAMPLES, () -> {
                    node.<StatsDBTest.MockValue>update("k1", "k2", "k3", c -> c.v += 1);
                    node.<StatsDBTest.MockValue>update("k1", "k2", "s3", c -> c.v += 1);
                    node.<StatsDBTest.MockChild2>update("k1", "k2", c -> c.vc += 1);
                }).experiments(EXPERIMENTS).inThreads(THREADS, SAMPLES).run();

                assertThat(master.<StatsDBTest.MockChild1>get("k1").sum).isGreaterThan(SAMPLES);

                node.sync();

                assertThat(master.<StatsDBTest.MockChild2>get("k1", "k2").vc).isEqualTo(SAMPLES * EXPERIMENTS + SAMPLES);

                assertThat(master.<StatsDBTest.MockChild1>get("k1").sum).isEqualTo((SAMPLES * EXPERIMENTS + SAMPLES) * 2);
                assertThat(master.<StatsDBTest.MockChild1>get("k1").sum2).isEqualTo(SAMPLES * EXPERIMENTS + SAMPLES);

                assertThat(master.<StatsDBTest.MockValue>get("k1", "k2", "k3").v).isEqualTo(SAMPLES * EXPERIMENTS + SAMPLES);
                assertThat(master.<StatsDBTest.MockValue>get("k1", "k2", "s3").v).isEqualTo(SAMPLES * EXPERIMENTS + SAMPLES);
            }
        } finally {
            loggerContext.start();
        }
    }
}
