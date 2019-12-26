/*
 * The MIT License (MIT)
 *
 * Copyright (c) Open Application Platform Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package oap.statsdb;

import lombok.extern.slf4j.Slf4j;
import oap.statsdb.RemoteStatsDB.Sync;
import oap.util.Cuid;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class StatsDBNode extends StatsDB implements Runnable, Closeable {
    private static final Cuid timestamp = Cuid.UNIQUE;
    private final StatsDBTransport transport;
    private final Cuid cuid;
    protected boolean lastSyncSuccess = false;

    public StatsDBNode(NodeSchema schema, StatsDBTransport transport) {
        this(schema, transport, Cuid.UNIQUE);
    }

    public StatsDBNode(NodeSchema schema, StatsDBTransport transport, Cuid cuid) {
        super(schema);
        this.transport = transport;
        this.cuid = cuid;
    }

    public synchronized void sync() {
        try {
            var snapshot = snapshot();
            if (!snapshot.isEmpty()) {
                var sync = new Sync(snapshot, cuid.next(), timestamp.nextLong());
                transport.send(sync).get();
            }

            lastSyncSuccess = true;
        } catch (Exception e) {
            lastSyncSuccess = false;
            log.error(e.getMessage(), e);
        }
    }

    private Map<String, Node> snapshot() {
        var ret = db;
        db = new ConcurrentHashMap<>();

        return ret;
    }

    @Override
    public void run() {
        sync();
    }

    @Override
    public synchronized void removeAll() {
        super.removeAll();
    }

    @Override
    public void close() {
        log.info("close");
        sync();
    }
}
