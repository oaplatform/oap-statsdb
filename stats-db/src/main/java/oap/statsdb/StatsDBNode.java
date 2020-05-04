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
import org.apache.commons.lang3.mutable.MutableObject;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class StatsDBNode extends StatsDB implements Runnable, Closeable {
    private final StatsDBTransport transport;
    private final Cuid timestamp;
    protected boolean lastSyncSuccess = false;

    public StatsDBNode(NodeSchema schema, StatsDBTransport transport) {
        this(schema, transport, Cuid.UNIQUE);
    }

    public StatsDBNode(NodeSchema schema, StatsDBTransport transport, Cuid timestamp) {
        super(schema);
        this.transport = transport;
        this.timestamp = timestamp;
    }

    public synchronized void sync() {
        try {
            var snapshot = snapshot();
            if (!snapshot.isEmpty()) {
                var sync = new Sync(snapshot, timestamp.next());
                transport.send(sync).get();
            }

            lastSyncSuccess = true;
        } catch (Exception e) {
            lastSyncSuccess = false;
            log.error(e.getMessage(), e);
        }
    }

    private Map<String, Node> snapshot() {
        var ret = new HashMap<String, Node>();
        var mnode = new MutableObject<Node>();
        for (var entry : db.entrySet()) {
            db.compute(entry.getKey(), (k, v) -> {
                mnode.setValue(v);
                return null;
            });
            ret.put(entry.getKey(), mnode.getValue());
        }

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
