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
import oap.io.IoStreams;
import oap.io.IoStreams.Encoding;
import oap.json.Binder;
import oap.statsdb.RemoteStatsDB.Sync;
import oap.util.Cuid;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class StatsDBNode extends StatsDB implements Runnable, Closeable {
    private static final Cuid timestamp = Cuid.UNIQUE;
    private final Path directory;
    private final StatsDBTransport transport;
    private final Cuid cuid;
    protected boolean lastSyncSuccess = false;
    volatile Sync sync = null;

    public StatsDBNode(NodeSchema schema, StatsDBTransport transport, Path directory) {
        this(schema, transport, directory, Cuid.UNIQUE);
    }

    public StatsDBNode(NodeSchema schema, StatsDBTransport transport, Path directory, Cuid cuid) {
        super(schema);
        this.directory = directory;
        this.transport = transport;
        this.cuid = cuid;

        if (directory != null) {
            var syncPath = directory.resolve("sync.db.gz");
            if (Files.exists(syncPath)) {
                log.info("sync file = {}", syncPath);
                sync = Binder.json.unmarshal(Sync.class, syncPath);
            } else {
                log.debug("{} not exists", syncPath);
            }
        }
    }

    public synchronized void sync() {
        if (sync == null) {
            var snapshot = snapshot();
            if (!snapshot.isEmpty()) {
                sync = new Sync(snapshot, cuid.next(), timestamp.nextLong());
                saveToFile();
            } else {
                lastSyncSuccess = true;
                return;
            }
        }

        try {
            if (transport.send(sync)) {
                sync = null;
                saveToFile();
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

    private synchronized void saveToFile() {
        if (directory != null) {
            var syncFile = directory.resolve("sync.db.gz");
            if (sync == null) try {
                log.debug("sync == null, remove {}", syncFile);
                Files.deleteIfExists(syncFile);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
            else {
                log.debug("saveToFile {}", syncFile);
                oap.io.Files.ensureFile(syncFile);
                try (var sfos = IoStreams.out(syncFile, Encoding.from(syncFile), IoStreams.DEFAULT_BUFFER, false, true)) {
                    Binder.json.marshal(sfos, sync);
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public void run() {
        sync();
    }

    @Override
    public synchronized void removeAll() {
        super.removeAll();

        sync = null;
        saveToFile();
    }

    @Override
    public void close() {
        log.info("close");
        sync();
        saveToFile();
    }
}
