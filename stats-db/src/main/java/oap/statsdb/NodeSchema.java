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

import com.google.common.base.Preconditions;
import lombok.ToString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Arrays.asList;

@ToString(callSuper = true)
public class NodeSchema extends ArrayList<NodeSchema.NodeConfiguration> {
    private final HashMap<String, Supplier<Node.Value>> cons = new HashMap<>();

    public NodeSchema() {
    }

    public NodeSchema(NodeConfiguration... confs) {
        this(asList(confs));
    }

    public NodeSchema(List<NodeConfiguration> confs) {
        super(confs);
        for (var c : confs) {
            cons.put(c.key, c.newInstance);
        }
    }

    public static NodeConfiguration nc(String key, Supplier<Node.Value> newInstance) {
        return new NodeConfiguration(key, newInstance);
    }

    @ToString
    public static class NodeConfiguration {
        public final String key;
        public final Supplier<Node.Value> newInstance;

        public NodeConfiguration(String key, Supplier<Node.Value> newInstance) {
            this.key = key;
            this.newInstance = newInstance;
        }
    }
}
