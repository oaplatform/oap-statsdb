package oap.statsdb;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.annotation.Nonnull;
import java.io.Serial;
import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

/**
 * Created by igor.petrenko on 2021-02-22.
 */
@ToString
@EqualsAndHashCode
public class NodeId implements Serializable, Iterable<String> {
    @Serial
    private static final long serialVersionUID = 5556915438010788701L;
    private final ArrayList<String> keys = new ArrayList<>();

    public NodeId() {
    }

    public NodeId(List<String> keys) {
        this.keys.addAll(keys);
    }

    public NodeId(String... keys) {
        Collections.addAll(this.keys, keys);
    }

    @Override
    @Nonnull
    public Iterator<String> iterator() {
        return keys.iterator();
    }

    @Override
    public void forEach(Consumer<? super String> action) {
        keys.forEach(action);
    }

    @Override
    public Spliterator<String> spliterator() {
        return keys.spliterator();
    }

    public int size() {
        return keys.size();
    }

    public String get(int index) {
        return keys.get(index);
    }
}
