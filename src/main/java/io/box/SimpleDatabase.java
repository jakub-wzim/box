package io.box;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleDatabase implements Database {

    private Map<String, String> map = new HashMap<String, String>();
    private Map<String, Long> counter = new HashMap<String, Long>();

    private List<Operation> stack = new ArrayList<>();
    private boolean inTx = false;

    public Database set(final String key, final String value) {
        if (!counter.containsKey(value)) {
            counter.put(value, 0L);
        }

        String prevValue = null;
        if (!map.containsKey(key)) {
            map.put(key, value);
            counter.put(value, counter.get(value) + 1);
        } else if (value.equals(map.get(key))) { // do nothing

        } else {
            counter.put(value, counter.get(value) + 1);
            prevValue = map.put(key, value);
            counter.put(prevValue, counter.get(prevValue) - 1);

        }
        if (inTx) {
            stack.add(new SetOperation(key, value, prevValue));
        }

        return this;
    }

    public String get(final String key) {
        return map.get(key);
    }

    public Database delete(final String key) {
        String prev = map.remove(key);
        if (prev != null) {
            counter.put(prev, counter.get(prev) - 1);
        }

        if (inTx && prev != null) {
            stack.add(new DeleteOperation(key, prev));
        }
        return this;
    }

    public long count(final String value) {

        final Long counter = this.counter.get(value);
        return counter == null ? 0 : counter;
    }

    public Database begin() {
        stack.add(new BeginOperation());
        inTx = true;
        return this;
    }

    public Database rollback() {
        if (stack.isEmpty()) {
            System.out.println("NO TRANSACTION");
            return this;
        }
        Operation op;
        do {
            op = stack.remove(stack.size() - 1);
            if (!(op instanceof BeginOperation)) {
                op.rollback();
            }
        } while (!(op instanceof BeginOperation));

        if (stack.isEmpty()) {
            inTx = false;
        }
        return this;
    }

    public Database commit() {
        stack = new ArrayList<>();
        inTx = false;
        return this;
    }

    class SetOperation extends Operation {
        final String key;
        final String value;
        final String prevValue;

        public SetOperation(final String key, final String value, final String prevValue) {
            this.key = key;
            this.value = value;
            this.prevValue = prevValue;
        }

        void rollback() {
            inTx = false;
            delete(key);
            set(key, prevValue);
            inTx = true;

        }
    }

    class DeleteOperation extends Operation {
        final String key;
        final String prevValue;

        public DeleteOperation(final String key, final String prevValue) {
            this.key = key;
            this.prevValue = prevValue;
        }

        void rollback() {
            inTx = false;
            set(key, prevValue);
            inTx = true;
        }
    }

    public static class BeginOperation extends Operation {
    }

    abstract static class Operation {
        void rollback() {
            throw new UnsupportedOperationException();
        }
    }
}
