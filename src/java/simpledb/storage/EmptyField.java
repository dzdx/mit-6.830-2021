package simpledb.storage;

import simpledb.common.Type;
import simpledb.execution.Predicate;

import java.io.DataOutputStream;
import java.io.IOException;

public class EmptyField implements Field {
    @Override
    public void serialize(DataOutputStream dos) throws IOException {
        throw new RuntimeException("not implements");
    }

    @Override
    public boolean compare(Predicate.Op op, Field value) {
        throw new RuntimeException("not implements");
    }

    @Override
    public Type getType() {
        throw new RuntimeException("not implements");
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof EmptyField;
    }
}
