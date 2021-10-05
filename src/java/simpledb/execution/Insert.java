package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private static final TupleDesc resultTd = new TupleDesc(new Type[]{Type.INT_TYPE});

    private final OpIterator child;
    private final TransactionId transactionId;
    private final int tableId;
    private volatile boolean executed = false;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.child = child;
        this.transactionId = t;
        this.tableId = tableId;
        // some code goes here
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return resultTd;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        // some code goes here
    }

    public void close() {
        super.close();
        child.close();
        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        // some code goes here
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (executed) {
            return null;
        }
        int count = 0;
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                Database.getBufferPool().insertTuple(this.transactionId, this.tableId, t);
            } catch (IOException e) {
                throw new DbException(e.getMessage());
            }
            count++;
        }
        Tuple tuple = new Tuple(resultTd);
        tuple.setField(0, new IntField(count));
        executed = true;
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }
}
