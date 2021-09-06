package simpledb.execution;

import simpledb.common.Type;
import simpledb.common.Utility;
import simpledb.storage.*;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldType;
    private final int afield;
    private final Op what;
    private final TupleDesc td;
    private final Map<Field, AggResult> results = new LinkedHashMap<>();

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if(gbfield != Aggregator.NO_GROUPING){
            this.td = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE,});
        }else{
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field keyField;
        if(gbfield != Aggregator.NO_GROUPING){
            keyField = tup.getField(gbfield);
        }else{
            keyField = Aggregator.EMPTY_FIELD;
        }
        int newValue = ((IntField) tup.getField(afield)).getValue();
        results.computeIfAbsent(keyField, k -> new AggResult(what)).merge(newValue);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>(results.size());
        for (Map.Entry<Field, AggResult> entry : results.entrySet()) {
            Tuple tp = new Tuple(td);
            if(gbfield != Aggregator.NO_GROUPING){
                tp.setField(0, entry.getKey());
                tp.setField(1, new IntField(entry.getValue().intResult()));
            }else{
                tp.setField(0, new IntField(entry.getValue().intResult()));
            }
            tuples.add(tp);
        }
        return new TupleIterator(td, tuples);
    }

    private static class AggResult {
        private int count;
        private float result;
        private final Op op;

        private AggResult(Op op) {
            this.op = op;
        }

        public void merge(int newValue) {
            if (count == 0) {
                result = newValue;
                count = 1;
                return;
            }
            switch (op) {
                case MIN:
                    result = Math.min(result, newValue);
                    break;
                case MAX:
                    result = Math.max(result, newValue);
                    break;
                case SUM:
                    result += newValue;
                    break;
                case AVG:
                    result = ((result * count) + newValue) / (count + 1);
                    break;
            }
            count++;
        }

        public int intResult() {
            if(Op.COUNT == op){
                return count;
            }
            return (int) result;
        }
    }
}
