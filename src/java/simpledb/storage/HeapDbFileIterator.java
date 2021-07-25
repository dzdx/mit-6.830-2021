package simpledb.storage;

import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapDbFileIterator implements DbFileIterator {
    private final HeapFile hf;
    private final int tableId;
    private Iterator<Tuple> pageIterator;
    private int pageCur = 0;

    public HeapDbFileIterator(HeapFile hf) {
        this.hf = hf;
        this.tableId = hf.getId();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        PageId pageId = new HeapPageId(tableId, pageCur);
        HeapPage page = (HeapPage) hf.readPage(pageId);
        pageIterator = page.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (pageIterator == null) {
            return false;
        }
        for (; ; ) {
            if (pageIterator.hasNext()) {
                return true;
            }
            pageCur++;
            if (pageCur >= hf.numPages()) {
                pageIterator = null;
                return false;
            }
            PageId pageId = new HeapPageId(tableId, pageCur);
            HeapPage page = (HeapPage) hf.readPage(pageId);
            pageIterator = page.iterator();
        }
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (pageIterator == null) {
            throw new NoSuchElementException();
        }
        for (; ; ) {
            if (pageIterator.hasNext()) {
                return pageIterator.next();
            }
            pageCur++;
            if (pageCur >= hf.numPages()) {
                pageIterator = null;
                throw new NoSuchElementException();
            }
            PageId pageId = new HeapPageId(tableId, pageCur);
            HeapPage page = (HeapPage) hf.readPage(pageId);
            pageIterator = page.iterator();
        }
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {

    }

    @Override
    public void close() {
        pageIterator = null;
    }
}
