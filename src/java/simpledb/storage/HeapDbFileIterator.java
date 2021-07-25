package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapDbFileIterator implements DbFileIterator {
    private final HeapFile hf;
    private final TransactionId tid;
    private final int tableId;
    private Iterator<Tuple> pageIterator;
    private int pageCur = 0;

    public HeapDbFileIterator(HeapFile hf, TransactionId tid) {
        this.hf = hf;
        this.tid = tid;
        this.tableId = hf.getId();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        if (pageCur >= hf.numPages()) {
            return;
        }
        PageId pageId = new HeapPageId(tableId, pageCur);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
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
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
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
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            pageIterator = page.iterator();
        }
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        pageCur = 0;
        PageId pageId = new HeapPageId(tableId, pageCur);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
        pageIterator = page.iterator();
    }

    @Override
    public void close() {
        pageIterator = null;
    }
}
