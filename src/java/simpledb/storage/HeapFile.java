package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.awt.image.DataBufferUShort;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     * the file that stores the on-disk backing store for this heap
     * file.
     */

    private final File f;
    private final RandomAccessFile rf;
    private final TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        try {
            this.rf = new RandomAccessFile(f, "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e.getMessage());
        }
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        HeapPageId hpid = (HeapPageId) pid;
        int pgNo = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        try {
            int offset = pageSize * pgNo;
            int fileLength = (int) rf.length();
            int size = Math.min(fileLength - pageSize * pgNo, pageSize);
            byte[] buffer = new byte[size];
            rf.seek(offset);
            rf.readFully(buffer);
            return new HeapPage(hpid, buffer);
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        HeapPageId hpid = (HeapPageId) page.getId();
        int pgNo = hpid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        int offset = pageSize * pgNo;
        rf.seek(offset);
        rf.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(f.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        HeapPage page;
        for (int pgNo = 0; ; pgNo++) {
            HeapPageId pageId = new HeapPageId(getId(), pgNo);
            try {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                if (page.getNumEmptySlots() == 0) {
                    continue;
                }
            } catch (IllegalArgumentException e) {
                page = new HeapPage(pageId, HeapPage.createEmptyPageData());
                writePage(page);
                page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            }
            break;
        }
        page.insertTuple(t);
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        HeapPage page;
        RecordId recordId = t.getRecordId();
        if (getId() != recordId.getPageId().getTableId()) {
            throw new DbException(String.format("tableId not equals %d != %d", getId(), recordId.getPageId().getTableId()));
        }
        HeapPageId pageId = new HeapPageId(getId(), recordId.getPageId().getPageNumber());

        page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return Collections.singletonList(page);
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapDbFileIterator(this, tid);
    }
}

