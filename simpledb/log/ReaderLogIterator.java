package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;

import java.util.Iterator;

public class ReaderLogIterator implements Iterator<byte[]> {

    private FileMgr fm;
    private BlockId blk;
    private Page p;
    private int currentpos;
    private int boundary;
    private int blkCnt=0;

    public ReaderLogIterator(FileMgr fm, BlockId blk) {
        this.fm = fm;
        this.blk = blk;
        int a = fm.length(blk.fileName());
        int c = fm.blockSize();
        this.blkCnt = fm.length(blk.fileName());
        byte[] b = new byte[fm.blockSize()];
        p = new Page(b);
        moveToBlockReverse(blk);
    }

    @Override
    public boolean hasNext() {
        return currentpos>boundary || blk.number()<blkCnt || (currentpos==boundary || blk.number()==blkCnt);
    }

    @Override
    public byte[] next() {
        if (currentpos < boundary) {
            blk = new BlockId(blk.fileName(), blk.number()+1);
            moveToBlockReverse(blk);
        }
        byte[] rec = p.getBytes(currentpos);
        moveToPreviousRecord();
        return rec;
    }

    private void moveToBlockReverse(BlockId blk) {
        fm.read(blk, p);
        boundary = p.getInt(0);
        currentpos = fm.blockSize();
        moveToPreviousRecord();
    }

    private void moveToPreviousRecord() {
        int c = boundary;
        int len = 0;
        while (c<currentpos) {
            len = p.getInt(c);
            c += Integer.BYTES + len;
        }
        currentpos = currentpos-len-Integer.BYTES;
    }
}
