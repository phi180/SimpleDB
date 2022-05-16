package simpledb.tx.recovery;

import simpledb.file.*;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class SetIntRecord implements LogRecord {
   private int txnum, offset, oldval, newval;
   private BlockId blk;

   /**
    * Create a new setint log record.
    * @param bb the bytebuffer containing the log values
    */
   public SetIntRecord(Page p) {
      int tpos = Integer.BYTES;
      txnum = p.getInt(tpos);
      int fpos = tpos + Integer.BYTES;
      String filename = p.getString(fpos);
      int bpos = fpos + Page.maxLength(filename.length());
      int blknum = p.getInt(bpos);
      blk = new BlockId(filename, blknum);
      int opos = bpos + Integer.BYTES;
      offset = p.getInt(opos);
      int vpos = opos + Integer.BYTES;      
      oldval = p.getInt(vpos);
      int npos = vpos + Integer.BYTES;
      newval = p.getInt(npos);
   }

   public int op() {
      return SETINT;
   }

   public int txNumber() {
      return txnum;
   }

   public String toString() {
      return "<SETINT " + txnum + " " + blk + " " + offset + " " + oldval + " " + newval +">";
   }

   /**
    * Replace the specified data value with the value saved in the log record.
    * The method pins a buffer to the specified block,
    * calls setInt to restore the saved value,
    * and unpins the buffer.
    * @see simpledb.tx.recovery.LogRecord#undo(int)
    */
   public void undo(Transaction tx) {
      tx.pin(blk);
      tx.setInt(blk, offset, oldval, false); // don't log the undo!
      tx.unpin(blk);
   }

   public void redo(Transaction tx) {
      tx.pin(blk);
      tx.setInt(blk, offset, newval, false); // don't log the redo!
      tx.unpin(blk);
   }

   /**
    * A static method to write a setInt record to the log.
    * This log record contains the SETINT operator,
    * followed by the transaction id, the filename, number,
    * and offset of the modified block, and the previous
    * integer value at that offset.
    * @return the LSN of the last log value
    */
   public static int writeToLog(LogMgr lm, int txnum, BlockId blk, int offset, int oldval, int newval) {
      int tpos = Integer.BYTES;
      int fpos = tpos + Integer.BYTES;
      int bpos = fpos + Page.maxLength(blk.fileName().length());
      int opos = bpos + Integer.BYTES;
      int vpos = opos + Integer.BYTES;
      int npos = vpos + Integer.BYTES;
      byte[] rec = new byte[npos + Integer.BYTES];
      Page p = new Page(rec);
      p.setInt(0, SETINT);
      p.setInt(tpos, txnum);
      p.setString(fpos, blk.fileName());
      p.setInt(bpos, blk.number());
      p.setInt(opos, offset);
      p.setInt(vpos, oldval);
      p.setInt(npos, newval);
      return lm.append(rec);
   }
}
