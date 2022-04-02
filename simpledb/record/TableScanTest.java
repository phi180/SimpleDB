package simpledb.record;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class TableScanTest {
   public static void main(String[] args) throws Exception {
      original();
   }

   private static void exercise3() {
      SimpleDB db = new SimpleDB("tabletest", 400, 60);
      Transaction tx = db.newTx();

      Schema sch = new Schema();
      sch.addStringField("A",6);
      sch.addStringField("B", 12);
      Layout layout = new Layout(sch);
      for (String fldname : layout.schema().fields()) {
         int offset = layout.offset(fldname);
         System.out.println(fldname + " has offset " + offset);
      }

      TableScan ts = new TableScan(tx, "T", layout);
      System.out.println("Filling the table with 100'000 random records. Current block = " + ts.getRid().blockNumber());
      for (int i=0; i<100000-1;  i++) {
         ts.insert();
         int n = (int) Math.round(Math.random() * 10000);
         ts.setString("A", "AAALLL");
         ts.setString("B", "112233112233");
         System.out.println("inserting into slot " + ts.getRid() + ": {" + n + ", " + "rec"+n + "}");
      }
      ts.insert();
      ts.setString("A", "AtestA");
      ts.setString("B", "recB11223344");

      RID insertion1Rid = ts.getRid();

      System.out.println("Deleting these records, whose A-values are less than 25.");
      int count = 0;
      ts.beforeFirst();
      while (ts.next()) {
         String a = ts.getString("A");
         String b = ts.getString("B");
         if (count<50000) {
            count++;
            System.out.println("slot " + ts.getRid() + ": {" + a + ", " + b + "}");
            ts.delete();
         }
      }

      System.out.println(count + " values under 10 were deleted.\n");

      RID deletionRid = ts.getRid();

      for (int i=0; i<50000;  i++) {
         ts.insert();
         int n = (int) Math.round(Math.random() * 10000);
         ts.setString("A", "AAALLL");
         ts.setString("B", "112233112233");
         System.out.println("inserting into slot " + ts.getRid() + ": {" + n + ", " + "rec"+n + "}");
      }

      RID insertion2Rid = ts.getRid();

      System.out.println("First insertions: blocks="+insertion1Rid.blockNumber());
      System.out.println("Deletions: blocks="+deletionRid.blockNumber());
      System.out.println("Second insertions: blocks="+insertion2Rid.blockNumber());


      // Now getting AtestA
      count = 0;
      ts.beforeFirst();
      while (ts.next()) {
         String a = ts.getString("A");
         String b = ts.getString("B");
         if (a.equals("AtestA")) {
            count++;
            System.out.println("slot " + ts.getRid() + ": {" + a + ", " + b + "}");
         }
      }

      System.out.println("Selection: count="+count);
      System.out.println("Read blocks="+ts.getRid().blockNumber());

   }


   private static void original() {
      SimpleDB db = new SimpleDB("tabletest", 400, 8);
      Transaction tx = db.newTx();

      Schema sch = new Schema();
      sch.addIntField("A");
      sch.addStringField("B", 9);
      Layout layout = new Layout(sch);
      for (String fldname : layout.schema().fields()) {
         int offset = layout.offset(fldname);
         System.out.println(fldname + " has offset " + offset);
      }

      System.out.println("Filling the table with 50 random records.");
      TableScan ts = new TableScan(tx, "T", layout);
      for (int i=0; i<50;  i++) {
         ts.insert();
         int n = (int) Math.round(Math.random() * 50);
         ts.setInt("A", n);
         ts.setString("B", "rec"+n);
         System.out.println("inserting into slot " + ts.getRid() + ": {" + n + ", " + "rec"+n + "}");
      }

      System.out.println("Deleting these records, whose A-values are less than 25.");
      int count = 0;
      ts.beforeFirst();
      while (ts.next()) {
         int a = ts.getInt("A");
         String b = ts.getString("B");
         if (a < 25) {
            count++;
            System.out.println("slot " + ts.getRid() + ": {" + a + ", " + b + "}");
            ts.delete();
         }
      }
      System.out.println(count + " values under 10 were deleted.\n");

      System.out.println("Here are the remaining records.");
      ts.beforeFirst();
      while (ts.next()) {
         int a = ts.getInt("A");
         String b = ts.getString("B");
         System.out.println("slot " + ts.getRid() + ": {" + a + ", " + b + "}");
      }
      ts.close();
      tx.commit();
   }
}
