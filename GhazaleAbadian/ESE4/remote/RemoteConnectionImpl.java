package simpledb.remote;

import simpledb.file.FileMgr;
import simpledb.server.SimpleDB;
import simpledb.stats.BasicFileStats;
import simpledb.tx.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The RMI server-side implementation of RemoteConnection.
 * @author Edward Sciore
 */
@SuppressWarnings("serial") 
class RemoteConnectionImpl extends UnicastRemoteObject implements RemoteConnection {
   private Transaction tx;
   
   /**
    * Creates a remote connection
    * and begins a new transaction for it.
    * @throws RemoteException
    */
   RemoteConnectionImpl() throws RemoteException {
      tx = new Transaction();
      printAllBlockStats();
      SimpleDB.fileMgr().resetMapStats();

   }
   
   /**
    * Creates a new RemoteStatement for this connection.
    * @see simpledb.remote.RemoteConnection#createStatement()
    */
   public RemoteStatement createStatement() throws RemoteException {
      return new RemoteStatementImpl(this);
   }
   
   /**
    * Closes the connection.
    * The current transaction is committed.
    * @see simpledb.remote.RemoteConnection#close()
    */
   public void close() throws RemoteException {
      tx.commit();
   }
   
// The following methods are used by the server-side classes.
   
   /**
    * Returns the transaction currently associated with
    * this connection.
    * @return the transaction associated with this connection
    */
   Transaction getTransaction() { 
      return tx;
   }
   
   /**
    * Commits the current transaction,
    * and begins a new one.
    */
   void commit() {
      tx.commit();
      System.out.println("Stampa statistiche");
      tx = new Transaction();
      printAllBlockStats();
   }
   
   /**
    * Rolls back the current transaction,
    * and begins a new one.
    */
   void rollback() {
      tx.rollback();
      tx = new Transaction();
   }
   private static void printAllBlockStats(){
	FileMgr fl=SimpleDB.fileMgr();
	for(String fileName :fl.getMapStats().keySet()){
		printBlockStats(fileName, fl.getMapStats().get(fileName));
		
	}
   }
   private static void printBlockStats(String fileName, BasicFileStats fileStats){
			System.out.println("Read"+" filename : "+fileName+" stats : "+fileStats.getBlockRead());
			System.out.println("Write"+" filename : "+fileName+" stats : "+fileStats.getBlockWritten());
			System.out.println("LRU");
   } 
  


}

