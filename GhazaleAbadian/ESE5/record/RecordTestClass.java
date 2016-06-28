package simpledb.record;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import simpledb.file.FileMgr;
import simpledb.server.SimpleDB;
import simpledb.stats.BasicFileStats;
import simpledb.stats.BasicRecordStats;
import simpledb.tx.Transaction;

public class RecordTestClass {
	
	public static void main(String[] args) {
		SimpleDB.init("studentdb");
		
		Transaction tx = new Transaction();

		Schema sch = new Schema();
		sch.addStringField("Nome", 10);
		sch.addIntField("Matricola");

		TableInfo ti = new TableInfo("prova", sch);
		RecordFile rf = new RecordFile(ti, tx);
		SimpleDB.fileMgr().resetMapStats();
		insertRecords(ti, tx, 10000,rf);
		printAllBlockStats();
		scan(ti, tx,rf);
		printAllBlockStats();
		deleteRecords(ti, tx, rf);
		printAllBlockStats();
		everageRecods(ti,tx,rf);
		printAllBlockStats();		
		insertRecords2(ti, tx, 7000, rf);
		printAllBlockStats();	
		scan(ti, tx, rf);
		printAllBlockStats();
	}
	
	private static void everageRecods(TableInfo ti, Transaction tx,
			RecordFile rf) {
		System.out.println("Ese 4 --- Scansione del campo Matricola, calcolo della media");
		
		tx = new Transaction();
		rf = new RecordFile(ti, tx);

		rf.beforeFirst();
		rf.next();
		
		int matricoleTotale = 0;
		int numRecords = 0;
		
		while(rf.next()) {
			matricoleTotale += rf.getInt("Matricola");
			numRecords++;
		}
		
		int Media = matricoleTotale/numRecords;
		System.out.println("Media matricole: " + Media + ", su " + numRecords + " records.");
		
		printAllBlockStats();
		System.out.println();
		rf.close();
		tx.commit();
		
	}

	private static void printAllBlockStats(){
		FileMgr fl=SimpleDB.fileMgr();
		for(String fileName :fl.getMapStats().keySet()){
			printBlockStats(fileName, fl.getMapStats().get(fileName));
			
		}
		SimpleDB.fileMgr().resetMapStats();
	   }
	   private static void printBlockStats(String fileName, BasicFileStats fileStats){
				System.out.println("Read"+" filename : "+fileName+" stats : "+fileStats.getBlockRead());
				System.out.println("Write"+" filename : "+fileName+" stats : "+fileStats.getBlockWritten());
				System.out.println("LRU");
	   } 
	
	private static void scan(TableInfo ti, Transaction tx,RecordFile rf) {
						
		System.out.println("Ese 2 --- Lettura dei record");
		
		tx = new Transaction();		
		
		rf.beforeFirst();
		rf.next();
		while(rf.next()) {
			//System.out.println(rf.getString("Nome") + ", " + rf.getInt("Matricola"));
			rf.getString("Nome");
			rf.getInt("Matricola");
		}
		
		printAllBlockStats();
		System.out.println();
		rf.close();
		tx.commit();
		
	}

	/*Metodo che crea e inserisce numberOfRecords record con valori casuali*/
	private static void insertRecords(TableInfo ti, Transaction tx, int numberOfRecords,RecordFile rf) {
		System.out.println("Ese 1 --- Inserimento di "+numberOfRecords+" record casuali ");		
		
		rf.beforeFirst();
		rf.next();

		for(int i=0; i<numberOfRecords; i++) {
			int ranNumb = (int)(Math.random()*100);
			String random = generateString(new Random(), "abcdefghijklmnopqrstuvz", 10);
			rf.insert();
			rf.setInt("Matricola", ranNumb);
			rf.setString("Nome", random);			
			rf.next();
		}
		printAllBlockStats();
		System.out.println();
		rf.close();
		tx.commit();

	}
	private static void insertRecords2(TableInfo ti, Transaction tx, int numberOfRecords,RecordFile rf) {
		System.out.println("Ese 1 --- Inserimento di "+numberOfRecords+" record casuali ");		
		
		rf.beforeFirst();
		while(rf.next()); // mi porto in fondo alla lista

		for(int i=0; i<numberOfRecords; i++) {
			int ranNumb = (int)(Math.random()*100);
			String random = generateString(new Random(), "abcdefghijklmnopqrstuvz", 10);
			rf.insert();
			rf.setInt("Matricola", ranNumb);
			rf.setString("Nome", random);			
			rf.next();
		}
		printAllBlockStats();
		System.out.println();
		rf.close();
		tx.commit();

	}

	/*Metodo che genera stringhe casuali*/
	public static String generateString(Random rng, String characters, int length) {
		char[] text = new char[length];
		for (int i = 0; i < length; i++) {
			text[i] = characters.charAt(rng.nextInt(characters.length()));
		}
		return new String(text);
	}

	/*Metodo che cancella numberOfRecords record */
	private static void deleteRecords(TableInfo ti, Transaction tx,RecordFile rf) {
		System.out.println("Ese 5 --- Eliminazione dei record se mattricola > 50");
		tx = new Transaction();
		rf.beforeFirst();
		rf.next();
		
		while(rf.next()) {
			if(rf.getInt("Matricola") > 50) {
				//System.out.println(rf.getInt("Matricola"));
				//rf.delete();
				rf.getInt("Matricola");
			}
		}
		
		printAllBlockStats();
		System.out.println();
		rf.close();
		tx.commit();
	}
	

}
