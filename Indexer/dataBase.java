import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class dataBase {
	private Object o1 = new Object();
	private Object o2 = new Object();
	private String Db_name;
	private Connection c = null;
	//final static String url1 = "jdbc:mysql://localhost:3306/" + Db_name;
    final static String user = "jimmy";
    final static String password = "jimmy";
    //private static Statement stmt = null;
	
    public dataBase(String Db_name) {
		//set url for this data base object	
    	String url = "jdbc:mysql://localhost:3306/" + Db_name;
		this.connect(url);
           
    }

	public void connect(String url) {
        
		//open connection
		try {
            this.c = DriverManager.getConnection(url, user, password);
            if (this.c != null) {
                System.out.println("Connected to the database " + Db_name);
            }
 
        } catch (SQLException ex) {
            System.out.println("An error occurred. Maybe user/password is invalid");
            ex.printStackTrace();
        }
    }
	
	public boolean insertNewDocument(int docID, int wordsCount,
			List<String> word, List<ArrayList<Boolean> > tags ,
			List<String> positions, List<Float> df, String pubDate,
			String pageDescription,String pageTitle) throws SQLException {
		
		synchronized (o1) {
			// check if the document already exist
			String selectQuery = "select * from documents where documentID = ?";
			PreparedStatement select = c.prepareStatement(selectQuery);
			select.setInt(1, docID);
			ResultSet rs = select.executeQuery();
			boolean newDoc = true;
			if(rs.next()) {
				newDoc = false;
			}
			
			if(newDoc) {// if new Doc
				String query = "INSERT INTO .documents(documentID, pageRank, wordsNumber, publishedDate, description, title) "
							+ "VALUES (?, ?, ?, ?, ?, ?);";
				PreparedStatement pst = c.prepareStatement(query); // prepare insert query
				 
				pst.setInt(1, docID);
				pst.setFloat(2, (float) .25);
				pst.setInt(3, wordsCount);
				pst.setString(4,pubDate);
				pst.setString(5,pageDescription);
				pst.setString(6,pageTitle);
				int done = pst.executeUpdate();
				 
			   /* if (done > 0) {
			        System.out.println("Insert New Record successfully");
			    }*/    
			}else {//
				String query = "update documents set wordsNumber = ?, publishedDate = ?, description = ?, title = ?  where documentID = ?";
				PreparedStatement pst = c.prepareStatement(query); // prepare insert query
				 
				pst.setInt(1, wordsCount);
				pst.setString(2, pubDate);
				pst.setString(3,pageDescription);
				pst.setString(4,pageTitle);
				pst.setInt(5, docID);
				int done = pst.executeUpdate();
				/*if (done > 0) {
			        System.out.println("Updated document record on words");
			    } */ 
				query = "UPDATE words inner join searchIndex on words.wordID = searchIndex.wordID "
							+ "set words.documentsNumber = words.documentsNumber-1 "
							+ "where searchIndex.documentID = ? ";
				pst = c.prepareStatement(query); // prepare insert query
				 
				pst.setInt(1, docID);
				done = pst.executeUpdate();
				 /*if (done > 0) {
				        System.out.println("REmoved document effect on words");
				    }*/   
				 
				query = "DELETE FROM searchIndex WHERE documentID = ?";
				pst = c.prepareStatement(query); // prepare insert query
				pst.setInt(1, docID);
				done = pst.executeUpdate();
			    /*if (done > 0) {
			        System.out.println("Deleted search index records successfully");
			    }*/    
			}
			//System.out.println("Document = "+String.valueOf(docID)+" Document finished");
			insertNewWord(word, docID,  tags , positions, df);
			//System.out.println("Document = "+String.valueOf(docID)+" Words finished");
			return newDoc;
	}
		
		
};
	
	public void insertNewWord(List<String> word, int docId, List<ArrayList<Boolean> > tags ,List<String> positions, List<Float> df) throws SQLException{
		//check if new word
		List<Integer> wordID = new ArrayList<Integer>(); //word ID
		c.setAutoCommit(false);
		for(int i = 0; i<word.size(); i++) {
			
			boolean newWord = true;//flag for new word
			
			String newWordQuery = "select wordID from words where word = ?";
			PreparedStatement select = c.prepareStatement(newWordQuery);
			
			select.setString(1, word.get(i));
			ResultSet rs = select.executeQuery();
			if(rs.next()) {// if not new
				newWord = false;
				wordID.add(rs.getInt(1));
			}
			
			if(newWord) {
				//Insert the new Word
				String query = "INSERT INTO words (wordID, word, documentsNumber) VALUES (0, ?, 1);";
				PreparedStatement pst = c.prepareStatement(query,Statement.RETURN_GENERATED_KEYS);
				pst.setString(1, word.get(i));
				int done = pst.executeUpdate(); 
				/*if (done > 0) {
			        System.out.println("New Word Record successfully");
			    } */   
				rs = pst.getGeneratedKeys();
				while(rs.next()) {
					wordID.add(rs.getInt(1));
				}
			}
			else {
				String query = "UPDATE words set documentsNumber = documentsNumber + 1 where wordID = ?";
				PreparedStatement pst = c.prepareStatement(query);
				pst.setInt(1, wordID.get(i));
				int done = pst.executeUpdate(); 
				/*if (done > 0) {
			        System.out.println("INC docNumber Word Record successfully");
			    }*/
			}
		}
		c.commit();
		c.setAutoCommit(true);
		//System.out.println("Document = "+String.valueOf(docId)+" Words part 1");
		String query = "INSERT INTO searchIndex (documentID, wordID, title, h1, h2, strong, b, "
				+ "description, df, positions) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
		PreparedStatement pst = c.prepareStatement(query);
		c.setAutoCommit(false);
		for(int i = 0; i<word.size(); i++) {
			// insert in search index
			
			
			pst.setInt(1, docId);
			pst.setInt(2,wordID.get(i));
			for(int j = 0; j<tags.get(i).size(); j++) {
				pst.setBoolean(j+3, tags.get(i).get(j));
			}
			pst.setFloat(9, df.get(i));
			pst.setString(10, positions.get(i));
			
			pst.executeUpdate(); 
			//if (done > 0) {
		      //  System.out.println("Added New search index Record successfully");
		    //}
		}
		
		c.commit();
		c.setAutoCommit(true);
		
	}
		
	public List<Object> getDocuments() throws SQLException{
		String Query = "Select * from Crawled_URLS where Changed = 1 limit 10;";
		PreparedStatement pst = c.prepareStatement(Query);
		ResultSet rs = pst.executeQuery();
		List<Object> docs= new ArrayList<Object>();
		while(rs.next()) {
			docs.add(rs.getInt(1));//add id
			docs.add(rs.getString(3));//add url
		}
		
		String updateQuery = "UPDATE Crawled_URLS SET Changed = 0 WHERE Url_ID  = ?";
		PreparedStatement upst = c.prepareStatement(updateQuery);
		c.setAutoCommit(false);
		for(int i = 0; i<docs.size(); i+=2) {
			upst.setInt(1, (int)docs.get(i));
			upst.executeUpdate();
		}
		c.commit();
		c.setAutoCommit(true);
		
		
		return docs;
	};
	
	public List<Object> getImages() throws SQLException{
		String Query = "select ID, ALT from Imgs_URLS where NEW = 1 limit 10";
		PreparedStatement pst = c.prepareStatement(Query);
		ResultSet rs = pst.executeQuery();
		List<Object> imgs= new ArrayList<Object>();
		while(rs.next()) {
			imgs.add(rs.getInt(1));//add id
			imgs.add(rs.getString(2));//add ALT
		}
		
		String updateQuery = "UPDATE Imgs_URLS SET NEW = 0 WHERE ID  = ?";
		PreparedStatement upst = c.prepareStatement(updateQuery);
		c.setAutoCommit(false);
		for(int i = 0; i<imgs.size(); i+=2) {
			upst.setInt(1, (int)imgs.get(i));
			upst.executeUpdate();
		}
		c.commit();
		c.setAutoCommit(true);
		
		
		return imgs;
	};
	
	public void insertImages(List<Integer> ids, List<ArrayList<String>> words) throws SQLException {
		String query = "INSERT INTO imageIndex (imageID,word)VALUES(?,?);";
		PreparedStatement pst = c.prepareStatement(query);
		c.setAutoCommit(false);
		for(int i = 0; i<ids.size(); i++) {
			for(int j = 0; j<words.get(i).size(); j++) {
				pst.setInt(1, ids.get(i));
				pst.setString(2, words.get(i).get(j));
				pst.executeUpdate();
			}
		}
		c.commit();
		c.setAutoCommit(true);
	}
	
	public void testFn() throws SQLException, ParseException{
		String query = "INSERT INTO .documents(documentID, pageRank, wordsNumber, publishedDate) "
				+ "VALUES (?, ?, ?, ?);";
	PreparedStatement pst = c.prepareStatement(query); // prepare insert query
	 
	pst.setInt(1, 1);
	pst.setFloat(2, (float) .25);
	pst.setInt(3, 50);
	String W="2002-11-30";
    Date date1=new SimpleDateFormat("yyyy-MM-dd").parse(W);
	pst.setString(4, W);
	int done = pst.executeUpdate();
					
}
	
	public static void main(String[] args) throws SQLException, ParseException {
		// TODO Auto-generated method stub
		dataBase db = new dataBase("testdb");
		db.testFn();
		
		
		/*
		List<String> finalWords = new ArrayList<String>();
        List<String> finalPosition = new ArrayList<String>();
        List<ArrayList<Boolean> > finalTags = new ArrayList<ArrayList<Boolean> >();
        List<Integer> occurrences = new ArrayList<Integer>();
        List<Float> df = new ArrayList<Float>();
		boolean doc1 = db.insertNewDocument(1, 3);
		ArrayList<Boolean> tags = new ArrayList<Boolean>();
		tags.add(true);
		tags.add(true);
		tags.add(false);
		tags.add(true);
		tags.add(false);
		tags.add(false);
		finalWords.add("jimmy");
		finalWords.add("amr");
		finalWords.add("gelesh"); */
		/*
		db.insertNewWord("jimmy", 1, tags, "1 4 5", doc1, (float).33);
		db.insertNewWord("amr", 1, tags, "3 5", doc1, (float).33);
		db.insertNewWord("gelesh", 1, tags, "7 8", doc1, (float).33);
		
		boolean doc2 = db.insertNewDocument(2, 3);
		db.insertNewWord("jimmy", 2, tags, "2", doc2, (float).5);
		db.insertNewWord("doda", 2, tags, "1", doc2, (float).5);
		db.insertNewWord("gelesh", 2, tags, "7", doc2, (float).5);
		db.insertNewWord("yosry", 2, tags, "10", doc2, (float).5);
		
*/
		
		
		
	}

}
