package project;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
 
 
public class MySqlTest {
 
    final static String Db_name = "testdb";
    final static String url1 = "jdbc:mysql://localhost:3306/" + Db_name;
    final static String user = "dawood";
    final static String password = "dawood123";
    private static Connection c = null;
    private static Statement stmt = null;
    static String update_sql = "update documents set pageRank = ? where documentID = ?";
    static PreparedStatement update_batch_st; 
 
    public static void connect() {
        try {
 
            c = DriverManager.getConnection(url1, user, password);
            if (c != null) {
                System.out.println("Connected to the database " + Db_name);
                update_batch_st = c.prepareStatement(update_sql);
            }
 
        } catch (SQLException ex) {
            System.out.println("An error occurred. Maybe user/password is invalid");
            ex.printStackTrace();
        }
    }
 
    public static void print(Object s) {
        System.out.println(s);
    }
 
 
    public static ResultSet Select_Doc_Rel() {
        try {
 
            String sql = "select distinct cl.ref,cw.Url_ID from Collected_URLS cl\n" + 
            		"inner join Crawled_URLS cw\n" + 
            		"on cl.Url = cw.Url\n" + 
            		"where cl.ref<>cw.Url_ID\n" + 
            		"and cl.ref <> -1\n" + 
            		"and cl.Taken = 1;";
 
            PreparedStatement pst = c.prepareStatement(sql); // prepare select * query
 
            ResultSet rs = pst.executeQuery();
            return rs;
 
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		return null;
 
    } 
    
    public static ResultSet Get_Docs_Rels_Count() {
        try {
 
            String sql = "select count(*) from documents_relations;";
 
            PreparedStatement pst = c.prepareStatement(sql); // prepare select * query
 
            ResultSet rs = pst.executeQuery();
            return rs;
 
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		return null;
 
    } 
    
    public static ResultSet Select_Docs() {
        try {
 
            String sql = "Select documentID,pageRank from documents;";
 
            PreparedStatement pst = c.prepareStatement(sql,
            		ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE); // prepare select * query
 
            ResultSet rs = pst.executeQuery();
            return rs;
 
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		return null;
 
    } 
    
    public static ResultSet Get_Docs_Count() {
        try {
 
            String sql = "Select count(*) from documents;";
 
            PreparedStatement pst = c.prepareStatement(sql
            		,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
 
            ResultSet rs = pst.executeQuery();
            return rs;
 
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		return null;
     }
    
    public static ResultSet Select_Words(ArrayList<String> searchWords) {
        try {
 
            String sql = "select si.wordID,si.documentID,si.title,si.h1,si.h2,si.strong,si.b,si.description,si.df from words w\n" + 
            		"inner join searchIndex si\n" + 
            		"on w.wordID = si.wordID\n" + 
            		"where word in (";
            
            for(int i = 0; i<searchWords.size(); i++) {
            	if(i != searchWords.size() - 1) {
            		sql += "'" + searchWords.get(i) + "' ,";
            	}else {
            		sql += "'" + searchWords.get(i) + "');";
            	}
            }
 
            PreparedStatement pst = c.prepareStatement(sql
            		,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
 
            ResultSet rs = pst.executeQuery();
            return rs;
 
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		return null;
     }
    
    
    public static ResultSet Select_Phrase(ArrayList<String> searchWords) {
        try {
 
            String sql = "select si.wordID,si.documentID,si.title,si.h1,si.h2,si.strong,si.b,si.description,si.df,si.positions,word from words w\n" + 
            		"inner join searchIndex si\n" + 
            		"on w.wordID = si.wordID\n" + 
            		"where word in (";
            
            for(int i = 0; i<searchWords.size(); i++) {
            	if(i != searchWords.size() - 1) {
            		sql += "'" + searchWords.get(i) + "' ,";
            	}else {
            		sql += "'" + searchWords.get(i) + "') ";
            	}
            }
            
            sql += "and si.documentID in (\n" + 
            		"SELECT si.documentID FROM words w\n" + 
            		"inner join searchIndex si\n" + 
            		"on w.wordID = si.wordID\n" + 
            		"WHERE word IN (";
            
            
            for(int i = 0; i<searchWords.size(); i++) {
            	if(i != searchWords.size() - 1) {
            		sql += "'" + searchWords.get(i) + "' ,";
            	}else {
            		sql += "'" + searchWords.get(i) + "') ";
            	}
            }
            
            sql += "GROUP BY si.documentID \n" + 
            		"HAVING COUNT(distinct(word)) = "+ searchWords.size() + ");";
 
            PreparedStatement pst = c.prepareStatement(sql
            		,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
 
            
            ResultSet rs = pst.executeQuery();
            return rs;
 
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		return null;
     }
    
    
    public static ResultSet Select_Docs_Phrase(ArrayList<String> searchWords) {
        try {
 
            String sql = "select documentID,pageRank,cu.Url,title,description,CURDATE()-publishedDate as days from documents d\n" + 
            		"inner join Crawled_URLS cu\n" + 
            		"on d.documentID = cu.Url_ID\n" + 
            		"where documentID in (\n" + 
            		"SELECT si.documentID FROM words w\n" + 
            		"inner join searchIndex si\n" + 
            		"on w.wordID = si.wordID\n" + 
            		"WHERE word IN (";
            
            for(int i = 0; i<searchWords.size(); i++) {
            	if(i != searchWords.size() - 1) {
            		sql += "'" + searchWords.get(i) + "' ,";
            	}else {
            		sql += "'" + searchWords.get(i) + "') ";
            	}
            }
            
            sql += "GROUP BY si.documentID \n" + 
            		"HAVING COUNT(distinct(word)) = "+ searchWords.size() + ");";
            PreparedStatement pst = c.prepareStatement(sql
            		,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
 
            ResultSet rs = pst.executeQuery();
            return rs;
 
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		return null;
     }
    
    public static ResultSet Select_Words_Counts(ArrayList<String> searchWords) {
        try {
 
            String sql = "select wordID,documentsNumber,word from words \n" + 
            		"where word in (";
            
            for(int i = 0; i<searchWords.size(); i++) {
            	if(i != searchWords.size() - 1) {
            		sql += "'" + searchWords.get(i) + "' ,";
            	}else {
            		sql += "'" + searchWords.get(i) + "');";
            	}
            }

            PreparedStatement pst = c.prepareStatement(sql
            		,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
 
            ResultSet rs = pst.executeQuery();
            return rs;
 
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		return null;
     }
    
    public static ResultSet Select_Docs_Of_Words(ArrayList<String> searchWords) {
        try {
 
            String sql = "select distinct(d.documentID),d.pageRank,cu.Url,d.title,d.description,CURDATE()-publishedDate as days from words w\n" + 
            		"inner join searchIndex si\n" + 
            		"on w.wordID = si.wordID\n" + 
            		"inner join documents d\n" + 
            		"on si.documentID = d.documentID\n" + 
            		"inner join Crawled_URLS cu\n" + 
            		"on d.documentID = cu.Url_ID\n" + 
            		"where word in (";
            
            for(int i = 0; i<searchWords.size(); i++) {
            	if(i != searchWords.size() - 1) {
            		sql += "'" + searchWords.get(i) + "' ,";
            	}else {
            		sql += "'" + searchWords.get(i) + "');";
            	}
            }
            
            PreparedStatement pst = c.prepareStatement(sql
            		,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
 
            ResultSet rs = pst.executeQuery();
            return rs;
 
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		return null;
     }
    
    
    public static ResultSet Select_Imgs(ArrayList<String> searchWords) {
        try {
 
            String sql = "select imu.Img_URL from imageIndex imx\n" + 
            		"inner join Imgs_URLS imu\n" + 
            		"on imx.imageID = imu.ID\n" + 
            		"where word in(";
            
            for(int i = 0; i<searchWords.size(); i++) {
            	if(i != searchWords.size() - 1) {
            		sql += "'" + searchWords.get(i) + "' ,";
            	}else {
            		sql += "'" + searchWords.get(i) + "');";
            	}
            }
            PreparedStatement pst = c.prepareStatement(sql
            		,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
 
            ResultSet rs = pst.executeQuery();
            return rs;
 
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		return null;
     }
    
    public static ResultSet Get_Word_Count(ArrayList<String> searchWords) {
        try {
 
            String sql = "select count(distinct(t.word)) from (\n" + 
            		"select word from words w\n" + 
            		"where word in (";
            
            for(int i = 0; i<searchWords.size(); i++) {
            	if(i != searchWords.size() - 1) {
            		sql += "'" + searchWords.get(i) + "' ,";
            	}else {
            		sql += "'" + searchWords.get(i) + "'))as t;";
            	}
            }
 
            PreparedStatement pst = c.prepareStatement(sql
            		,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
 
            ResultSet rs = pst.executeQuery();
            return rs;
 
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		return null;
     }
    
    public static void update_Prank(int id, float p_rank, Boolean excu) throws SQLException {
    	if(!excu) {
    		update_batch_st.setInt(2, id);
    		update_batch_st.setFloat(1, p_rank);
    		update_batch_st.addBatch(); 
    	}
    	else {
    		update_batch_st.executeBatch();
    	}
 
    }
}

	
