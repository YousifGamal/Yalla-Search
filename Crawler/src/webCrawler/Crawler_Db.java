package webCrawler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class Crawler_Db {

	final static String Db_name = "testdb";
	final static String url1 = "jdbc:mysql://localhost:3306/" + Db_name;
	final static String user = "Gellesh";
	final static String password = "Gellesh123";
	final private int Total_Links = 7000;
	private  Connection c = null;
	private static Statement stmt = null;
	private Object o1 = new Object();
	private Object o2 = new Object();
	private int key;

	static void print(Object s) {
		System.out.println(s); // helper fumction to print
	}

	public void connect() {
		try {

			setC(DriverManager.getConnection(url1, user, password));
			if (getC() != null) {
				print("Thread " + Thread.currentThread().getName() + " Connected to the database " + Db_name);
			}

		} catch (SQLException ex) {
			print("An error occurred. Maybe user/password is invalid");
			ex.printStackTrace();
		}
	}

	public void close() {
		if (getC() != null) {
			try {
				getC().close();
				print("Thread " + Thread.currentThread().getName() + " Closed database connection " + Db_name);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				print(e.getMessage());
			}
		}
	}

	public  Connection getC() {
		return this.c;
	}

	public  void setC(Connection c) {
		this.c = c;
	}

	public int Get_recrawled_pages() {
		int num = 0;
		try {

			String sql = "SELECT Count(*) FROM Crawled_URLS where recrawled = true ;"; // get numbers of visited pages
			PreparedStatement pst = getC().prepareStatement(sql);
			ResultSet rs = pst.executeQuery();
			rs.next();
			num = rs.getInt(1);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return num;
	}
	
	public int Get_inqueue_pages() {
		int num = 0;
		try {

			String sql = "SELECT count( distinct URL) FROM Collected_URLS where taken = 0;"; // get numbers of visited pages
			PreparedStatement pst = getC().prepareStatement(sql);
			ResultSet rs = pst.executeQuery();
			rs.next();
			num = rs.getInt(1);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return num;
	}
	
	public int Get_visted_pages() {
		int num = 0;
		try {

			String sql = "SELECT Count(*) FROM Crawled_URLS ;"; // get numbers of visited pages
			PreparedStatement pst = getC().prepareStatement(sql);
			ResultSet rs = pst.executeQuery();
			rs.next();
			num = rs.getInt(1);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return num;
	}

	public void Insert_seeds() {
		String str;
		UrlChecked checker = new UrlChecked();
		str = "https://en.wikipedia.org/wiki/Main_Page";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://www.bbc.com/news";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://www.imdb.com/chart/top/";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://www.fifa.com/";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://www.goal.com/en";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://www.theguardian.com/news";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://www.imdb.com/search/name/?gender=male,female&ref_=rlm";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://www.billboard.com/charts";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://edition.cnn.com/world";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://www.uefa.com/";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://www.javatpoint.com/java-tutorial";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://www.cbc.ca/news";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://www.gsmarena.com/";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://www.apple.com";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://www.amazon.com/";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://abcnews.go.com/";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		str = "https://dmoz-odp.org";
		str = checker.Normalize_URl(str);
		Insert_Url(str);
		
		
	
	}

	public void Insert_data(String data) {
		try {

			String sql = "INSERT INTO test (data) VALUES (?);";

			PreparedStatement pst = getC().prepareStatement(sql); // prepare insert query

			pst.setString(1,data);
			pst.executeUpdate();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public void Insert_Url(String Url) {
		try {

			String sql = "INSERT INTO Collected_URLS (Url,Taken, Ref) VALUES (?,?,?);";

			PreparedStatement pst = getC().prepareStatement(sql); // prepare insert query

			pst.setString(1, Url);
			pst.setBoolean(2, false);
			pst.setInt(3, -1); // -1 indicate seeds
			pst.executeUpdate();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void Update_Data(String url, String htmldoc, Set<Pair> ImgUrls) {
		synchronized (o2) {

			try {
				String sql = "UPDATE Crawled_URLS SET Html_doc = ? ,Changed = true  WHERE (Url = ? );"; // update doc of
																										// current url

				PreparedStatement pst;
				pst = getC().prepareStatement(sql);
				pst.setString(1, htmldoc);
				pst.setString(2, url);
				int done = pst.executeUpdate();

				getC().setAutoCommit(false);
				Insert_ImgUrls(ImgUrls); // add images to db
				getC().commit();
				getC().setAutoCommit(true);
				if (done > 0) {
					//print("Update  " + done + " Record successfully " + "Thread " + Thread.currentThread().getName());
				}

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public void Insert_Data(Set<String> Urls, String url, String htmldoc, Set<Pair> ImgUrls) {
		synchronized (o2) {

			try {
				String sql = "SELECT Count(*) FROM Crawled_URLS WHERE Url = ? ;"; // check if this url is founded in
				// Crawled
				// urls
				PreparedStatement pst;
				pst = getC().prepareStatement(sql);
				pst.setString(1, url);
				ResultSet rs2 = pst.executeQuery();
				rs2.next();
				if (rs2.getInt(1) == 0) {
					Insert_Visted_Url(url, htmldoc); // add url and returned document in visited urls
					getC().setAutoCommit(false);
					
					if(Get_inqueue_pages() + Get_visted_pages() < Total_Links )
					{
						Insert_Urls(Urls); // add result of the crawled links to total links in db
					}
					Insert_ImgUrls(ImgUrls); // add images to db
					getC().commit();
					getC().setAutoCommit(true);

				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public void Insert_Urls(Set<String> Urls) {
		UrlChecked checker = new UrlChecked();
		try {

			String sql = "INSERT INTO Collected_URLS (Url,Taken, Ref) VALUES (?,?,?);";

			PreparedStatement pst = getC().prepareStatement(sql); // prepare insert query
			//c.setAutoCommit(false);

			for (String url : Urls) {
				if (url.length() > 600) {
					continue;
				}
				url = checker.Normalize_URl(url); // first Normalize_URl
				if(url.isBlank())
				{
					continue;
				}
				pst.setString(1, url);
				pst.setBoolean(2, false);
				pst.setInt(3, key);
				pst.executeUpdate();
			}
			//c.commit();
			//c.setAutoCommit(true);

			//print("Inserted ALL " + Urls.size() + " Records successfully");

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void Insert_Visted_Url(String url, String htmldoc) {

		try {

			String sql = "INSERT INTO Crawled_URLS (Url, Html_doc,changed,recrawled) VALUES (?,?,?,?);";

			PreparedStatement pst = getC().prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS); // prepare insert
																										// query

			pst.setString(1, url);
			pst.setString(2, htmldoc);
			pst.setBoolean(3, true);
			pst.setBoolean(4, false);
			int done = pst.executeUpdate();

			if (done > 0) {
				//print("Insert Visted URL successfully " + "Thread " + Thread.currentThread().getName());
			}

			ResultSet rs = pst.getGeneratedKeys();

			if (rs.next()) {
				key = rs.getInt(1);
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void Select_All_From_Collected_URLS() {
		try {

			String sql = "SELECT * FROM Collected_URLS WHERE Taken = false Order By ID ;";

			PreparedStatement pst = getC().prepareStatement(sql); // prepare select * query

			ResultSet rs = pst.executeQuery();

			while (rs.next()) {

				System.out.print(rs.getInt(1));
				System.out.print(": ");
				System.out.print(rs.getString(2));
				System.out.print(": ");
				System.out.println(rs.getBoolean(3));
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void Update_TakenURl(String url) {
		try {

			String sql = "UPDATE Collected_URLS SET Taken = true WHERE (Url = ? );";

			PreparedStatement pst = getC().prepareStatement(sql); // prepare insert query

			pst.setString(1, url);
			int done = pst.executeUpdate();

			if (done > 0) {
				//print("Update  " + done + " Record successfully " + "Thread " + Thread.currentThread().getName());
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String Get_Next_URl() {
		//print("Thread " + Thread.currentThread().getName() + " want to get nexturl");
		synchronized (o1) {
			//print("Thread " + Thread.currentThread().getName() + " entered to get nexturl");
			String Next_url = null;
			//String Org_url = null;
			UrlChecked checker = new UrlChecked();
			boolean allowed = false;
			boolean founded = false;
			try {

				String sql = "SELECT * FROM Collected_URLS WHERE Taken = false Order By ID ;"; // get not taken urls

				PreparedStatement pst = getC().prepareStatement(sql); // prepare select * query

				while (!founded) {
					ResultSet rs = pst.executeQuery();
					while (rs.next()) {
						Next_url = rs.getString(2);
						//Org_url = Next_url;
						allowed = checker.check_url_allowed(Next_url); // check if it doesn't violate robot.txt
						//Next_url = norm_url;
						if (!allowed) {
							//print("disallowed url -> " + Next_url);
							Update_TakenURl(Next_url);
							continue;
						}

						sql = "SELECT Count(*) FROM Crawled_URLS WHERE Url = ? ;"; // check if this url is founded in
																					// Crawled
																					// urls
						pst = getC().prepareStatement(sql);
						pst.setString(1, Next_url);
						ResultSet rs2 = pst.executeQuery();
						rs2.next();
						if (rs2.getInt(1) == 0) {
							//print("taken url ->  " + Next_url + " Thread " + Thread.currentThread().getName());
							founded = true;
							Update_TakenURl(Next_url);
							break;

						}
						//print("previous taken url -> " + Next_url);
						Update_TakenURl(Next_url);

					}
				}

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return Next_url;

		}
	}

	public void Update_Crawled_URL(String url) {
		try {
			String sql = "UPDATE Crawled_URLS SET recrawled = true  WHERE (Url = ? );";
			PreparedStatement pst = getC().prepareStatement(sql); // prepare insert query
			pst.setString(1, url);
			int done = pst.executeUpdate();

			if (done > 0) {
				//print("Update  " + done + " Record successfully " + "Thread " + Thread.currentThread().getName());
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String Get_Crawled_URl() {
		//print("Thread " + Thread.currentThread().getName() + " want to get nexturl");
		synchronized (o1) {
			//print("Thread " + Thread.currentThread().getName() + " entered to get nexturl");
			String Next_url = null;

			try {
				String sql = "SELECT Url FROM Crawled_URLS WHERE recrawled = false Order By Url_ID ;"; // get not
																										// RECRAWLED
																										// urls
				PreparedStatement pst = getC().prepareStatement(sql); // prepare select * query

				ResultSet rs = pst.executeQuery();
				rs.next();
				Next_url = rs.getString(1);
				Update_Crawled_URL(Next_url);

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				print(e.getMessage());
			}

			return Next_url;

		}
	}
	
	public void Delete_All_From_Collected_URLS() {
		try {

			String sql = "DELETE FROM Collected_URLS;";

			PreparedStatement pst = getC().prepareStatement(sql); // prepare select * query

			int done = pst.executeUpdate();

			if (done > 0) {
				//print("Delete " + done + " Records successfully");
			}

			sql = "ALTER TABLE testdb.Collected_URLS AUTO_INCREMENT = 1;";
			pst = getC().prepareStatement(sql);
			pst.executeUpdate();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void Delete_All_From_Crawled_URLS() {
		try {

			String sql = "DELETE FROM Crawled_URLS;";

			PreparedStatement pst = getC().prepareStatement(sql); // prepare select * query

			int done = pst.executeUpdate();

			if (done > 0) {
				//print("Delete " + done + " Records successfully");
			}

			sql = "ALTER TABLE testdb.Crawled_URLS AUTO_INCREMENT = 1;";
			pst = getC().prepareStatement(sql);
			pst.executeUpdate();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void Delete_All_From_Imgs_URLS() {
		try {

			String sql = " DELETE FROM Imgs_URLS; ";

			PreparedStatement pst = getC().prepareStatement(sql); // prepare select * query

			int done = pst.executeUpdate();

			if (done > 0) {
				//print("Delete " + done + " Records successfully");
			}
			sql = "ALTER TABLE testdb.Imgs_URLS AUTO_INCREMENT = 1;";
			pst = getC().prepareStatement(sql);
			pst.executeUpdate();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void Insert_ImgUrls(Set<Pair> Urls) {
		try {

			String sql = "INSERT INTO Imgs_URLS (Img_URL,ALT,New) VALUES (?,?,?);";

			PreparedStatement pst = getC().prepareStatement(sql); // prepare insert query
			//c.setAutoCommit(false);
			
			String sql2 = "SELECT Count(*) FROM Imgs_URLS WHERE Img_URL = ? ;"; // check if this url is taken before
			
			PreparedStatement pst2;
			pst2 = getC().prepareStatement(sql2);

			for (Pair Imgurl : Urls) {
				
				if(Imgurl.getImgURL().length() > 600)
					continue;
				
				pst2.setString(1, Imgurl.getImgURL());
				ResultSet rs2 = pst2.executeQuery();
				
				rs2.next();
				if (rs2.getInt(1) == 0) {
					pst.setString(1, Imgurl.getImgURL());
					pst.setString(2, Imgurl.getALT());
					pst.setBoolean(3, true);
					pst.executeUpdate();
				}

			}
			
			//c.commit();
			//c.setAutoCommit(true);

			//print("Inserted ALL Imgs Urls " + Urls.size() + " Records successfully");

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void remove_static()

	{
		try {
			String sql = "UPDATE Crawled_URLS SET recrawled = true  WHERE (Url like \"%wiki%\");"; 																					// recrawling
			PreparedStatement pst;
			pst = getC().prepareStatement(sql);
			int done = pst.executeUpdate();
			if (done > 0) {
				//print("Update  " + done + " Record successfully " + "Thread " + Thread.currentThread().getName());
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
		
	public void Set_no_Crawling() {
	
			try {
				String sql = "UPDATE Crawled_URLS SET recrawled = false  ;"; // reset url for another recrawling																					
				PreparedStatement pst;
				pst = getC().prepareStatement(sql);				
				int done = pst.executeUpdate();
				if (done > 0) {
					//print("Update  " + done + " Record successfully " + "Thread " + Thread.currentThread().getName());
				}

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	public void Reset() {
		Delete_All_From_Collected_URLS();
		Delete_All_From_Crawled_URLS();
		Delete_All_From_Imgs_URLS();
		Insert_seeds();
	}

	public String Get_doc(int id) {
		String doc = null;
		

		try {

			String sql = "SELECT Html_doc FROM Crawled_URLS WHERE Url_ID = ? ;";

			PreparedStatement pst = getC().prepareStatement(sql); // prepare select * query
			pst.setInt(1, id);

			ResultSet rs = pst.executeQuery();

			rs.next();

			doc = rs.getString(1);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return doc;
	}

	

	public static void main(String[] args) throws SQLException {
		
		Crawler_Db db = new Crawler_Db();

		db.connect();
		db.Reset();
		db.Insert_seeds();
		
		db.Set_no_Crawling();
		
		db.close();

	}

}
