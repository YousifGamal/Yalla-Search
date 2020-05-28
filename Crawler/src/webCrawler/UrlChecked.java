package webCrawler;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;

import java.net.URL;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class UrlChecked {

	//final static String disurl1 = "https://calendar.google.com/calendar/r";
	//final static String disurl2 = "https://google.com/maps/preview";
	static String domain = null;
	static String path = null;
	static String path_abs = null;

	
	static  void print(Object s) {
		System.out.println(s);   // helper fumction to print
	}

	public  String Normalize_URl(String url)  // normalization function
	{
		/*
		 *  some added normalizing
		 *  
		 * 1- space produce errors "http://www.example.com/some space" -> done 
		 * 2- add / at end -> done 
		 * 3- %20 should stay as its   bt2lb space  
		 * 4 - Removing directory index -> done
		 * 5 - limit the protocol -> done 
		 * 6 - remove all after ?   # problem in normalize function el mfrod nzbtha 
		 * 7- remove .. at first  -> http://www.example.com/../a/b/../c/./d.html -> done 
		 * 
		 */


		
		url = url.replaceAll("\\s", ""); // remove spaces in urls
		//System.out.println(url);
		String protocol = null;
		String host = null;
		String path = null;
		try {
			 protocol = URI.create(url).normalize().getScheme().toLowerCase();
			 host = URI.create(url).normalize().getHost().toLowerCase();
			 path = URI.create(url).normalize().getPath();
		}
		catch ( Exception e)
		{
			print(e + "error in normlizing  + on this string -> " + url);
			return "";
		}
		// System.out.println(host);
		 //System.out.println(path);
		//System.out.println(protocol);
		
		
		if(url.contains("?"))  // Sorting the query parameters
		{
			if(url.endsWith("?"))
			{
				path = path + "?";
			}
			if(url.endsWith("="))
			{
				path = path + "?";
			}
			//System.out.print(url.split("l",2));
			path = path + "?" +url.split(Pattern.quote("?"),2)[1];
			
			
			
		}

		if (protocol.equals("https")) // limit the protocol
		{
			protocol = "http";
		}
		if (path.startsWith("/../")) // remove .. at first
		{
			path = path.substring(3);
		}

		if (path.endsWith("default.asp") | path.endsWith("index.html")) // Removing directory index
		{
			path = path.replace("default.asp", "");
			path = path.replace("index.html", "");
		}
		/*if (!path.endsWith("/") & !path.endsWith("?") ) // make all urls end with /
		{
			path = path + "/";
		}*/

		String newurl = protocol + "://" + host + path;

		//System.out.println(newurl);
		
		return newurl;

	}

	public  int getDomainName(String url_string) {  // set domain and path 
		URL url = null;
		try {
			url = new URL(url_string);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			print("Error at making url to get domain name  url is -> " + url);
			return -1;
		}
		domain = url.getHost();
		path_abs = url.getPath();
		path = url.getPath();
		int path_start = url_string.indexOf(path);
		path = url_string.substring(path_start,url_string.length());

		return 1;
	}

	public  boolean Read_Robot_file()   // get robot.txt file to check if url is allowed to be visted

	{

		boolean can_crawl = true;

		try {
			Document doc = Jsoup.connect("https://" + domain + "/robots.txt").get();
			String docBody = doc.body().text().toLowerCase();
			// System.out.println(docBody + " before split");
			if(docBody.isEmpty())
			{
				return true;
			}
			
			
			String [] str;
			str = docBody.split("user-agent: " + Pattern.quote("*"));
			if(str.length > 1)
			{
				//print("true");
				//System.out.println(docBody + " before split");
				docBody = str[1];
				if(docBody.contains("user-agent:"))
				{
					str = docBody.split("user-agent:");
					if(str.length > 0)
						docBody = str[0];// split to get for user-agent: * only
				}
			}
			
			docBody = docBody + "    ";
			//print(docBody);
			//print(docBody.length());
			//print(path);
			String path1 = path;
			String path2 = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
			
			
			if(path2.contains("?") & !path2.endsWith("?"))
			{
				//print(path_abs);
				int check_disallow = docBody.indexOf("disallow: " + path_abs + " ");
				if(check_disallow !=-1)
					return false;
			}
			
			boolean check_disallowall = docBody.contains("disallow: /  ");
			
			if(check_disallowall)
			{
				return false;
			}
			int check_disallow1 = docBody.indexOf(" disallow: " + path1 + " ");
			int check_disallow2 = docBody.indexOf(" disallow: " + path2 + " ");
			int check_allow1 = docBody.indexOf(" allow: " + path1 + " ");
			int check_allow2 = docBody.indexOf(" allow: " + path2 + " ");
			if (check_disallow1 != -1 | check_disallow2 != -1) {
			
				if (check_allow1 != -1 | check_allow2 != -1) {

					can_crawl = true;
				} else {
					can_crawl = false;
				}
			}

			// System.out.println(can_crawl);
			// System.out.println(docBody);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//print(e.getMessage());
			if(e.getMessage() == "HTTP error fetching URL") // cannot get robots.txt or it doesn't exist
			{
				return true;
			}
			return false;
		}

		return can_crawl;
	}

	

	public boolean check_url_allowed(String norm_url) {   				// this function calls the above functions 
		//String norm_url = Normalize_URl(url); // first Normalize_URl
		getDomainName(norm_url); // get domain name
		//print(domain);
		if (domain == "-1" | domain == null)
			return false;
		boolean success = Read_Robot_file(); // check if we can crawl this url
/*		if (success) {
			print("we can crawl this url -> " + norm_url);
		} else {
			print("we cannot crawl this url -> " + norm_url);
		}*/

		return success;

	}



}
