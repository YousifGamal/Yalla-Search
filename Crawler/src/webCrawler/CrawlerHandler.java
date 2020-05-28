package webCrawler;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class CrawlerHandler implements Runnable {

	private int MAX_No_of_Crawled_pages ;
	// private Set<String> VistedPages = new HashSet<String>();
	// private List<String> PagesToVisit = new LinkedList<String>();

	private Crawler_Db db;

	public CrawlerHandler(Crawler_Db db ,int num) {
		this.db = db;
		this.MAX_No_of_Crawled_pages = num;
	}

	public void StartCrawling() {

		while (db.Get_visted_pages() < MAX_No_of_Crawled_pages) { // check if we reach the limit
			String currentUrl;
			Crawler crawl = new Crawler();

			currentUrl = this.getnextUrl(0);

			boolean success = crawl.Crawl(currentUrl); // call crawl method
			if (success) {
				System.out.println("Thread " + Thread.currentThread().getName()+" returned with - > " + crawl.getLinks().size() + " links, and " +crawl.get_Images().size() + " Imgs from url -> " + currentUrl);
				db.Insert_Data(crawl.getLinks(), currentUrl, crawl.get_document() , crawl.get_Images());
			} else {
				System.out.println("error happen in crawler cannot fetch page");
			}

		}
		System.out.println("\n" +"Thread " + Thread.currentThread().getName()+ " **Done** Visited " + db.Get_visted_pages() + " web page(s)");
		
	}
	
	public void reCrawl() {
		db.remove_static();
		System.out.println("\n" +"Thread " + Thread.currentThread().getName()+ " Begin Crawling");

		while (db.Get_visted_pages() != db.Get_recrawled_pages()) { // check if we reach the limit
			String currentUrl;
			Crawler crawl = new Crawler();

			currentUrl = this.getnextUrl(1);

			boolean success = crawl.Crawl(currentUrl); // call crawl method
			if (success) {
				System.out.println("Thread " + Thread.currentThread().getName()+" returned with - > " + crawl.getLinks().size() + " links, and " +crawl.get_Images().size() + " Imgs from url -> " + currentUrl);
				db.Update_Data(currentUrl, crawl.get_document() , crawl.get_Images());
			} else {
				System.out.println("error happen in crawler cannot fetch page :" + currentUrl);
			}

		}
		System.out.println("\n" +"Thread " + Thread.currentThread().getName()+ " **Done** Updated " + db.Get_visted_pages() + " web page(s)");
		
	}


	public void setMAX_No_of_Crawled_pages(int mAX_No_of_Crawled_pages) {
		MAX_No_of_Crawled_pages = mAX_No_of_Crawled_pages;
	}

	private String getnextUrl(int type) {
		String nextUrl;
		if(type == 0) {
			nextUrl = db.Get_Next_URl();
		}
		else
		{
			nextUrl = db.Get_Crawled_URl();
		}

		return nextUrl;
	}

	public void run() {
		if (db.Get_visted_pages() < MAX_No_of_Crawled_pages) {
			StartCrawling();
		} else {
			reCrawl();
		}	

	}

}
