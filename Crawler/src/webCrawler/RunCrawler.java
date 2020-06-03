package webCrawler;

import java.util.Scanner;

public class RunCrawler {
	final private static int MAX_No_of_Crawled_pages = 10080;

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

		Crawler_Db db = new Crawler_Db();
		db.connect(); // connect to database

		System.out.println(" Enter the number of threads you wish to Crawl with...");
		Scanner in = new Scanner(System.in);
		int num = in.nextInt();
		int begin = db.Get_visted_pages();
		
		System.out.println("Crawled pages = "+db.Get_visted_pages() + "  the goal to reach = " + MAX_No_of_Crawled_pages);
		
		long start = System.currentTimeMillis();
		Thread[] threads = new Thread[num];
		for (int i = 0; i < threads.length; i++) {
			threads[i] = new Thread(new CrawlerHandler(db, MAX_No_of_Crawled_pages));
			threads[i].setName(String.valueOf(i + 1));
			threads[i].start();
		}

		// wait for the threads running in the background to finish
		for (Thread thread : threads) {
			thread.join();
		}
		
		int finsh = db.Get_visted_pages() - begin ;
		long end = System.currentTimeMillis();
	      //finding the time difference and converting it into seconds
	    float sec = (end - start) / 1000F; 
	   // db.Set_no_Crawling();
	    db.close(); // close connection
		System.out.println("All threads joined program is terminating ... took " + sec +" seconds to finish " + finsh + " doc ");
		
	}

}
