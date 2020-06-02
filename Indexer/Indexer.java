import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;
import java.text.SimpleDateFormat;  
import java.util.Date;  

import org.jsoup.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
//System.out.println();
import org.jsoup.select.Elements;

public class Indexer implements Runnable{
	
	//private Document doc;
	private dataBase db;
	private String HTML;
	private int docID;
	private List<String> ALT;
	private List <Integer> imgIds;
	private boolean docFlag;
	static List<String> Stop_Words;
	
	
	public Indexer( dataBase db, String HTML, int docID,List<String> ALT, List <Integer> imgIds,boolean docFlag ) {
		this.db = db;
		this.HTML = HTML;
		this.docID =docID;
		this.ALT = ALT;
		this.imgIds = imgIds;
		this.docFlag = docFlag;
	}
	
	public void indexDocument()  {
		// read stop words from file
		//Stop_Words = Stop_Words();
		
		
		// Declare Stemmer Object
		Stemmer s = new Stemmer();
		
		
		//get html file
		//String fileName = "src/test.html";
		
		
		// Parse the file
        Document doc = null;
		
		doc = Jsoup.parse(this.HTML);
        List<String> words = parse_html(doc);
        List<String> description = get_meta_description_tag(doc);
        
        
        // Process the parsed data 
        //Tags order [title, h1, h2, strong, b, description]
        List<ArrayList<Boolean> > tags = new ArrayList<ArrayList<Boolean> >();
        int it = 0;
        int pos_index = 0;
        List<Integer> postions = new ArrayList<Integer>();
        while(it < words.size()) {
        	//extract word and delete it from list
        	String original = words.get(0);
        	words.remove(0);
        	//get lower case of the word
        	String lower  = original.toLowerCase();
        	
        	// if stop word : remove it (do nothing)
        	if(Stop_Words.contains(lower) == false) {
        		//get tags of the word
        		ArrayList<Boolean> tempTags = get_tags(doc,original);
        		tempTags.add(false); // for description
        		tags.add(tempTags);
        		
        		// stem the word
        		s.add(lower.toCharArray(),lower.length());
        		s.stem();
        		lower = s.toString();
        		words.add(lower);
        		it++;
        		postions.add(pos_index);
        	}
        	pos_index++;
        	//else do nothing
        }
        //System.out.println(postions.size());
        //System.out.println(words.size());
        
        if(description != null) {
        	while(description.size() > 0 ) {
        		//extract word and delete it from list
            	String original = description.get(0);
            	description.remove(0);
            	//get lower case of the word
            	String lower  = original.toLowerCase();
            	
            	// if stop word : remove it (do nothing)
            	// if stop word : remove it (do nothing)
            	if(Stop_Words.contains(lower) == false) {
            		//get tags of the word
            		ArrayList<Boolean> tempTags = get_tags(doc,original);
            		tempTags.add(true); // for description
            		tags.add(tempTags);
            		
            		// stem the word
            		s.add(lower.toCharArray(),lower.length());
            		s.stem();
            		lower = s.toString();
            		words.add(lower);
            		postions.add(-1);
            	}
            	//else do nothing
        	}
        }
        
        //Now we have all stemmed words in words and all their tags in tags (in same order)
        
        //prepare the final output to be written to the data base
        int total_words = words.size();
        List<String> finalWords = new ArrayList<String>();
        List<String> finalPosition = new ArrayList<String>();
        List<ArrayList<Boolean> > finalTags = new ArrayList<ArrayList<Boolean> >();
        List<Integer> occurrences = new ArrayList<Integer>();
        List<Float> df = new ArrayList<Float>();
        for(int i =0; i<words.size(); i++) {
        	String w = words.get(i);
        	ArrayList<Boolean> t = tags.get(i);
        	int index = finalWords.indexOf(w);
        	if(index == -1) {//new entry
        		finalWords.add(w);
        		finalTags.add(t);
        		finalPosition.add(String.valueOf(postions.get(i)));
        		occurrences.add(1);
        	}
        	else { // get the entry or the tags and add the new position
        		// update tags
        		ArrayList<Boolean> oldT =  finalTags.get(index);
        		for(int j = 0; j<oldT.size(); j++) {
        			if(oldT.get(j) || t.get(j)) {
        				t.set(j,true);
        			}
        		}
        		finalTags.set(index, t);
        		
        		//update position
        		String pos = finalPosition.get(index);
        		pos = pos+" "+String.valueOf(postions.get(i));
        		finalPosition.set(index, pos);
        		
        		//increment occurrences 
        		int count = occurrences.get(index);
        		count+=1;
        		occurrences.set(index, count);
        		
        	}
        
        
        }
        
        for(int i = 0; i < finalWords.size(); i++) {
        	float temp = (float)occurrences.get(i)/total_words;
        	df.add(temp);        	
        	}

        String publishedDate = getPublishDate(doc);
        String pageDescription = get_describtion(doc);
        String pageTitle = get_title(doc);
        
        boolean newDoc = false;
		try {
			System.out.println("Document = "+String.valueOf(docID)+" parsing "+String.valueOf(total_words));
			if(total_words > 0) {
			newDoc = db.insertNewDocument(docID, total_words,finalWords, finalTags, finalPosition, df,publishedDate,pageDescription,pageTitle);
			}
			//db.testFn(finalWords, docID, finalTags, finalPosition, newDoc, df);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        /*for(int i = 0; i < finalWords.size(); i++) {
        try {
			db.insertNewWord(finalWords.get(i), docID, finalTags.get(i), finalPosition.get(i), newDoc, df.get(i));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
    	//System.out.println(finalWords.get(i));
    	//System.out.println(df.get(i));
    	//System.out.println(finalPosition.get(i));
    	//System.out.println(finalTags.get(i).toString());
        	
    	
    	}*/
        System.out.println("Document = "+String.valueOf(docID)+" is done");
        


	}
	
	public void indexImage() throws SQLException {	
		// Declare Stemmer Object
		Stemmer s = new Stemmer();
		List<ArrayList<String>> total_words = new ArrayList<ArrayList<String>>();
		
		
		for(int k = 0; k<ALT.size(); k++) {
		
			String[] all = ALT.get(k).split("\\W+");
	        List<String> words = new ArrayList<String>(Arrays.asList(all));
	        
	        
	        // Process the parsed data 
	        int it = 0;
	        while(it < words.size()) {
	        	//extract word and delete it from list
	        	String original = words.get(0);
	        	words.remove(0);
	        	//get lower case of the word
	        	String lower  = original.toLowerCase();
	        	
	        	// if stop word : remove it (do nothing)
	        	if(Stop_Words.contains(lower) == false) {
	        		
	        		// stem the word
	        		s.add(lower.toCharArray(),lower.length());
	        		s.stem();
	        		lower = s.toString();
	        		words.add(lower);
	        		it++;
	        	}
	        	//else do nothing
	        }
	        
	        
	        
	        //Now we have all stemmed words in words and all their tags in tags (in same order)
	        
	        //prepare the final output to be written to the data base
	        ArrayList<String> finalWords = new ArrayList<String>();
	        for(int i =0; i<words.size(); i++) {
	        	String w = words.get(i);
	        	int index = finalWords.indexOf(w);
	        	if(index == -1) {//new entry
	        		finalWords.add(w);
	        		
	        	}
	        
	        }
	        total_words.add(finalWords);
		}
		
		//Add to data base
		db.insertImages(this.imgIds, total_words);
		System.out.println(String.valueOf(imgIds.size())+" images are done");

	}
	
	
	
	public void run() {
		if(this.docFlag) {
			indexDocument();
		}
		else {
			try {
				indexImage();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	
	// convert html in string list
	public List<String> parse_html(Document doc){
		//get all text of html (not tags attributes)
		String text = doc.text();
		//divide them two words and put them into list
        String[] all = text.split("\\W+");
        List<String> words = new ArrayList<String>(Arrays.asList(all));
        return words; 
	}

	// get tags of a word
	public ArrayList<Boolean> get_tags(Document doc, String word)  {
		Elements elements =  doc.getElementsContainingOwnText(word);
        //Elements elements =  doc.getElementsContainingText("italics");
		ArrayList<Boolean> tags = new ArrayList<Boolean>();
		ArrayList<String> allTags = new ArrayList<String>(); 
        for(int i = 0; i<elements.size();i++) {
        	allTags.add(elements.get(i).tagName());
        }
		//title tag
        if (allTags.contains("title")) {
			tags.add(true);
		}
		else {
			tags.add(false);
		}
      //h1 tag
        if (allTags.contains("h1")) {
			tags.add(true);
		}
		else {
			tags.add(false);
		}
      //h2 tag
        if (allTags.contains("h2")) {
			tags.add(true);
		}
		else {
			tags.add(false);
		}
      //strong tag
        if (allTags.contains("strong")) {
			tags.add(true);
		}
		else {
			tags.add(false);
		}
      //b tag
        if (allTags.contains("b")) {
			tags.add(true);
		}
		else {
			tags.add(false);
		}
      return tags;
		
		
	}

	public List<String> get_meta_description_tag(Document doc){
		//search for meta-description tag
		Elements metalinks = doc.select("meta[name=description]");
		//get the content of the tag
		if(metalinks.size() <= 0) {
			return null;
		}
        String mete_description_string = metalinks.get(0).attr("content");
        //divide the string into list of strings
        String[] mete_description_array = mete_description_string.split("\\W+");
        List<String> mete_description = new ArrayList<String>(Arrays.asList(mete_description_array));
        return mete_description;
	}
	public String get_describtion(Document doc) {
		Elements metalinks = doc.select("meta[name=description]");
		//get the content of the tag
		if(metalinks.size() <= 0) {
			return null;
		}
		return metalinks.get(0).attr("content");
	} 
	
	public String get_title(Document doc) {
		Elements metalinks = doc.select("title");
		//get the content of the tag
		if(metalinks.size() <= 0) {
			return null;
		}
		return metalinks.get(0).text();
	}

	// Read stop words from a file
	public static List<String> Stop_Words() throws FileNotFoundException{
		
		File file = new File("src/stop_words.txt");
		Scanner fscn = new Scanner(file);
		
		List<String> stopWords = new ArrayList<String>();

		while(fscn.hasNext()) {
			//skip triangle wordScanner 
			stopWords.add(fscn.nextLine());
		}
		fscn.close();
		/*for(String s: stopWords) {
			System.out.println(s);
		}*/
		System.out.println(stopWords.size());
		return stopWords;
	}
	
	
	//get date from the document
	public String getPublishDate(Document doc) {
		// search for all tags that have attributes pubdate or itemprop
		Elements timeTags = doc.getElementsByAttribute("pubdate");
        Elements divs = doc.getElementsByAttribute("itemprop");
        List<String> dates = new ArrayList<String>();
        if (divs.size() > 0) {
	        for (int i = 0; i<divs.size(); i++) {
	        	//if itemprop attribute of this tag is itemprop = "datePublished"
	        	if(divs.get(i).attr("itemprop").equals("datePublished")) {
	        		//then get date as content="2021-7-1"
	        		String s = divs.get(i).attr("content");
	        		s = s.substring(0, Math.min(s.length(), 10));
	        		dates.add(s);
	        	}
	        }
        }
        
        if (timeTags.size() > 0) {
	        for (int i = 0; i<timeTags.size(); i++) {
	        	if(timeTags.get(i).hasAttr("pubdate")) {
	        		// get date as attrb datetime = "2021-7-1"
	        		String s =timeTags.get(i).attr("datetime");
	        		s = s.substring(0, Math.min(s.length(), 10));
	        		dates.add(s);
	        	}
	        }
        }
        // if no dates found will return empty string
		for(int i = 0; i< dates.size(); i++) {
			if (dates.get(i).length() == 10 ) {
				return dates.get(i);
			}
		}
		return null;
	}
		
	public static void main(String[] args) throws FileNotFoundException, InterruptedException, SQLException {
		Stop_Words = Stop_Words();
		dataBase docDB = new dataBase("testdb");
		dataBase imgDB = new dataBase("testdb");
		dataBase db = new dataBase("testdb");
		List<Object> docs= new ArrayList<Object>();
		List<Object> imgs= new ArrayList<Object>();
		int maxIterations = 100;
		
		int it = 0;
		boolean noDocs = false;
		boolean noImgs = false;
		long start = System.currentTimeMillis();
		while(( (noDocs == false) || (noImgs == false) ) && it<maxIterations ) {
			it++;
			List<Thread> threads = new ArrayList<Thread>();
			List<Integer> ids = new ArrayList<Integer>();
			List<String> htmls = new ArrayList<String>();
			List<Integer> imgIds = new ArrayList<Integer>();
			List<String> ALTs = new ArrayList<String>();
			//get 10 images 
			imgs = docDB.getImages();
			if(imgs.size() <= 0) {
				noImgs = true;
			}
			for(int j = 0; j<imgs.size(); j+=2) {
				imgIds.add((int)imgs.get(j));
				ALTs.add((String)imgs.get(j+1));
			}
			//make thread for the 10 images
			threads.add(new Thread (new Indexer(imgDB,null,0,ALTs,imgIds,false)));
			
			//get 10 documents
			docs =  docDB.getDocuments();
			if(docs.size() <= 0) {
				noDocs = true;
			}
			for(int j = 0; j<docs.size(); j+=2) {
				ids.add((int)docs.get(j));
				htmls.add((String)docs.get(j+1));
			}
			//make thread for each doc
			for(int k = 0; k<ids.size(); k++) {
				System.out.println(ids.get(k));
				threads.add(new Thread (new Indexer(db,htmls.get(k),ids.get(k),null,null,true)));
			}
			//start all threads
			for(int k = 0; k<threads.size(); k++) {
				threads.get(k).start();
			}
			//join all threads
			for(int k = 0; k<threads.size(); k++) {
				threads.get(k).join();
			}
		}
		long end = System.currentTimeMillis();
	      //finding the time difference and converting it into seconds
	    float sec = (end - start) / 1000F;
	    System.out.println("All threads joined program is terminating ... took " + sec +" seconds");
		
		//test case
		
		

	}

}
