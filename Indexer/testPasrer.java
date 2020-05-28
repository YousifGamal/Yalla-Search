

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.*;

import org.jsoup.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
//System.out.println();
import org.jsoup.select.Elements;







public class testPasrer {
	public static List<String> Stop_Words ;
	public static void main(String[] args) throws IOException,FileNotFoundException {
		
		// read stop words from file
		Stop_Words = Stop_Words();
		
		
		// Declare Stemmer Object
		Stemmer s = new Stemmer();
		
		
		//get html file
		String fileName = "src/test.html";
		
		
		// Parse the file
        Document doc = Jsoup.parse(new File(fileName), "utf-8");
        List<String> words = parse_html(doc);
        List<String> description = get_meta_description_tag(doc);
        
        
        // Process the parsed data 
        //Tags order [title, h1, h2, strong, b, description]
        List<ArrayList<Boolean> > tags = new ArrayList<ArrayList<Boolean> >();
        int it = 0;
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
        	}
        	//else do nothing
        }
        
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
        		finalPosition.add(String.valueOf(i));
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
        		pos = pos+" "+String.valueOf(i);
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

        	
        
        for(int i = 0; i < finalWords.size(); i++) {
    	System.out.println(finalWords.get(i));
    	System.out.println(df.get(i));
    	System.out.println(finalPosition.get(i));
    	System.out.println(finalTags.get(i).toString());
    	
    	}
        


	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////
	// convert html in string list
	public static List<String> parse_html(Document doc){
		//get all text of html (not tags attributes)
		String text = doc.text();
		//divide them two words and put them into list
        String[] all = text.split("\\W+");
        List<String> words = new ArrayList<String>(Arrays.asList(all));
        return words; 
	}

	
	
	
	public static List<String> get_meta_description_tag(Document doc){
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
	
	
	// get tags of a word
	public static ArrayList<Boolean> get_tags(Document doc, String word)  {
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

}
