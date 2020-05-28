
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.jsoup.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

//System.out.println();

public class gitWordTag {
	public static String getPublishDate(Document doc) {
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
		if(dates.size()>0) {
			return dates.get(0);
		}
		return null;
	}
	
	public static String get_describtion(Document doc) {
		Elements metalinks = doc.select("meta[name=description]");
		//get the content of the tag
		if(metalinks.size() <= 0) {
			return null;
		}
		return metalinks.get(0).attr("content");
	} 
	



	public static void main(String[] args) throws IOException, ParseException {
		String fileName = "src/test.html";
        Document doc = Jsoup.parse(new File(fileName), "utf-8"); 
        String d =  get_describtion(doc);
        System.out.println(d);
        String text = doc.text();
        //Elements elements =  doc.getElementsContainingOwnText("italics");
        //Elements elements =  doc.getElementsContainingText("italics");
       // for(int i = 0; i<elements.size();i++) {
        //	//System.out.pr	intln(elements.get(i).tagName());
        //}
        
        //Elements metalinks = doc.select("time");
        System.out.println( getPublishDate(doc));
        Elements metalinks = doc.getElementsByAttribute("pubdate");
        Elements els = doc.getElementsByAttribute("itemprop");
        List<String> dates = new ArrayList<String>();
        if (metalinks.size() > 0) {
	        	
	        	//System.out.println(metalinks.get(0).text());
	        //String mete_description_string = metalinks.get(0).attr("content");
	        //String[] mete_description_array = mete_description_string.split("\\W+");
	        
	        //List<String> mete_description = new ArrayList<String>(Arrays.asList(mete_description_array));
	        for (int i = 0; i<metalinks.size(); i++) {
	        	if(metalinks.get(i).hasAttr("pubdate")) {
	        		dates.add(metalinks.get(i).attr("datetime"));
	        	}
	        }
        }
        if (els.size() > 0) {
        	
        	//System.out.println(metalinks.get(0).text());
        //String mete_description_string = metalinks.get(0).attr("content");
        //String[] mete_description_array = mete_description_string.split("\\W+");
        
        //List<String> mete_description = new ArrayList<String>(Arrays.asList(mete_description_array));
        for (int i = 0; i<els.size(); i++) {
        	if(els.get(i).attr("itemprop").equals("datePublished")) {
        		String s = els.get(i).attr("content");
        		s = s.substring(0, Math.min(s.length(), 10));
        		dates.add(s);
        	}
        }
    }
        
        for(String W: dates) {
        	String sDate1=W;
            Date date1=new SimpleDateFormat("yyyy-MM-dd").parse(sDate1);
            //System.out.println(sDate1+"\t"+date1);  
        	//System.out.println(W);
        }

        String ss = "-1 -2 11 22 105 -102";
        ArrayList<Integer> lst = new ArrayList<Integer>();
        for (String field : ss.split(" +")) {
            lst.add(Integer.parseInt(field));
            System.out.println(Integer.parseInt(field));}
        
 }
}
