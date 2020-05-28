package webCrawler;

import java.awt.Image;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Crawler {

	private Set<String> linksfound = new HashSet<String>(); // list to store all founded links
	private String URLDocument; // Document to store founded document
	private Set<Pair> Imglinks = new HashSet<Pair>(); // list to store all founded Images links
	private static final String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.112 Safari/535.1";
	public boolean Crawl(String url_string) { // this function take a url and get html document and extract links on
												// this page

		System.out.println("\n**Visiting** Received web page at " + url_string + " of size = " + url_string.length());

		Document htmlDocument = null;
		try {
			 htmlDocument  = Jsoup.connect(url_string).get();
			 //htmlDocument = Jsoup.connect(url_string).userAgent(USER_AGENT).referrer("http://www.google.com").get();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			print(e.getMessage() + "  in crawl function");
			return false;
		}
		
		URLDocument = htmlDocument.toString();
		//print(URLDocument);
		if (! URLDocument.contains("</html>"))
		{
			return false;
			
		}
		Elements links = htmlDocument.select("a[href]");
		Elements media = htmlDocument.body().getElementsByTag("img");
		if(links != null)
		{
			for (Element link : links) {
				
				String l = link.absUrl("href");
				
				
				if (l.isBlank() | !l.contains("https") | !l.contains("http") ) {
					continue;
				}

				else {
					linksfound.add(l);
				}

			}
		}
		
		
		if(media != null)
		{
			getImages(media);
		}
		

		return true;

	}

	public boolean check_Image_Area(String imgurl) {
		/*
		Image img = null;
		try {
			img = ImageIO.read(new URL(imgurl));
		} catch (MalformedURLException e1) {
			// e1.printStackTrace();
			return false;
		} catch (IOException e1) {

			// e1.printStackTrace();
			return false;
		}
		if (img != null) {
			// to get rid from the small , thin images
			if (img.getWidth(null) < 70 | img.getHeight(null) < 70) {
				return false;
			} else {
				return true;
			}
		}
		return false;*/
		
		InputStream stream;
		try {
			stream = new URL(imgurl).openStream();
			Object obj = ImageIO.createImageInputStream(stream);
			ImageReader reader = ImageIO.getImageReaders(obj).next();
			reader.setInput(obj);
			if(reader.getWidth(0) < 70 | reader.getHeight(0) < 70)
			{
				stream.close();
				return false;		
			}
			else
			{
				stream.close();
				return true;
			}
			
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			//print(e.getMessage());
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//print(e.getMessage());
			return false;
		} catch (NoSuchElementException e) {
			//print(e.getMessage());
			//print(imgurl + "      not found in images");
			return false;
		}
	}

	public void getImages(Elements media) {
		int counter = 0;
		Pair p;
		for (Element src : media) {

			String imgurl = src.attr("abs:src");
			String alt = src.attr("alt");
			if (imgurl.isBlank()) {
				imgurl = src.attr("abs:data-src");
			}

			if (imgurl == null | imgurl == "" | !imgurl.contains("https") | !imgurl.contains("http") | (alt.isBlank())) {
				/*
				 * print(imgurl + "   url"); print(" Bad URL -> " + alt ); print(" src -> " +
				 * src); print(" ");
				 */
				continue;
			} else {
				counter += 1;
				int lazy = imgurl.indexOf("/t_lazy");
				if (lazy != -1) {
					imgurl = imgurl.substring(0, lazy) + imgurl.substring(lazy + 7, imgurl.length());
				}
		
				int hdri = imgurl.indexOf("/{width}{hidpi}");
				if (hdri != -1) {
					imgurl = imgurl.substring(0, hdri) + "/480" + imgurl.substring(hdri + 15, imgurl.length());
				}
				
				int width = imgurl.indexOf("/{width}");
				if (width != -1) {
					imgurl = imgurl.substring(0, width) + "/700" + imgurl.substring(width + 8, imgurl.length());
				}

				if (alt.contains("<p>")) {

					Document doc = Jsoup.parse(alt);
					Element e = doc.select("p").first();
					alt = e.text();

				}
				
				if(imgurl.contains("{")){
					print(imgurl + "   invalid img url at thread -> " +Thread.currentThread().getName());
				 
				}
						
				if (imgurl.contains(".gif")) {

					continue;

				}
				
				if (!check_Image_Area(imgurl)) {

					continue;

				}
				
				

				if (alt.length() > 600) {
					alt = alt.substring(0, 600);

				}
				p = new Pair(imgurl, alt);
				Imglinks.add(p);

			}
		}
		print("taken imgs " + counter + " / " + media.size());

	}

	public Set<String> getLinks() { // this function gets all links found on by the crawler in this page
		// System.out.println(linksfound);
		return this.linksfound;
	}

	public String get_document() { // this function gets all links found on by the crawler in this page
		// System.out.println(URLDocument);
		return this.URLDocument;
	}

	public Set<Pair> get_Images() {
		return Imglinks;
	}

	static void print(Object s) {
		System.out.println(s); // helper fumction to print
	}

}
