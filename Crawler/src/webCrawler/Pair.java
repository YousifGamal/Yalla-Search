package webCrawler;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class Pair {

	private String ImgURL;
	private String ALT;

	public Pair(String ImgURL, String ALT) {
		this.setImgURL(ImgURL);
		this.setALT(ALT);
	}

	public String getImgURL() {
		return ImgURL;
	}

	public void setImgURL(String imgURL) {
		ImgURL = imgURL;
	}

	public String getALT() {
		return ALT;
	}

	public void setALT(String aLT) {
		ALT = aLT;
	}

	public void print() {
		System.out.println(this.getImgURL() + " " + this.getALT());
	}

	@Override
	public boolean equals(Object obj) {

		// if both the object references are
		// referring to the same object.
		if (this == obj)
			return true;

		// it checks if the argument is of the
		// type Geek by comparing the classes
		// of the passed argument and this object.
		// if(!(obj instanceof Geek)) return false; ---> avoid.
		if (obj == null || obj.getClass() != this.getClass())
			return false;

		// type casting of the argument.
		Pair pair = (Pair) obj;

		// comparing the state of argument with
		// the state of 'this' Object.
		return (pair.getImgURL().equals(this.getImgURL()));
		//return (pair.getImgURL().equals(this.getImgURL()) && pair.getALT().equals(this.getALT()));
	}

	@Override
	public int hashCode() {

		/*
		 * int result = 17; result = 31 * result + ImgURL.hashCode(); result = 31 *
		 * result + ALT.hashCode(); return result;
		 */
		return Objects.hash(getImgURL());
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Set<Pair> Imglinks = new HashSet<Pair>(); // list to store all founded Images links
		Pair one = new Pair("url1", "alt1");
		Pair two = new Pair("url1", "alt1dd");
		Imglinks.add(one);
		Imglinks.add(two);
		for (Pair p : Imglinks) {
			p.print();
		}


		Set<String> linksfound = new HashSet<String>();
		linksfound.add("url1");
		linksfound.add("url1");
		for (String p : linksfound) {
			System.out.println(p);
		}

	}

}
