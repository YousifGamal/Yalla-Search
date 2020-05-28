package project;
import static java.util.stream.Collectors.toMap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;



public class PageRanker {
	float pageRankScore = 0; 
	float pageRankScorePrev = 0;
	float d = (float) 0.85;	
	
	public void pagePopularity() throws SQLException{
		
		ResultSet docs_rels = MySqlTest.Select_Doc_Rel();
		ResultSet docs = MySqlTest.Select_Docs();
		HashMap<Object, Float> docRank = new HashMap<Object, Float>();
		Map<Integer, List<Integer>> docPointers = new HashMap<Integer, List<Integer>>();
		Map<Integer, Integer> doc_out_size = new HashMap<Integer, Integer>();
		
		int doc_count = 0;
		while(docs.next()) {
			doc_count += 1;
		}
		int[] docs_ids = new int[doc_count];
		docs.first();
		for(int i = 0; i < doc_count; i++){
			docRank.put(docs.getInt(1), docs.getFloat(2));
			docs_ids[i] = docs.getInt(1);
			docs.next();
		}
			
		while(docs_rels.next()) {
			if(docRank.get(docs_rels.getInt(2)) != null && docRank.get(docs_rels.getInt(1)) != null) {
				if (docPointers.get(docs_rels.getInt(2)) == null) {
					docPointers.put(docs_rels.getInt(2), new ArrayList<Integer>());
				}
				docPointers.get(docs_rels.getInt(2)).add(docs_rels.getInt(1));
				if (doc_out_size.get(docs_rels.getInt(1)) == null) {
					doc_out_size.put(docs_rels.getInt(1), 0);
				}
				doc_out_size.put(docs_rels.getInt(1), doc_out_size.get(docs_rels.getInt(1)) + 1);
			}
		}
				
	
		Boolean finished = false;
		float tol = (float) 0.1;
		
		int iterations = 0;
		while(!finished) {
			finished  = true;
			for (int indx = 0; indx<doc_count; indx++) {
				pageRankScorePrev = (float) docRank.get(docs_ids[indx]);
				pageRankScore = (float) docRank.get(docs_ids[indx]);
				if(docPointers.get(docs_ids[indx]) != null) {
					int curr = 0;
					for (int i =0; i<docPointers.get(docs_ids[indx]).size(); i++) {
						curr = docPointers.get(docs_ids[indx]).get(i);
						pageRankScore += docRank.get(curr) / doc_out_size.get(curr);
					}
					pageRankScore = (1 - d) / doc_count + d * pageRankScore;
					docRank.put(docs_ids[indx], pageRankScore);
					if (Math.abs(pageRankScorePrev - pageRankScore) > tol) {
						finished = false;
					}
				}else {
					pageRankScore = (1 - d) / doc_count;
					docRank.put(docs_ids[indx], pageRankScore);
				}
			}
			iterations += 1;
		}
		
		docs.beforeFirst();
		
		while(docs.next()) {
			MySqlTest.update_Prank(docs.getInt(1), docRank.get(docs.getInt(1)), false);
		}
		MySqlTest.update_Prank(0, 0, true);
	}
	
	public List<Object> pageRelevance(ArrayList<String> searchWords, Boolean isPhrase
			, int offset, int length) throws SQLException{
		
		
		int wordCount = searchWords.size();
		int docsCount = 0;
		MultiKeyHashMap<Integer,Integer,Object> word_title = new MultiKeyHashMap<Integer,Integer,Object>();
		MultiKeyHashMap<Integer,Integer,Object> word_h1 = new MultiKeyHashMap<Integer,Integer,Object>();
		MultiKeyHashMap<Integer,Integer,Object> word_h2 = new MultiKeyHashMap<Integer,Integer,Object>();
		MultiKeyHashMap<Integer,Integer,Object> word_strong = new MultiKeyHashMap<Integer,Integer,Object>();
		MultiKeyHashMap<Integer,Integer,Object> word_b = new MultiKeyHashMap<Integer,Integer,Object>();
		MultiKeyHashMap<Integer,Integer,Object> word_description = new MultiKeyHashMap<Integer,Integer,Object>();
		MultiKeyHashMap<Integer,Integer,Object> word_tf = new MultiKeyHashMap<Integer,Integer,Object>();
		ResultSet words;
		ResultSet words_counts;
		ResultSet docs;
		ResultSet total_docs_count = MySqlTest.Get_Docs_Count();
		total_docs_count.next();
		int totalDocsCount = total_docs_count.getInt(1);
		HashMap<Object, Float> docRank = new HashMap<Object, Float>();
		HashMap<Object, String> docUrl = new HashMap<Object, String>();
		HashMap<Object, String> docTitle = new HashMap<Object, String>();
		HashMap<Object, String> docDesc = new HashMap<Object, String>();
		HashMap<Object, Float> docScore = new HashMap<Object, Float>();
		HashMap<Object, Integer> docDate = new HashMap<Object, Integer>();
		HashMap<Integer, Integer> wordsCount = new HashMap<Integer, Integer>();
		HashMap<String, Integer> wordIds = new HashMap<String, Integer>();
		ArrayList<Integer> docIds = new ArrayList<Integer>();
		
		
		words_counts = MySqlTest.Select_Words_Counts(searchWords);
		
		while(words_counts.next()) {
			wordsCount.put(words_counts.getInt(1), words_counts.getInt(2));
			wordIds.put(words_counts.getString(3), words_counts.getInt(1));
		}
		words_counts.beforeFirst();
		MultiKeyHashMap<Integer,Integer,ArrayList<Integer>> word_pos = new MultiKeyHashMap<Integer,Integer,ArrayList<Integer>>();

		if(isPhrase && searchWords.size()>1) {
			words = MySqlTest.Select_Phrase(searchWords);
			docs = MySqlTest.Select_Docs_Phrase(searchWords);
			while(docs.next()) {
				docRank.put(docs.getInt(1), docs.getFloat(2));
				docUrl.put(docs.getInt(1), docs.getString(3));
				docTitle.put(docs.getInt(1), docs.getString(4));
				docDesc.put(docs.getInt(1), docs.getString(5));
				docDate.put(docs.getInt(1), docs.getInt(6));
			}
			docs.beforeFirst();
			
			while(words.next()) {
				ArrayList<Integer> pos = new ArrayList<Integer>();
				String[] arrOfStr = words.getString(10).split(" ");
				for (int i = 0; i < arrOfStr.length; i++){
					pos.add(Integer.parseInt(arrOfStr[i])); 
				}
				word_pos.put(words.getInt(2), wordIds.get(words.getString(11)), pos);
			}
			words.beforeFirst();
						
			while(docs.next()) {
				Boolean isDocPhrased = false;
				Integer fw = wordIds.get(searchWords.get(0));
				Integer nw = 0;
				int currPos = 0;
				int currLength = 2;
				Integer cw = wordIds.get(searchWords.get(1));
				int fwSize = word_pos.get(docs.getInt(1), fw).size();
				int fwPos = 0;
				Boolean w1w2connected = false;
				for(int i = 0; i<fwSize; i++) {
					fwPos = word_pos.get(docs.getInt(1), fw).get(i);
					int cwSize = word_pos.get(docs.getInt(1), cw).size();
					w1w2connected = false;
					//loop to find if w1 and w2 connected
					for(int j = 0; j<cwSize; j++) {
						int cwPos = word_pos.get(docs.getInt(1), cw).get(j);
						if(cwPos - fwPos == 1) {
							w1w2connected = true;
							currPos = cwPos;
							if(searchWords.size() > currLength) {
								nw = wordIds.get(searchWords.get(currLength));
							}
							break;
						}else {
							if(cwPos - fwPos > 1)
								break;
						}
					}
					
					Boolean exit1 =false;
					Boolean exit2 =false;
					if(searchWords.size() > 2 && w1w2connected) {

						while(!exit1 && !exit2) {
							int nwSize = word_pos.get(docs.getInt(1), nw).size();
							//loop to find if cw and nw connected
							for(int j = 0; j<nwSize; j++) {
								int nwPos = word_pos.get(docs.getInt(1), nw).get(j);
								if(nwPos - currPos == 1) {
									currLength++;
									currPos = nwPos;
									if(searchWords.size() > currLength) {
										nw = wordIds.get(searchWords.get(currLength));
										break;
									}else {
										isDocPhrased = true;
										exit2 = true;
										break;
									}
								}else {
									if(nwPos - currPos > 1) {
										exit1 = true;
										currLength = 2;
										break;
									}
								}
							}
						}
					}
					if(exit2 || (searchWords.size()==2 && w1w2connected))
						break;
				}
				
				if(isDocPhrased  || (searchWords.size()==2 && w1w2connected)) {
					docIds.add(docs.getInt(1));
				}
			}
			System.out.println(docIds);
			docsCount = docIds.size();
		}else {
			words = MySqlTest.Select_Words(searchWords);
			docs = MySqlTest.Select_Docs_Of_Words(searchWords);
			while(docs.next()) {
				docRank.put(docs.getInt(1), docs.getFloat(2));
				docUrl.put(docs.getInt(1), docs.getString(3));
				docTitle.put(docs.getInt(1), docs.getString(4));
				docDesc.put(docs.getInt(1), docs.getString(5));
				docDate.put(docs.getInt(1), docs.getInt(6));
				docIds.add(docs.getInt(1));
				docsCount += 1;
			}
			docs.first();
		}


		
		while(words.next()) {
			word_title.put(words.getInt(2), words.getInt(1), words.getInt(3));
			word_h1.put(words.getInt(2), words.getInt(1), words.getInt(4));
			word_h2.put(words.getInt(2), words.getInt(1), words.getInt(5));
			word_strong.put(words.getInt(2), words.getInt(1), words.getInt(6));
			word_b.put(words.getInt(2), words.getInt(1), words.getInt(7));
			word_description.put(words.getInt(2), words.getInt(1), words.getInt(8));
			word_tf.put(words.getInt(2), words.getInt(1), words.getFloat(9));
		}
		words.first();
				
		int curr_doc_id = 0;
		int curr_word_id = 0;
		float curr_doc_score = 0;
		float curr_doc_tfidf = 0;
		for(int i = 0; i < docsCount; i++){
			curr_doc_tfidf = 0;
			curr_doc_score = 0;
			curr_doc_id = docIds.get(i);
			curr_doc_score = (float) (0.05 * docRank.get(curr_doc_id));
			while(words_counts.next())  {
				curr_word_id = words_counts.getInt(1);
				if(word_tf.get(curr_doc_id, curr_word_id) != null){
					curr_doc_tfidf += ((float)word_tf.get(curr_doc_id, curr_word_id) 
							* Math.log10(totalDocsCount / wordsCount.get(curr_word_id)));
				}
				
				if(word_title.get(curr_doc_id, curr_word_id) != null){
					curr_doc_score += (int)word_title.get(curr_doc_id, curr_word_id) * 0.15;
				}
				
				if(word_h1.get(curr_doc_id, curr_word_id) != null){
					curr_doc_score += (int)word_h1.get(curr_doc_id, curr_word_id) * 0.1;
				}
				
				if(word_h2.get(curr_doc_id, curr_word_id) != null){
					curr_doc_score += (int)word_h2.get(curr_doc_id, curr_word_id) * 0.1;
				}
				
				if(word_strong.get(curr_doc_id, curr_word_id) != null){
					curr_doc_score += (int)word_strong.get(curr_doc_id, curr_word_id) * 0.1;
				}
				
				if(word_b.get(curr_doc_id, curr_word_id) != null){
					curr_doc_score += (int)word_b.get(curr_doc_id, curr_word_id) * 0.05;
				}
				
				if(word_description.get(curr_doc_id, curr_word_id) != null){
					curr_doc_score += (int)word_description.get(curr_doc_id, curr_word_id) * 0.15;
				}
			}
			if(docDate.get(curr_doc_id) != 0) {
				curr_doc_score += (1 / docDate.get(curr_doc_id)) * 0.1;
			}
			words_counts.beforeFirst();
			curr_doc_score += curr_doc_tfidf * 0.2;
			docScore.put(curr_doc_id, curr_doc_score);
		}

		Map<Integer, Float> urls = new HashMap<>();
		
		for(int i =0; i<docsCount; i++) {
			urls.put(docIds.get(i), docScore.get(docIds.get(i)));
		}
		
		Map<Integer, Float> sorted = urls
	            .entrySet()
	            .stream()
	            .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
	            .collect(
	                toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
	                    LinkedHashMap::new));
	     
		ArrayList<Integer> sortedUrls = new ArrayList<Integer>();
		
		sorted.forEach((k, v) -> {
			sortedUrls.add(k);
			});
		ArrayList<Integer> wantedUrls;
		if(length >= (sortedUrls.size() - offset)) {
			wantedUrls = new ArrayList<Integer>(sortedUrls.subList(offset, sortedUrls.size() - 1));
		}else {
			wantedUrls = new ArrayList<Integer>(sortedUrls.subList(offset, length - 1));
		}
		
		List<Object> response = new ArrayList<>();
		List<Object> results = new ArrayList<>();
		Dictionary searchResult = new Hashtable();
		Dictionary page = new Hashtable();
		Dictionary pageDetails = new Hashtable();
		
		for(int i =0; i<wantedUrls.size(); i++) {
			int doc_id = wantedUrls.get(i);
			Dictionary<String, String> result = new Hashtable<String, String>();
			result.put("url", docUrl.get(doc_id));
			if(docTitle.get(doc_id) != null)
				result.put("title", docTitle.get(doc_id));
			else
				result.put("title", "null");
			
			if(docDesc.get(doc_id) != null)
				result.put("description", docDesc.get(doc_id));
			else
				result.put("description", "null");
			results.add(result);
		}
		
		searchResult.put("searchResults", results);
		page.put("totalSize", sortedUrls.size());
		pageDetails.put("pageDetails", page);
		
		response.add(searchResult);
		response.add(pageDetails);
		return response;
		
	}
	
	public List<Object> imageRelevance(ArrayList<String> searchWords
			, int offset, int length) throws SQLException{
		ResultSet imgs = MySqlTest.Select_Imgs(searchWords);
		int imgCount = 0;
				
		List<Object> response = new ArrayList<>();
		List<Object> results = new ArrayList<>();
		Dictionary searchResult = new Hashtable();
		Dictionary page = new Hashtable();
		Dictionary pageDetails = new Hashtable();
		
		while(imgs.next()){
			imgCount++;
		}

		if(offset + 1 <= imgCount)
			imgs.absolute(offset);
		
		Boolean exit = false;	
		int currImgCount = 0;
		while(imgs.next() && !exit){
			Dictionary<String, String> result = new Hashtable<String, String>();
			result.put("url", imgs.getString(1));
			results.add(result);
			currImgCount++;
			if(currImgCount == length)
				exit = true;
		}
		
		searchResult.put("searchResults", results);
		page.put("totalSize", imgCount);
		pageDetails.put("pageDetails", page);
		
		response.add(searchResult);
		response.add(pageDetails);

		return response;
		
	}	
	
	public static void main(String[] args) throws SQLException {
		MySqlTest.connect();
		PageRanker p1 = new PageRanker();
		//p1.pagePopularity();
		
		ArrayList<String> searchWords = new ArrayList<String>();
		//searchWords.add("java");
		//searchWords.add("javatpoint");
		//searchWords.add("metro");
		searchWords.add("mobile");
		searchWords.add("plate");
		p1.imageRelevance(searchWords, 0, 10);
		//p1.pageRelevance(searchWords, true, 0, 100);
	}
	
}
