package books_analyzer_dao;

import java.util.ArrayList;

public class Character {
	private int id;
	public final String name;
	public ArrayList<String> sentences;
	
	public Character(String n) {
		this.name = n;
		this.sentences = new ArrayList<String>();
	}
	
	public Character(String n, ArrayList<String> s) {
		this.name = n;
		this.sentences = s;
	}

	public String getName() {
		return this.name; 
	}
	
	public ArrayList<String> getSentences() {
		return this.sentences; 
	}
	
	public void addSentence(Sentence s) {
		sentences.add(s.getContent()); 
	}
}
