package books_analyzer_worker;

public class Sentence {
	public final String content;
	private String[] words;
	public int charactersReferenced;
	
	public Sentence(String s) {
		this.charactersReferenced = 0;
		this.content = s;
		this.words = s.replace("\\", "").replace("'s", "").replace("\"", "").replace(".", "").replace(",", "").replace("?", "").replace("!","").split("\\s|'");
	}
	
	public String getContent() {
		return this.content;
	}
	
	public String[] getWords() {
		return this.words;
	}
	
	public boolean references(Character character) {
		return this.content.toLowerCase().contains(character.getName().toLowerCase());	
	}
}
