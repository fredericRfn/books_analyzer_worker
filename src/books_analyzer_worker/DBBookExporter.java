package books_analyzer_worker;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import books_analyzer_dao.*;
import books_analyzer_dao.Character;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Driver;
import com.mysql.jdbc.Statement;

public class DBBookExporter {
	public static String url;
	
	public DBBookExporter() {
		try {
			DriverManager.registerDriver((Driver) Class.forName("com.mysql.jdbc.Driver").newInstance());
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		//this.url = "jdbc:mysql://localhost:3306/STG_BOOKS_2?user=root&password=root"; // local configuration
		DBBookExporter.url = "jdbc:mysql://54.191.210.230:3306/STG_BOOKS_2?user=root&password=0"; // EC2 configuration
		
	}

	private static ResultSet executeSQLQuery(String sql) throws SQLException {
		Connection connection = (Connection) DriverManager.getConnection(url);
		Statement statement = (Statement) connection.createStatement();
		ResultSet resultSet = statement.executeQuery(sql);
		//resultSet.close();
		//statement.close();
		//connection.close();
		return resultSet;
	}
	private static int executeSQLUpdate(String sql) throws SQLException {
		Connection connection = (Connection) DriverManager.getConnection(url);
		Statement statement = (Statement) connection.createStatement();
		int resultSet = statement.executeUpdate(sql);
		statement.close();
		connection.close();
		return resultSet;
	}
	
	public static void updateFlag(String idBook, int flag) {
		String sql = "UPDATE Books SET flag=" + flag + " WHERE idBook=" + idBook + ";";
		try {
			executeSQLUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	

	public static HashMap<String, String> getMetadata(String idBook) {
		String sql = "SELECT title, author, url FROM Books WHERE idBook=" + idBook + ";";
 		HashMap<String, String> metadata = new HashMap<String, String>();
		ResultSet rs;
		try {
			rs = executeSQLQuery(sql);
	 		rs.next();
	 		metadata.put("title", rs.getString("title"));
	 		metadata.put("author", rs.getString("author"));
	 		metadata.put("url", rs.getString("url"));
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return metadata;
	}
	
	private static HashMap<String, Integer> getCharactersAndIds(ArrayList<Character> characters) {
		// This method will change, the idCharacter is hash_function(idBook + name)
		ResultSet rs = null;
		HashMap<String, Integer> namesAndIds = new HashMap<String, Integer>();
		
		String sql = "SELECT idCharacter, name FROM Characters WHERE name IN (";
		for(Character c: characters) { sql = sql + "'" + c.getName() + "',"; }
		sql = sql + ");";
 		try {
			rs = executeSQLQuery(sql.replace(",);", ");").replace("\n", ""));
	 		while(rs.next()) {
				namesAndIds.put(rs.getString("name"), rs.getInt("idCharacter"));
			}
		} catch (SQLException e) {
			System.out.println(sql);
			e.printStackTrace();
		}
		return namesAndIds;
	}

	private static HashMap<String, Integer> getSentencesAndIds(ArrayList<Sentence> sentences) {
		// This method will change, the idCharacter is hash_function(idBook + sentence)
		ResultSet rs = null;
		HashMap<String, Integer> sentencesAndIds = new HashMap<String, Integer>();
		
		String sql = "SELECT idSentence, content FROM Sentences WHERE content IN (";
		for(Sentence s: sentences) { sql = sql + "'" + escape(s.getContent()) + "',"; }
		sql = sql + ");";
 		
		try {
			rs = executeSQLQuery(sql.replace(",);",");").replace("\n", "").replace(",''",""));
	 		while(rs.next()) {
	 			sentencesAndIds.put(escape(rs.getString("content")), rs.getInt("idSentence"));
			}
		} catch (SQLException e) {
			System.out.println(sql);
			e.printStackTrace();
		}
		return sentencesAndIds;
	}
	
	public static void export(String idBook, Book book) {
		System.out.println("Exporting book to database");
		String valuesInSQL = "";
		// STEP 1: get the book ID based on the title and the author
		String sql = "";
		
		// STEP 2: Add the sentences to the Sentences table
		System.out.println("Exporting sentences to database");
		ArrayList<Sentence> sentences = book.getSentences(); 
		for(Sentence s: sentences) {
			valuesInSQL = valuesInSQL + "('" + String.join("','", idBook, escape(s.getContent())) + "')";
		};
		
		sql = "INSERT INTO Sentences(idBook, content) VALUES " + valuesInSQL.replace(")(", "),(") + ";\n";
		try {
			executeSQLUpdate(sql);
		} catch (SQLException e) {
			System.out.println(sql);
			e.printStackTrace();
		}
 		// STEP 3: Add the characters to the Characters table
		System.out.println("Exporting characters to database");
		valuesInSQL ="";
		ArrayList<Character> characters = book.getCharacters(); 
		for(Character c: characters) {
			valuesInSQL = valuesInSQL + "('" + String.join("','", c.getName()) + "')";
		};
		
		sql = "INSERT INTO Characters(name) VALUES " + valuesInSQL.replace(")(", "),(") + ";";
 		try {
			executeSQLUpdate(sql.replace("\"", "#").replace("\n", ""));
		} catch (SQLException e) {
			System.out.println(sql);
			e.printStackTrace();
		}
 		// STEP 4: Export the CharacterSentences
		System.out.println("Exporting characterSentences to database");
		valuesInSQL ="";
		ArrayList<CharacterSentence> characterSentences = book.getCharacterSentences(); 
		System.out.println("Number of CharacterSentences to be exported:" + characterSentences.size());
	
		// Obtain all the ids corresponding to the sentences and characters, with TWO SQL queries only
		HashMap<String, Integer> namesAndIds = getCharactersAndIds(characters);
		HashMap<String, Integer> sentencesAndIds = getSentencesAndIds(sentences);

		for(CharacterSentence cs: characterSentences) {
			System.out.println(cs.getSentence().getContent());
			if (sentencesAndIds.get(escape(cs.getSentence().getContent()))!=null) {
			valuesInSQL = valuesInSQL + "("
					+ String.join(",", 
							namesAndIds.get(cs.getCharacter().getName()).toString(),
							sentencesAndIds.get(escape(cs.getSentence().getContent())).toString())
					+ ")";
			}
	 		
		};
		
		sql = "INSERT INTO CharacterSentence(idCharacter, idSentence) VALUES " + valuesInSQL.replace(")(", "),(") + ";";
		try {
			executeSQLUpdate(sql.replace("\"", "#").replace("\n", ""));
		} catch (SQLException e) {
			System.out.println(sql);
			e.printStackTrace();
		}
	}
	
	private static String escape(String s) {
		return s.replace("'", "#").replace("\"","#").replace("\n", "#");
	}
}
