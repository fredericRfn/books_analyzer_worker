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
			//this.url = "jdbc:mysql://localhost:3306/STG_BOOKS_2?user=root&password=root"; // local configuration
			DBBookExporter.url = "jdbc:mysql://54.191.210.230:3306/STG_BOOKS_2?user=root&password=0"; // EC2 configuration
			executeSQLQuery("SET GLOBAL max_allowed_packet =" + 1024*1024 + ";");
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	private ResultSet executeSQLQuery(String sql) throws SQLException {
		Connection connection = (Connection) DriverManager.getConnection(url);
		Statement statement = (Statement) connection.createStatement();
		ResultSet resultSet = statement.executeQuery(sql);
		//resultSet.close();
		//statement.close();
		//connection.close();
		return resultSet;
	}
	private int executeSQLUpdate(String sql) throws SQLException {
		Connection connection = (Connection) DriverManager.getConnection(url);
		Statement statement = (Statement) connection.createStatement();
		int resultSet = statement.executeUpdate(sql);
		statement.close();
		connection.close();
		return resultSet;
	}
	
	public void updateFlag(String idBook, int flag) {
		String sql = "UPDATE Books SET flag=" + flag + " WHERE idBook=" + idBook + ";";
		try {
			executeSQLUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public HashMap<String, String> getMetadata(String idBook) {
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
	
	public void exportSentences(String idBook, Book book) {
		String valuesInSQL = "";
		String sql = "";
		ArrayList<Sentence> sentences = book.getSentences(); 
		String id;
		for(Sentence s: sentences) {
			id = IdCreator.create(idBook + s.content);
			valuesInSQL = valuesInSQL + "('" + String.join("','", id, idBook, escape(s.getContent())) + "')";
		};
		sql = "INSERT INTO Sentences(idSentence, idBook, content) VALUES " + valuesInSQL.replace(")(", "),(") + ";\n";
		try {
			executeSQLUpdate(sql);
		} catch (SQLException e) {
			System.out.println(sql);
			e.printStackTrace();
		}
	}
	public void exportCharacters(String idBook, Book book) {
		String valuesInSQL =""; String id; String sql;
		ArrayList<Character> characters = book.getCharacters(); 
		for(Character c: characters) {
			id = IdCreator.create(idBook + c.name);
			valuesInSQL = valuesInSQL + "('" + String.join("','", id, c.getName()) + "')";
		};
		sql = "INSERT INTO Characters(idCharacter, name) VALUES " + valuesInSQL.replace(")(", "),(") + ";";
 		try {
			executeSQLUpdate(sql.replace("\"", "#").replace("\n", ""));
		} catch (SQLException e) {
			System.out.println(sql);
			e.printStackTrace();
		}
	}
	public void exportCharacterSentences(String idBook, Book book) {	
		String valuesInSQL =""; String id; String sql;
		ArrayList<CharacterSentence> characterSentences = book.getCharacterSentences(); 
		for(CharacterSentence cs: characterSentences) {
			System.out.println(cs.getSentence().getContent()); 
			valuesInSQL = valuesInSQL + "("
					+ String.join(",", 
							IdCreator.create(idBook + cs.getCharacter().getName()),
							IdCreator.create(idBook + cs.getSentence().getContent()))
					+ ")";
		};
		sql = "INSERT INTO CharacterSentence(idCharacter, idSentence) VALUES " + valuesInSQL.replace(")(", "),(") + ";";
		try {
			executeSQLUpdate(sql.replace("\"", "#").replace("\n", ""));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private String escape(String s) {
		return s.replace("'", "#").replace("\"","#").replace("\n", "#");
	}
}
