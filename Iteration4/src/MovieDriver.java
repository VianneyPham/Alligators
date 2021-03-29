package Iteration4.src;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MovieDriver {
	public static boolean processMovieSongs(){
		Connection allConn = null;
		try{ 	
			allConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/omdb", "root", "");

			PreparedStatement mstmt = allConn.prepareStatement("INSERT INTO movies(native_name, year_made) SELECT DISTINCT native_name, year_made FROM ms_test_data WHERE NOT EXISTS (SELECT 1 FROM movies WHERE native_name = ms_test_data.native_name and year_made = ms_test_data.year_made)");		
			int movieCreated = mstmt.executeUpdate();

			PreparedStatement sstmt = allConn.prepareStatement("INSERT INTO songs(title) SELECT DISTINCT title FROM ms_test_data WHERE NOT EXISTS ( SELECT title FROM songs WHERE songs.title = ms_test_data.title)");		
			int songCreated = sstmt.executeUpdate();			

			PreparedStatement msStmt = allConn.prepareStatement("INSERT INTO movie_song (movie_id, song_id) SELECT movies.movie_id, songs.song_id FROM movies, songs WHERE NOT EXISTS (SELECT 1 FROM movie_song WHERE movies.movie_id = movie_song.movie_id )");
			int msCreated = msStmt.executeUpdate();


			if(movieCreated > 1 && songCreated > 1 && msCreated > 1){
				PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Created S Created MS Created' WHERE ID = ms_test_data.ID");
				movieStatusUpdateM.execute();
			}else if(movieCreated < 1 && songCreated < 1 && msCreated < 1){
				PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Ignored S Ignored MS Ignored' WHERE ID = ms_test_data.ID");
				movieStatusUpdateM.execute();
			}else if(movieCreated > 1 && songCreated < 1 && msCreated < 1){
				PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Created S Ignored MS Ignored' WHERE ID = ms_test_data.ID");
				movieStatusUpdateM.execute();
			}else if(movieCreated < 1 && songCreated > 1 && msCreated < 1){
				PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Ignored S Created MS Ignored' WHERE ID = ms_test_data.ID");
				movieStatusUpdateM.execute();
			}else if(movieCreated < 1 && songCreated < 1 && msCreated > 1){
				PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Ignored S Ignored MS Created' WHERE ID = ms_test_data.ID");
				movieStatusUpdateM.execute();
			}else if(movieCreated > 1 && songCreated > 1 && msCreated < 1){
				PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Created S Created MS Ignored' WHERE ID = ms_test_data.ID");
				movieStatusUpdateM.execute();
			}else if(movieCreated > 1 && songCreated < 1 && msCreated > 1){
				PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Created S Ignored MS Created' WHERE ID = ms_test_data.ID");
				movieStatusUpdateM.execute();
			}else if(movieCreated < 1 && songCreated > 1 && msCreated > 1){
				PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Ignored S Created MS Created' WHERE ID = ms_test_data.ID");
				movieStatusUpdateM.execute();
			}
		allConn.close();
		
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		return true;
	}
	public static void main(String[] args) {
		processMovieSongs();
	}
}