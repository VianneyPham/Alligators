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
			
			Statement allStmt = allConn.createStatement();

			ResultSet rs = allStmt.executeQuery("SELECT * FROM ms_test_data");

			String movieRS = "SELECT * FROM movies WHERE native_name = ? AND year_made = ?";

			String insertMovie = "INSERT INTO movies (native_name, year_made, english_name) VALUES (?,?,?)";

			String songRS = "SELECT title FROM songs WHERE title = ?";

			String insertSong = "INSERT INTO songs (title,theme) VALUES (?,?)";

			PreparedStatement pmaxid = allConn.prepareStatement("SELECT max(ID) FROM ms_test_data");
			ResultSet maxIDrs = pmaxid.executeQuery();
			int maxID = 0;
			if( maxIDrs.next()){
				maxID = maxIDrs.getInt(1);
			}

			for (int i = 0; i < maxID; i++){
				
				rs.next();
				int movieCreated = 0;
				int songCreated = 0;
				int msCreated = 0;

				int currentMovie = 0;

				String nativeNameTD = rs.getString("native_name");
				int yearMadeTD = rs.getInt("year_made");
			
				PreparedStatement ps = allConn.prepareStatement(movieRS);
				ps.setString(1, nativeNameTD);
				ps.setInt(2,yearMadeTD);

				ResultSet mrs = ps.executeQuery();
				System.out.println(mrs.next());
				mrs.beforeFirst();
				if (mrs.next() == true)
				{
					System.out.println("Movie already exists!");
					currentMovie += 1;
				}
				else
				{
					PreparedStatement moviePS = allConn.prepareStatement(insertMovie);
					moviePS.setString(1,nativeNameTD);
					moviePS.setInt(2,yearMadeTD);
					moviePS.setString(3,nativeNameTD);

					int row = moviePS.executeUpdate();
					System.out.println("Movie has been added!");
					movieCreated += 1;
					currentMovie += 1;
				}

				String titleTD = rs.getString("title");
				titleTD = titleTD.replace("\"","");
				System.out.println(titleTD);
				PreparedStatement song_ps = allConn.prepareStatement(songRS);
				song_ps.setString(1, titleTD);

				ResultSet srs = song_ps.executeQuery();
				System.out.println(srs.next());

				srs.beforeFirst();
				if (srs.next() == true)
				{
					System.out.println("Song already exists!");
				}
				else
				{
					PreparedStatement songPS = allConn.prepareStatement(insertSong);
					songPS.setString(1,titleTD);
					songPS.setString(2, "Null");

					int row = songPS.executeUpdate();
					System.out.println("Song has been added!");
					songCreated += 1;
				}

				System.out.println(movieCreated);
				System.out.println(songCreated);
				System.out.println(msCreated);

				if(movieCreated > 0 && songCreated > 0 && msCreated > 0){
					PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Created S Created MS Created' WHERE title = ?");
					movieStatusUpdateM.setString(1,titleTD);
					movieStatusUpdateM.execute();
				} 
				else {
					PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Ignored S Ignored MS Ignored' WHERE title = ?");
					movieStatusUpdateM.setString(1,titleTD);
					movieStatusUpdateM.execute();
				}

				if (movieCreated > 0 && songCreated == 0 && msCreated == 0){
					PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Created S Ignored MS Ignored' WHERE title = ?");
					movieStatusUpdateM.setString(1,titleTD);
					movieStatusUpdateM.execute();
				}
				
				else if(movieCreated == 0 && songCreated > 0 && msCreated == 0){
					PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Ignored S Created MS Ignored' WHERE title = ?");
					movieStatusUpdateM.setString(1,titleTD);
					movieStatusUpdateM.execute();
				}

				else if(movieCreated == 0 && songCreated == 0 && msCreated > 0){
					PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Ignored S Ignored MS Created' WHERE title = ?");
					movieStatusUpdateM.setString(1,titleTD);
					movieStatusUpdateM.execute();
				}

				else if(movieCreated > 0 && songCreated > 0 && msCreated == 0){
					PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Created S Created MS Ignored' WHERE title = ?");
					movieStatusUpdateM.setString(1,titleTD);
					movieStatusUpdateM.execute();
				}

				else if(movieCreated > 0 && songCreated == 0 && msCreated > 0){
					PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Created S Ignored MS Created' WHERE title = ?");
					movieStatusUpdateM.setString(1,titleTD);
					movieStatusUpdateM.execute();
				}

				else if(movieCreated == 0 && songCreated > 0 && msCreated > 0){
					PreparedStatement movieStatusUpdateM = allConn.prepareStatement("UPDATE ms_test_data SET execution_status = 'M Ignored S Created MS Created' WHERE title = ?");
					movieStatusUpdateM.setString(1,titleTD);
					movieStatusUpdateM.execute();
				}

				
				System.out.println("\n");
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