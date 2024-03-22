import java.sql.*;
import java.io.*;
import java.util.*;

public class Code 
{

    private static final String DB_URL = "jdbc:mysql://localhost:3306/";

    

    private Connection connect(String database) 
    {
        Connection conn = null;
        try 
        {
            conn = DriverManager.getConnection(DB_URL + database);
        } 
        catch (Exception e) 
        {
            e.printStackTrace();
        }
        return conn;
    }

    public void createDatabase(String database) 
    {
        String jdbcUrl = "jdbc:mysql://localhost:3306";
        try (Connection conn = DriverManager.getConnection(jdbcUrl);Statement stmt = conn.createStatement()) 
        {

            String sql = "CREATE DATABASE IF NOT EXISTS "+database;
            stmt.executeUpdate(sql);
            System.out.println("Database created successfully...");
        } 
        catch (SQLException e) 
        {
            e.printStackTrace();
        }
    }

    public void createDummyTable(String database) 
    {
        String sqlCreateTable = "CREATE TABLE IF NOT EXISTS MOVIES_CSV (" +
                                    "ID INT PRIMARY KEY, " +
                                    "Title VARCHAR(255), " +
                                    "ReleaseDate DATE, " +
                                    "GenreName VARCHAR(255), " +
                                    "Director VARCHAR(255), " +
                                    "LeadActors VARCHAR(255), " +
                                    "SupportingActors VARCHAR(255), " +
                                    "MovieLanguage VARCHAR(255), " +
                                    "Runtime INT, " +
                                    "Budget INT, " +
                                    "BoxOfficeRevenue INT, " +
                                    "Rating DECIMAL(6,4), " +
                                    "ReleaseType VARCHAR(50)" +
                                    ")";

        try (Connection conn = this.connect(database);Statement stmt = conn.createStatement()) 
        {
            stmt.execute(sqlCreateTable);
            System.out.println("Table created successfully...");
        } 
        catch (SQLException e) 
        {
            e.printStackTrace();
        }
    }

    public void createTables(String database) 
    {
        // SQL statement for creating the Movies table
        String sqlCreateMovies = "CREATE TABLE IF NOT EXISTS Movies (" +
                                 "MovieID INT AUTO_INCREMENT PRIMARY KEY, " +
                                 "Title VARCHAR(255), " +
                                 "ReleaseDate DATE, " +
                                 "Runtime INT, " +
                                 "Budget DECIMAL(12,2), " +
                                 "BoxOfficeRevenue DECIMAL(12,2), " +
                                 "Rating VARCHAR(5), " +
                                 "ReleaseType VARCHAR(50))";
    
        // SQL statement for creating the MoviePersonnel table
        String sqlCreateMoviePersonnel = "CREATE TABLE IF NOT EXISTS MoviePersonnel (" +
                                         "PersonnelID INT AUTO_INCREMENT PRIMARY KEY, " +
                                         "MovieID INT, " +
                                         "Name VARCHAR(255), " +
                                         "Role VARCHAR(255), " +
                                         "FOREIGN KEY (MovieID) REFERENCES Movies(MovieID))";
    
        // SQL statement for creating the Genres table
        String sqlCreateGenres = "CREATE TABLE IF NOT EXISTS Genres (" +
                                 "GenreID INT AUTO_INCREMENT PRIMARY KEY, " +
                                 "GenreName VARCHAR(100))";
    
        // SQL statement for creating the MovieGenres junction table
        String sqlCreateMovieGenres = "CREATE TABLE IF NOT EXISTS MovieGenres (" +
                                      "MovGenID INT AUTO_INCREMENT PRIMARY KEY, " +
                                      "GenreID INT, " +
                                      "MovID INT, " +
                                      "FOREIGN KEY (GenreID) REFERENCES Genres(GenreID), " +
                                      "FOREIGN KEY (MovID) REFERENCES Movies(MovieID))";
    
        // SQL statement for creating the Languages table
        String sqlCreateLanguages = "CREATE TABLE IF NOT EXISTS Languages (" +
                                    "LanguageID INT AUTO_INCREMENT PRIMARY KEY, " +
                                    "LanguageName VARCHAR(100))";
    
        // SQL statement for creating the MovieLanguage table
        String sqlCreateMovieLanguage = "CREATE TABLE IF NOT EXISTS MovieLanguage (" +
                                        "MovLangID INT AUTO_INCREMENT PRIMARY KEY, " +
                                        "MovID INT, " +
                                        "LangID INT, " +
                                        "FOREIGN KEY (MovID) REFERENCES Movies(MovieID), " +
                                        "FOREIGN KEY (LangID) REFERENCES Languages(LanguageID))";
    
        try (Connection conn = this.connect(database);Statement stmt = conn.createStatement()) 
        {
            // Execute the SQL statements to create the tables
            stmt.execute(sqlCreateMovies);
            stmt.execute(sqlCreateMoviePersonnel);
            stmt.execute(sqlCreateGenres);
            stmt.execute(sqlCreateMovieGenres);
            stmt.execute(sqlCreateLanguages);
            stmt.execute(sqlCreateMovieLanguage);
            
            System.out.println("All tables created successfully...");
        } 
        catch (SQLException e) 
        {
            e.printStackTrace();
        }
    }
    

    public void reader(String fileName,String database) 
    {
        String line;
        String insertMovies = "INSERT INTO DummyTables " +
                                "(Title, ReleaseDate, Genre, Director, LeadActors, SupportingActors, Language, Runtime, Budget, BoxOfficeRevenue, Rating, ReleaseType) " +
                                "VALUES (?, STR_TO_DATE(?, '%d/%m/%Y'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        int lineNumber = 1; // To keep track of the CSV line numbers for error reporting.
    
        try (Connection conn = this.connect(database);
             BufferedReader br = new BufferedReader(new FileReader(fileName));
             PreparedStatement pstmt = conn.prepareStatement(insertMovies)) 
        {
            br.readLine();
            while ((line = br.readLine()) != null) 
            {
                lineNumber++;
                // Use -1 as the limit to include trailing empty strings.
                String[] data = line.split(",", -1);
    
                if (data.length != 12) 
                {
                    System.out.println("Skipping line " + lineNumber + ": Incorrect data format.");
                    continue;
                }
                if (data[0].equals("Title")) 
                {
                    continue;
                }
                try
                {
                    pstmt.setString(1, data[0].trim()); // Title
                    pstmt.setString(2, data[1].trim()); // ReleaseDate
                    pstmt.setString(3, data[2].trim()); // Genre
                    pstmt.setString(4, data[3].trim()); // Director
                    pstmt.setString(5, data[4].trim()); // LeadActors
                    pstmt.setString(6, data[5].trim()); // SupportingActors
                    pstmt.setString(7, data[6].trim()); // Language
                    pstmt.setInt(8, Integer.parseInt(data[7].trim())); // Runtime
                    pstmt.setLong(9, Long.parseLong(data[8].trim())); // Budget
                    pstmt.setLong(10, Long.parseLong(data[9].trim())); // BoxOfficeRevenue
                    pstmt.setFloat(11, Float.parseFloat(data[10].trim())); // Rating
                    pstmt.setString(12, data[11].trim()); // ReleaseType

                    pstmt.executeUpdate();

                }
                catch (NumberFormatException | SQLException e) 
                {
                    System.out.println("Error processing line " + lineNumber + ": " + e.getMessage());
                }
                    
            }
            System.out.println("Insertion complete.");
        } 
        catch (IOException e) 
        {
            System.out.println("Reader IO Exception: " + e.getMessage());
        } 
        catch (SQLException e) 
        {
            System.out.println("Reader SQL Exception: " + e.getMessage());
        }
    }


    public static void main(String[] args) 
    {
        Code obj = new Code();
        String[] databases = {"database1"};
        for(String database: databases)
        {
            obj.createDatabase(database);
            obj.createDummyTable(database);
            obj.createTables(database);
            obj.reader("movies.csv",database);
        }
        
    }
}
