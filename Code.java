import java.sql.*;
import java.io.*;
import java.util.*;

public class Code 
{

    private static final String DB_URL = "jdbc:mysql://localhost:3306/";

    
    //Creating Connection with MySQLServer
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

    //Creating the Database
    public void createDatabase(String database) 
    {
        String jdbcUrl = "jdbc:mysql://localhost:3306";
        try (Connection conn = DriverManager.getConnection(jdbcUrl);Statement stmt = conn.createStatement()) 
        {
            String sqldropDatabase = "DROP DATABASE IF EXISTS " + database + ";";
            String sql = "CREATE DATABASE IF NOT EXISTS "+database;
            stmt.execute(sqldropDatabase);
            stmt.executeUpdate(sql);
            System.out.println("Database for Student " +database+ " has been created...\n");
        } 
        catch (SQLException e) 
        {
            e.printStackTrace();
        }
    }
    //Create Tables Step 1
    public void createWorkingTable(String database) 
    {
        
        String sqlCreateTable = "CREATE TABLE IF NOT EXISTS MOVIES_CSV (" +
                                    "ID          INT AUTO_INCREMENT PRIMARY KEY, " +
                                    "Title       VARCHAR(255), " +
                                    "ReleaseDate DATE, " +
                                    "GenreName   VARCHAR(500), " +
                                    "Director    VARCHAR(100), " +
                                    "LeadActors  VARCHAR(500), " +
                                    "SupportingActors VARCHAR(500), " +
                                    "MovieLanguage    VARCHAR(500), " +
                                    "Runtime          INT, " +
                                    "Budget           INT, " +
                                    "BoxOfficeRevenue INT, " +
                                    "Rating           DECIMAL(6,4), " +
                                    "ReleaseType      VARCHAR(50)" +
                                    ")";

        try (Connection conn = this.connect(database);Statement stmt = conn.createStatement()) 
        {
            
            stmt.execute(sqlCreateTable);
            //System.out.println("Table created successfully...");
        } 
        catch (SQLException e) 
        {
            e.printStackTrace();
        }
    }

    //Create Tables Step 2
    public void createTables(String database) 
    {

        // SQL statement for creating the Genres table
        

        String sqlCreateGenres = "CREATE TABLE IF NOT EXISTS Genres (" +
                                 "ID        INT AUTO_INCREMENT PRIMARY KEY, " +
                                 "GenreName VARCHAR(100))";

        //SQL statement for creating the Actors Table
        
        String sqlCreateActors = "CREATE TABLE IF NOT EXISTS Actors (" +
                                 "ID         INT AUTO_INCREMENT PRIMARY KEY, " +
                                 "ActorName  VARCHAR(100))";

        // SQL statement for creating the Languages table
        
        String sqlCreateLanguages = "CREATE TABLE IF NOT EXISTS Languages (" +
                                    "ID           INT AUTO_INCREMENT PRIMARY KEY, " +
                                    "LanguageName VARCHAR(50))";
    

        // SQL statement for creating the Movies table
        
        String sqlCreateMovies = "CREATE TABLE IF NOT EXISTS Movies (" +
                                 "ID               INT AUTO_INCREMENT PRIMARY KEY, " +
                                 "Title            VARCHAR(255), " +
                                 "ReleaseDate      DATE, " +
                                 "Runtime          INT, " +
                                 "Budget           INT, " +
                                 "BoxOfficeRevenue INT, " +
                                 "Rating           INT, " +
                                 "ReleaseType      VARCHAR(50), " +
                                 "DIRECTOR         VARCHAR(100))";

        // SQL statement for creating the MovieGenres 
        
        String sqlCreateMovieGenres = "CREATE TABLE IF NOT EXISTS MovieGenres (" +
                                      "ID         INT AUTO_INCREMENT PRIMARY KEY, " +
                                      "Movie_ID   INT, " +
                                      "Genre_ID   INT, " +
                                      "FOREIGN KEY (Genre_ID) REFERENCES Genres(ID), " +
                                      "FOREIGN KEY (Movie_ID) REFERENCES Movies(ID))";
    
    
        // SQL statement for creating the MovieActors table
        
        String sqlCreateMovieActors =   "CREATE TABLE IF NOT EXISTS MovieActors (" +
                                        "ID        INT AUTO_INCREMENT PRIMARY KEY, " +
                                        "Movie_ID   INT, " +
                                        "ActorType enum('LEAD','SUPPORTING'), " +
                                        "Actor_ID  INT, " +
                                        "FOREIGN KEY (Actor_ID) REFERENCES Actors(ID), " +
                                        "FOREIGN KEY (Movie_ID) REFERENCES Movies(ID))";

        // SQL statement for creating the MovieLanguage table
        
        String sqlCreateMovieLanguages ="CREATE TABLE IF NOT EXISTS MovieLanguages (" +
                                        "ID           INT AUTO_INCREMENT PRIMARY KEY, " +
                                        "Movie_ID     INT, " +
                                        "Language_ID  INT, " +
                                        "FOREIGN KEY  (Movie_ID) REFERENCES Movies(ID), " +
                                        "FOREIGN KEY  (Language_ID) REFERENCES Languages(ID))";
    
        try (Connection conn = this.connect(database);Statement stmt = conn.createStatement()) 
        {
            // Execute the SQL statements to create the tables
            stmt.execute(sqlCreateGenres);
            stmt.execute(sqlCreateActors);
            stmt.execute(sqlCreateLanguages);
            stmt.execute(sqlCreateMovies);
            stmt.execute(sqlCreateMovieGenres);
            stmt.execute(sqlCreateMovieActors);
            stmt.execute(sqlCreateMovieLanguages);
        } 
        catch (SQLException e) 
        {
            e.printStackTrace();
        }
    }

    public void queries(String database)
    {
        String query1 = "delete from Movies order by ID Asc limit 1;";
        String query2 = "delete from Languages order by ID asc limit 1;";
        String query3 = "SELECT a.Title AS Title, COUNT(*) AS No_Of_Languages " +
                          "FROM Movies a, MovieLanguages b " +
                          "WHERE a.ID = b.Movie_ID " +
                          "GROUP BY a.title " +
                          "HAVING COUNT(*) > 3;";
        String query4 = "SELECT a.Title AS Title, COUNT(*) AS No_Of_Actors " +
                          "FROM Movies a, MovieActors b " +
                          "WHERE a.ID = b.Movie_ID " +
                          "GROUP BY a.Title " +
                          "HAVING COUNT(*) > 6;";
        try (Connection conn = this.connect(database);
        Statement stmt = conn.createStatement();) 
        {
            stmt.executeUpdate(query1);
        }
        catch (SQLException e) 
        {
            System.out.println("\nQuery 1 : " + e.getMessage());
        }

        try (Connection conn = this.connect(database);
        Statement stmt = conn.createStatement();) 
        {
            stmt.executeUpdate(query1);
        }
        catch (SQLException e) 
        {
            System.out.println("\nQuery 2: " + e.getMessage());
        }
        try (Connection conn = this.connect(database);
        Statement stmt = conn.createStatement();
        ResultSet rs1 = stmt.executeQuery(query3)) 
        {   
            System.out.println("\nQuery 3 : The movies released in more than 3 languages\n" );
            System.out.printf("%-45s %15s %n", "Title", "No of Languages");
            while (rs1.next()) 
            {
                String title = rs1.getString("Title");
                int noOfLanguages = rs1.getInt("No_Of_Languages");
                System.out.printf("%-45s %15d %n", title, noOfLanguages);
            }
        }
        catch (SQLException e) 
        {
            System.out.println("An error occurred: " + e.getMessage());
        }

        try (Connection conn = this.connect(database);
        Statement stmt = conn.createStatement();
        ResultSet rs2 = stmt.executeQuery(query4)) 
        {
            System.out.println("\nQuery 4 : The movies having more than 6 actors\n" );
            System.out.printf("%-45s %15s %n", "Title", "No of Actors");
            while (rs2.next()) 
            {
                String title = rs2.getString("Title");
                int noOfActors = rs2.getInt("No_Of_Actors");
                System.out.printf("%-45s %15d %n", title, noOfActors);
            }
            System.out.println("\n\n");

        }
        catch (SQLException e) 
        {
            System.out.println("An error occurred: " + e.getMessage());
        }

     

    }
    
    public void createProcedures(String database)
    {
        //Statement to drop import_data procedure
        String drop_Import_data = "drop PROCEDURE IF EXISTS import_data;";
        
        //Statment to create the import_data 
        String import_data =   "CREATE PROCEDURE import_data ()\n" +
                                "BEGIN\n" +
                                "    DECLARE n int;\n" +
                                "    DECLARE finished INTEGER DEFAULT NULL;\n" +
                                "    DECLARE v_ID INTEGER DEFAULT NULL;\n" +
                                "    DECLARE v_Title VARCHAR(100) DEFAULT \"\";\n" +
                                "    DECLARE v_ReleaseDate DATE DEFAULT NULL;\n" +
                                "    DECLARE v_GenreName VARCHAR(500) DEFAULT \"\";\n" +
                                "    DECLARE v_Director VARCHAR(100) DEFAULT \"\";\n" +
                                "    DECLARE v_LeadActors VARCHAR(500) DEFAULT \"\";\n" +
                                "    DECLARE v_SupportingActors VARCHAR(500) DEFAULT \"\";\n" +
                                "    DECLARE v_MovieLanguage VARCHAR(500) DEFAULT \"\";\n" +
                                "    DECLARE v_Runtime INTEGER DEFAULT NULL;\n" +
                                "    DECLARE v_Budget INTEGER DEFAULT NULL;\n" +
                                "    DECLARE v_BoxOfficeRevenue INTEGER DEFAULT NULL;\n" +
                                "    DECLARE v_Rating DECIMAL(6,4) DEFAULT 0.0;\n" +
                                "    DECLARE v_ReleaseType VARCHAR(100) DEFAULT \"\";\n" +
                                "    #Cursor declaration\n" +
                                "    DECLARE curName CURSOR FOR SELECT * from MOVIES_CSV;\n" +
                                "    #declare NOT FOUND handler\n" +
                                "    DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished = 1;\n" +
                                "    #Open cursor\n" +
                                "    OPEN curName;\n" +
                                "    #fetch records\n" +
                                "    getValues: LOOP\n" +
                                "      FETCH curName INTO v_ID,v_Title,v_ReleaseDate,v_GenreName,v_Director,v_LeadActors,v_SupportingActors,v_MovieLanguage,v_Runtime,v_Budget,v_BoxOfficeRevenue,v_Rating,v_ReleaseType;\n" +
                                "\n" +
                                "      IF finished = 1 THEN \n" +
                                "        LEAVE getValues;\n" +
                                "      END IF;\n" +
                                "\n" +
                                "      set n=0;\n" +
                                "      select count(*) into n from Movies where ID=v_ID;\n" +
                                "      if n>0 then\n" +
                                "        ITERATE getValues;\n" +
                                "      end if;\n" +
                                "      insert into Movies (ID,Title,ReleaseDate,Runtime,Budget,BoxOfficeRevenue,Rating,ReleaseType,Director) \n" +
                                "      values (v_ID,v_Title,v_ReleaseDate,v_Runtime,v_Budget,v_BoxOfficeRevenue,v_Rating,v_ReleaseType,v_Director);\n" +
                                "\n" +
                                "      call set_child_data(v_ID,v_GenreName,v_LeadActors,v_SupportingActors,v_MovieLanguage);\n" +
                                "\n" +
                                "   END LOOP getValues;\n" +
                                "   CLOSE curName;\n" +
                                "\n" +
                                "END\n";
        
        
        //Statment to Drop set_child_data procedure
        String drop_set_child_data = "drop PROCEDURE IF EXISTS set_child_data;";
        
        //Statment to create a set_child_data
        String set_child_data = "CREATE PROCEDURE set_child_data (\n" +
                                "IN cur_Movie_ID int,\n" +
                                "cur_Genres varchar(500),\n" +
                                "cur_Actors varchar(500),\n" +
                                "cur_SupActors varchar(500),\n" +
                                "cur_Languages varchar(500)\n" +
                                ")\n" +
                                "BEGIN\n" +
                                "  DECLARE m int;\n" +
                                "  DECLARE i int;\n" +
                                "  DECLARE v_Genre_ID int;\n" +
                                "  DECLARE v_Actor_ID int;\n" +
                                "  DECLARE v_SupActor_ID int;\n" +
                                "  DECLARE v_Language_ID int;\n" +
                                "  DECLARE cur_GenreName VARCHAR(100) DEFAULT \"\";\n" +
                                "  DECLARE cur_ActorName VARCHAR(100) DEFAULT \"\";\n" +
                                "  DECLARE cur_SupActorName VARCHAR(100) DEFAULT \"\";\n" +
                                "  DECLARE cur_LanguageName VARCHAR(100) DEFAULT \"\";\n" +
                                "\n" +
                                "  -- START Genres\n" +
                                "  if cur_Genres is not null then\n" +
                                "      set m=0;\n" +
                                "      SELECT LENGTH(cur_Genres)-LENGTH(REPLACE(cur_Genres, \":\", \"\")) into m;\n" +
                                "      set i=1;\n" +
                                "      set m=m+1;\n" +
                                "      Genre_loop: LOOP\n" +
                                "          select replace(SUBSTRING_INDEX(cur_Genres,':',i),concat(SUBSTRING_INDEX(cur_Genres,':',(i-1)),':'),\"\") into cur_GenreName;\n" +
                                "          if cur_GenreName is not null then\n" +
                                "              set v_Genre_ID=0;\n" +
                                "              select ID into v_Genre_ID from Genres where GenreName=cur_GenreName;\n" +
                                "              if ifnull(v_Genre_ID,0)=0 then\n" +
                                "                insert into Genres(GenreName) values(cur_GenreName);\n" +
                                "                SELECT LAST_INSERT_ID() into v_Genre_ID;\n" +
                                "              end if;\n" +
                                "              insert into MovieGenres (Movie_ID,Genre_ID) values (cur_Movie_ID,v_Genre_ID);\n" +
                                "          end if;\n" +
                                "          set i=i+1;\n" +
                                "          if i>m then\n" +
                                "            LEAVE Genre_loop;\n" +
                                "          end if;\n" +
                                "      END LOOP Genre_loop;\n" +
                                "  end if;\n" +
                                "  -- END Genres\n" +
                                "\n" +
                                "  -- START Actors\n" +
                                "  if cur_Actors is not null then\n" +
                                "      set m=0;\n" +
                                "      SELECT LENGTH(cur_Actors)-LENGTH(REPLACE(cur_Actors, \":\", \"\")) into m;\n" +
                                "      set i=1;\n" +
                                "      set m=m+1;\n" +
                                "      Actor_loop: LOOP\n" +
                                "          select replace(SUBSTRING_INDEX(cur_Actors,':',i),concat(SUBSTRING_INDEX(cur_Actors,':',(i-1)),':'),\"\") into cur_ActorName;\n" +
                                "          if cur_ActorName is not null then\n" +
                                "              set v_Actor_ID=0;\n" +
                                "              select ID into v_Actor_ID from Actors where ActorName=cur_ActorName;\n" +
                                "              if ifnull(v_Actor_ID,0)=0 then\n" +
                                "                insert into Actors(ActorName) values(cur_ActorName);\n" +
                                "                SELECT LAST_INSERT_ID() into v_Actor_ID;\n" +
                                "              end if;\n" +
                                "              insert into MovieActors (Movie_ID,ActorType,Actor_ID) values (cur_Movie_ID,'LEAD',v_Actor_ID);\n" +
                                "          end if;\n" +
                                "          set i=i+1;\n" +
                                "          if i>m then\n" +
                                "            LEAVE Actor_loop;\n" +
                                "          end if;\n" +
                                "      END LOOP Actor_loop;\n" +
                                "  end if;\n" +
                                "  -- END Actors\n" +
                                "\n" +
                                "  -- START SupActors\n" +
                                "  if cur_SupActors is not null then\n" +
                                "      set m=0;\n" +
                                "      SELECT LENGTH(cur_SupActors)-LENGTH(REPLACE(cur_SupActors, \":\", \"\")) into m;\n" +
                                "      set i=1;\n" +
                                "      set m=m+1;\n" +
                                "      SupActor_loop: LOOP\n" +
                                "          select replace(SUBSTRING_INDEX(cur_SupActors,':',i),concat(SUBSTRING_INDEX(cur_SupActors,':',(i-1)),':'),\"\") into cur_SupActorName;\n" +
                                "          if cur_SupActorName is not null then\n" +
                                "              set v_SupActor_ID=0;\n" +
                                "              select ID into v_SupActor_ID from Actors where ActorName=cur_SupActorName;\n" +
                                "              if ifnull(v_SupActor_ID,0)=0 then\n" +
                                "                insert into Actors(ActorName) values(cur_SupActorName);\n" +
                                "                SELECT LAST_INSERT_ID() into v_SupActor_ID;\n" +
                                "              end if;\n" +
                                "              insert into MovieActors (Movie_ID,ActorType,Actor_ID) values (cur_Movie_ID,'SUPPORTING',v_SupActor_ID);\n" +
                                "          end if;\n" +
                                "          set i=i+1;\n" +
                                "          if i>m then\n" +
                                "            LEAVE SupActor_loop;\n" +
                                "          end if;\n" +
                                "      END LOOP SupActor_loop;\n" +
                                "  end if;\n" +
                                "  -- END SupActors\n" +
                                "\n" +
                                "  -- START Languages\n" +
                                "  if cur_Languages is not null then\n" +
                                "      set m=0;\n" +
                                "      SELECT LENGTH(cur_Languages)-LENGTH(REPLACE(cur_Languages, \":\", \"\")) into m;\n" +
                                "      set i=1;\n" +
                                "      set m=m+1;\n" +
                                "      Language_loop: LOOP\n" +
                                "          select replace(SUBSTRING_INDEX(cur_Languages,':',i),concat(SUBSTRING_INDEX(cur_Languages,':',(i-1)),':'),\"\") into cur_LanguageName;\n" +
                                "          if cur_LanguageName is not null then\n" +
                                "              set v_Language_ID=0;\n" +
                                "              select ID into v_Language_ID from Languages where LanguageName=cur_LanguageName;\n" +
                                "              if ifnull(v_Language_ID,0)=0 then\n" +
                                "                insert into Languages(LanguageName) values(cur_LanguageName);\n" +
                                "                SELECT LAST_INSERT_ID() into v_Language_ID;\n" +
                                "              end if;\n" +
                                "              insert into MovieLanguages (Movie_ID,Language_ID) values (cur_Movie_ID,v_Language_ID);\n" +
                                "          end if;\n" +
                                "          set i=i+1;\n" +
                                "          if i>m then\n" +
                                "            LEAVE Language_loop;\n" +
                                "          end if;\n" +
                                "      END LOOP Language_loop;\n" +
                                "  end if;\n" +
                                "  -- END Languages\n" +
                                "END\n";
        
        try (Connection conn = this.connect(database);Statement stmt = conn.createStatement()) 
        {
            // Execute the SQL statements to create the tables
            stmt.execute(drop_Import_data);
            stmt.execute(import_data);
            stmt.execute(drop_set_child_data);
            stmt.execute(set_child_data);
            
        } 
        catch (SQLException e) 
        {
            e.printStackTrace();
        }

    }

    public void callProcedure(String database)
    {
        String callProcedureString = "call import_data;";
        try(Connection conn = this.connect(database);Statement stmt = conn.createStatement())
        {
            stmt.executeQuery(callProcedureString);
            System.out.println("Database for Student "+database+" has been populated...\n");
        }
        catch(SQLException e)
        {
            e.printStackTrace();
        } 
    }

    public void reader(String fileName,String database) 
    {
        String line;
        String insertMovies = "INSERT INTO MOVIES_CSV " +
                      "(Title, ReleaseDate, GenreName, Director, LeadActors, SupportingActors, MovieLanguage, Runtime, Budget, BoxOfficeRevenue, Rating, ReleaseType) " +
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
                    System.out.println("Skipping line " + lineNumber + data[0] + ": Incorrect data format.");
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
                    System.out.println("Error processing line " + lineNumber +  ": " + e.getMessage());
                }
                    
            }
            //System.out.println("Insertion complete.");
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
        String[] databases = {"38540681","38776405","38863006"};
        for(String database : databases)
        {
            obj.createDatabase("DM"+database);
            obj.createWorkingTable("DM"+database);
            obj.createTables("DM"+database);
            obj.createProcedures("DM"+database);
            obj.reader(database+".csv","DM"+database);
            obj.callProcedure("DM"+database);
            obj.queries("DM"+database);
        }
        
    }
}
