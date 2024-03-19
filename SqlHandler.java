import java.sql.Statement;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NativeCSVReader 
{
	public void reader(String fileName)
	{
        try (BufferedReader br = new BufferedReader(new FileReader(fileName)))
        {
            String line;
            List<String[]> allRows = new ArrayList<>();

            // Read each line from the CSV file
            while ((line = br.readLine()) != null) {
                // Split the line into an array of values using a comma as the delimiter
                String[] row = line.split(",");
                allRows.add(row);
            }

            // Assuming the first row contains column headers
            String[] headers = allRows.get(0);

            // Create arrays for each column dynamically
            int numColumns = headers.length;
            String[][] dataArrays = new String[numColumns][];

            // Initialize arrays
            for (int i = 0; i < numColumns; i++) {
                dataArrays[i] = new String[allRows.size()];
            }

            // Populate arrays with data
            for (int i = 1; i < allRows.size(); i++) {
                String[] row = allRows.get(i);
                for (int j = 0; j < numColumns; j++) {
                    dataArrays[j][i - 1] = row[j];
                }
            }
            System.out.println("Reading "+fileName+" is completed... There are "+numColumns+" attributes in the file..");
            
            // Example: Printing the data from the arrays
            for (int i = 0; i < numColumns; i++) {
                System.out.println("Attribute " + headers[i] + ": " + String.join(", ", dataArrays[i]));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
	}

   public void createDatabase() 
   {
        String jdbcUrl = "jdbc:mysql://localhost:3306/";
        Connection connection = null;
        String databaseName = "SCC201LAB";

        try 
        {
            connection = DriverManager.getConnection(jdbcUrl);
            Statement statement = connection.createStatement();
            String createDatabaseQuery = "CREATE DATABASE " + databaseName;
            statement.executeUpdate(createDatabaseQuery);
            System.out.println("Database '" + databaseName + "' created successfully.");
        } 
        catch (SQLException e) 
        {
            e.printStackTrace();
        } 
        finally 
        {
            try 
            {
                if (connection != null) 
                {
                    connection.close();
                }
            } 
            catch (SQLException ex) 
            {
                ex.printStackTrace();
            }
        }
    }


    public static void main(String[] args) 
    {
        // Replace "your_file.csv" with the path to your CSV file
        String PalBasicCSVFile = "pals_basic.csv";
        String PalStatCSVFile = "pal_stats.csv";
        
        NativeCSVReader urazsReader = new NativeCSVReader();
        System.out.println("Reading the "+PalBasicCSVFile+" now");
        urazsReader.reader(PalBasicCSVFile);
        System.out.println("Reading the "+PalStatCSVFile+" now");
        urazsReader.reader(PalStatCSVFile);
        urazsReader.createDatabase();
    }
}
