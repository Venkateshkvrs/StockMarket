package org.example;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Timer;
import java.util.TimerTask;
public class NewStreamProcessing {
    // Database Credentials
    public static DBConfig dbConfig=new DBConfig();
    public static String url = dbConfig.getUrl();
    public static String username = dbConfig.getUsername();
    public static String password = dbConfig.getPassword();
    /* Offset: Stores Number of Bytes to Skip to get to Start of Window... */
    public static int Offset = 0;

    public static int curr = 0;

    public NewStreamProcessing() throws FileNotFoundException {
    }

    public static void startStreamService(String factsDir ,String dimensionDir, String xmlFileName) throws SQLException, IOException {
        int Size = 0;
        int Velocity = 0;
        int TimeClick = 0;
        String csvPath = "";
        File csvFile = null;
        String[] columnTypes = null;
        QueriesProcessing queriesProcessing = new QueriesProcessing();
        // Process XML Instance & Get DataSource Properties
        try {
            File inputFile = new File(dimensionDir+xmlFileName);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(inputFile);
            doc.getDocumentElement().normalize();
            NodeList dataSourceList = doc.getElementsByTagName("DataSource");
            for (int i = 0; i < dataSourceList.getLength(); i++) {
                Element dataSource = (Element) dataSourceList.item(i);
                // Getting Column Types of Fact Table from XML...
                NodeList DimensionVariableKeysList =  dataSource.getElementsByTagName("DimensionVariableKey");
                NodeList FactVariablesList =  dataSource.getElementsByTagName("FactVariable");
                columnTypes = new String[DimensionVariableKeysList.getLength()+FactVariablesList.getLength()];
                int ind = 0;
                for(; ind < DimensionVariableKeysList.getLength(); ind++){
                    Element DimensionVariableKey = (Element) DimensionVariableKeysList.item(ind);
                    columnTypes[ind] = DimensionVariableKey.getAttribute("type");
                }
                for(int j=0 ; j < FactVariablesList.getLength(); j++){
                    Element FactVariable = (Element) FactVariablesList.item(j);
                    columnTypes[ind++] = FactVariable.getAttribute("type");
                }
                // Getting Window Properties...
                String size = dataSource.getElementsByTagName("size").item(0).getTextContent();
                String sizeUnits = dataSource.getElementsByTagName("size").item(0).getAttributes().getNamedItem("units").getTextContent();
                String time = dataSource.getElementsByTagName("time").item(0).getTextContent();
                String timeUnits = dataSource.getElementsByTagName("time").item(0).getAttributes().getNamedItem("units").getTextContent();
                String filePath = dataSource.getElementsByTagName("FilePath").item(0).getTextContent();
                String filetype = dataSource.getElementsByTagName("FileType").item(0).getTextContent();
                String velocity = dataSource.getElementsByTagName("velocity").item(0).getTextContent();
                String velocityUnits = dataSource.getElementsByTagName("velocity").item(0).getAttributes().getNamedItem("units").getTextContent();


                Size = Integer.parseInt(size);
                Velocity = Integer.parseInt(velocity);
                System.out.println("Size : " + size + " " + sizeUnits);
                System.out.println("Clock Tick : " + time + " " + timeUnits);
                System.out.println("Velocity : " + velocity + " " + velocityUnits);

                if(timeUnits.contentEquals("seconds")){
                    TimeClick = Integer.parseInt(time) * 1000;
                }
                else{
                    TimeClick = Integer.parseInt(time);
                }
                System.out.println("File Path : " + filePath);

                csvPath = factsDir.concat(filePath.concat(filetype));
                csvFile = new File(csvPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Setup  Database Connection
        Connection conn = DriverManager.getConnection(url,username,password);
        // Read Header of csv File into 'header'...
        BufferedReader reader = new BufferedReader(new FileReader(csvFile));
        String header = reader.readLine();
        byte[] bytes = header.getBytes(StandardCharsets.UTF_8);
        //System.out.println("Bytes Read" + bytes.length);
        Offset += bytes.length;
        //System.out.println(header);
        reader.close();
        // Setting Timer Configuration & Adding Method Call to Sliding Window...
        Timer timer = new Timer();
        File finalCsvFile = csvFile;
        int finalVelocity = Velocity;
        int finalSize = Size;
        String[] finalColumnTypes = columnTypes;
        curr = finalSize;
        String joinTables = getDimentionTables();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    int newRows=0;
                    newRows = SlidingWindow(conn, header, finalCsvFile, finalVelocity, finalSize, finalColumnTypes);
                    System.out.println("New Lines Added: "+ newRows);
                    if(newRows > 0) {
                    String createBaseViewQuery =
                    "CREATE OR REPLACE VIEW  mergeview as with filterFactTable as (select * , ROW_NUMBER()  OVER () AS rn from FactTable order by rn desc limit " + newRows+ ") Select * from filterFactTable "+joinTables;
                    System.out.println(createBaseViewQuery);
                    Statement createmergeviewStmt = conn.createStatement();
                    createmergeviewStmt.executeUpdate(createBaseViewQuery);
                        queriesProcessing.generateSummary();
                    }

                } catch (IOException | SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 0, TimeClick);
    }

    public static String getDimentionTables() throws SQLException {
        Connection conn = DriverManager.getConnection(url,username,password);
        Statement dimensionTables = conn.createStatement();
        StringBuilder joinTables = new StringBuilder("");
        ResultSet rsTables = dimensionTables.executeQuery("SELECT * FROM DimensionTables");
        while (rsTables.next()) {
            String tableName = rsTables.getString("DimensionTable");
            joinTables.append(" NATURAL JOIN " + tableName);
        }
        return  joinTables.toString();

    }


    /* Sliding Window Method Definition... */
    public static int SlidingWindow(Connection conn, String header, File csvFile, int Velocity, int Size, String[] columnTypes) throws IOException, SQLException {
        System.out.println("...............................");
        System.out.println("[Clearing fact table...]");
        // Preparing to Truncate Table: FactTable (Simply Drops and Creates)
        Statement statement = conn.createStatement();
        String truncateFactTableQuery = "TRUNCATE TABLE FactTable";
        statement.executeUpdate(truncateFactTableQuery);
        System.out.println("[Fact table Truncated Successfully...]");
        // Processing csv File
        System.out.println("[Adding Stream Data...]");
        FileReader fileReader1 = new FileReader(csvFile);
        fileReader1.skip(Offset+1);
        FileReader fileReader2 = new FileReader(csvFile);
        fileReader2.skip(Offset+1);
        // For Holding Start of Window
        BufferedReader R = new BufferedReader(fileReader1);
        // For Counting Next Available Window
        BufferedReader S = new BufferedReader(fileReader2);
        int temp = Velocity, window = Size;
        while (window > 0){
            String csvLine = S.readLine();
            String[] values = csvLine.split(",");
            // Inserting Window Data: Row by Row...
            String insertSql = "INSERT INTO FactTable (" + header;
            insertSql += ") VALUES (";
            for (int i = 0; i < values.length; i++) {
                if (columnTypes[i].equals("VARCHAR(255)")) {
                    insertSql += "'" + values[i] + "'";
                } else {
                    insertSql += values[i];
                }
                if (i < values.length - 1) {
                    insertSql += ", ";
                }
            }
            insertSql += ")";
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(insertSql);
            System.out.println(insertSql+" Executed Successfully...");
            --window;
        }
        while (!(S.readLine() == null) && temp > 0){
            --temp;
        }
        // Evaluate Available Lines of Data...
        int avail = Velocity - temp;
        // Now, Move Window @[(Velocity >= avail) ? Velocity : avail]
        for(int i = 0 ; i < avail ; i++){
            String csvLine = R.readLine();
            byte[] lineBytes = csvLine.getBytes(StandardCharsets.UTF_8);
            Offset += lineBytes.length + 1;
        }
        int curRows = curr;
        curr = avail;
        return curRows;
    }
}
