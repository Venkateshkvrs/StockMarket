package org.example;

import java.io.*;
import java.sql.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;
public class DimensionalProcessing {

    DBConfig dbConfig=new DBConfig();
    String url = dbConfig.getUrl();
    String username = dbConfig.getUsername();
    String password = dbConfig.getPassword();
    String metaTable = "DimensionTables";
    public DimensionalProcessing(){}
    public boolean CreateMetaDT(){
        String sql = "CREATE TABLE DimensionTables " +
                "(id INT NOT NULL AUTO_INCREMENT, " +
                " DimensionTable VARCHAR(255) NOT NULL UNIQUE, " +
                " Pri_Key VARCHAR(255), " +
                " PRIMARY KEY ( id ))";
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
            System.out.println("Table created successfully");
            return true;
        } catch (SQLException e) {
            System.out.println(sql+" "+e.getMessage());
            return false;
        }
    }
    public boolean GenerateDTs(String workingDir, String xmlFileName){
        String xmlFile = workingDir.concat(xmlFileName);
        try {
            // Read the metadata from the XML file
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(new File(xmlFile));
            doc.getDocumentElement().normalize();

            // Get the column names and data types from the XML
            NodeList DimensionTableList = doc.getElementsByTagName("DimensionTable");
            String[] DimensionTableNames = new String[DimensionTableList.getLength()];
            System.out.println("[Build Started ...]");
            for (int i = 0; i < DimensionTableList.getLength(); i++) {
                String primaryKey = "";
                Element DimensionTable = (Element) DimensionTableList.item(i);
                DimensionTableNames[i] = DimensionTable.getAttribute("name");
                System.out.println(DimensionTableNames[i]);
                NodeList DimensionVariableList =  DimensionTable.getElementsByTagName("DimensionVariable");
                String[] DimensionVariableNames = new String[DimensionVariableList.getLength()];
                String[] DimensionVariableTypes = new String[DimensionVariableList.getLength()];
                String[] DimensionVariableProperties = new String[DimensionVariableList.getLength()];
                for (int j = 0; j < DimensionVariableList.getLength(); j++){
                    Element DimensionVariable = (Element) DimensionVariableList.item(j);
                    DimensionVariableNames[j] = DimensionVariable.getTextContent();
                    DimensionVariableTypes[j] = DimensionVariable.getAttribute("type");
                    DimensionVariableProperties[j] = DimensionVariable.getAttribute("property");
                    if(DimensionVariableProperties[j].equalsIgnoreCase("PRIMARY KEY")){
                        primaryKey = DimensionVariableNames[j];
                        System.out.println("Primary Key: "+primaryKey);
                    }
                    //System.out.println(DimensionVariableNames[j]+" : "+DimensionVariableTypes[j]+" : "+DimensionVariableProperties[j]);
                }
                System.out.println("************");
                /*... Persist in RDBMS ... */
                String sql = "CREATE TABLE " + DimensionTableNames[i] + " (";
                for (int j = 0; j < DimensionVariableNames.length; j++) {
                    sql += DimensionVariableNames[j] + " " + DimensionVariableTypes[j] + " " + DimensionVariableProperties[j];
                    if (j < DimensionVariableNames.length - 1) {
                        sql += ", ";
                    }
                }
                sql += ")";
                String insertSql = "INSERT INTO " + metaTable + " VALUES(null, '" + DimensionTableNames[i] + "', '" + primaryKey + "')";
                try (Connection conn = DriverManager.getConnection(url, username, password);
                     Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(sql);
                    System.out.println("[ Table " + DimensionTableNames[i] + " created successfully... ]");
                    stmt.executeUpdate(insertSql);
                    System.out.println("[ Table " + DimensionTableNames[i] + " successfully added into DimensionTables ... ]");
                } catch (SQLException e) {
                    System.out.println(sql+" "+e.getMessage());
                }
                System.out.println("*** Populating Tables Started ***");
                DimensionalProcessing DimProc = new DimensionalProcessing();
                if (DimProc.PopulateDTs(workingDir, DimensionTableNames[i], DimensionVariableNames, DimensionVariableTypes)){
                    System.out.println("Operation Success ...");
                }
                else {
                    System.out.println("Operation Failed ...");
                }
            }
            System.out.println("[Build Ended ...]");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    public boolean PopulateDTs(String workingDir, String csvFileName, String[] columnNames, String[] columnTypes) throws IOException {
        String csvFile = workingDir.concat("/");
        csvFile += csvFileName;
        csvFile += ".csv";
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement()) {
            // Read the data from the CSV file and insert it into MySQL
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                String insertSql = "INSERT INTO " + csvFileName + " (";
                for (int i = 0; i < columnNames.length; i++) {
                    insertSql += columnNames[i];
                    if (i < columnNames.length - 1) {
                        insertSql += ", ";
                    }
                }
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
                stmt.executeUpdate(insertSql);
                System.out.println(insertSql+" Executed Successfully...");
            }
            br.close();
            System.out.println("For Table: "+ csvFileName +", Values Populated Successfully...");
            return true;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

}
