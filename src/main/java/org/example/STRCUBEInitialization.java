package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class STRCUBEInitialization {
    public static void main(String[] args) {

        String dimensionsDirectory = "./dimensions/";
        String xmlFileName = "DMInstance.xml";
        dropDatabase("STOCKMARKETSTRCUBE");
        DimensionalProcessing Dimproc = new DimensionalProcessing();
        Dimproc.CreateMetaDT();
        Dimproc.GenerateDTs(dimensionsDirectory, xmlFileName);
        FactTableProcessing factTableProcessing=new FactTableProcessing();
        factTableProcessing.GenerateFT(dimensionsDirectory,xmlFileName);
        CubeProcessing cubeProcessing = new CubeProcessing();
        cubeProcessing.start(dimensionsDirectory+xmlFileName);
        SummaryTableGeneration summaryTableGeneration =new SummaryTableGeneration();
        summaryTableGeneration.createSummaryTable();
    }
    public static void dropDatabase(String dbName){
        DBConfig dbConfig=new DBConfig();
        try (Connection conn = DriverManager.getConnection(dbConfig.getUrl(), dbConfig.getUsername(), dbConfig.getPassword());
             Statement stmt = conn.createStatement()) {
            String sql = "DROP DATABASE IF EXISTS " + dbName;
            stmt.executeUpdate(sql);
            System.out.println("Database " + dbName + " dropped successfully.");
        } catch (SQLException e) {
            System.out.println("Database " + dbName + " could not be dropped.");
            e.printStackTrace();
        }
    }
}
