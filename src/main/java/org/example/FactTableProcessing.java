package org.example;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class FactTableProcessing {
    DBConfig dbConfig=new DBConfig();
    String url = dbConfig.getUrl();
    String username = dbConfig.getUsername();
    String password = dbConfig.getPassword();
    public FactTableProcessing() {
    }
    public boolean GenerateFT(String workingDir, String xmlFileName){
        String xmlFile = workingDir.concat(xmlFileName);
        try{
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(new File(xmlFile));
            doc.getDocumentElement().normalize();

            NodeList dimensionTableList = doc.getElementsByTagName("DimensionVariableKey");
            String[] dimensionTableNames = new String[dimensionTableList.getLength()];
            String[] dimensionKeyNames = new String[dimensionTableList.getLength()];

            for(int i=0;i<dimensionTableList.getLength();i++){
                Element dimensionTable = (Element) dimensionTableList.item(i);
                dimensionTableNames[i] = dimensionTable.getAttribute("DimensionTable");
                dimensionKeyNames[i]=dimensionTable.getTextContent();
//                System.out.println(dimensionTableNames[i]);
//                System.out.println(factVariableList.item(i).getAttribute());
            }

//            NodeList tempList = doc.getElementsByTagName("DimensionVariableKey");
//            System.out.println(tempList.getLength());
//            System.out.println(doc.getElementsByTagName("FactVariables").item(0).getChildNodes().getLength());
//            NodeList factVariableList = doc.getElementsByTagName("FileContents").item(0).getChildNodes().item(3).getChildNodes();
            int total=doc.getElementsByTagName("FactVariables").item(0).getChildNodes().getLength()/2;
            NodeList factVariableList = doc.getElementsByTagName("FactVariable");
//            System.out.println(factVariableList.getLength());
            String[] factVariableNames = new String[factVariableList.getLength()];
            String[] factVariableTypes = new String[factVariableList.getLength()];
            String[] factVariableProperties = new String[factVariableList.getLength()];

            for(int i=0;i<total;i++){
                Element factVariable = (Element) factVariableList.item(i);
                factVariableNames[i] = factVariable.getTextContent();
                factVariableTypes[i] = factVariable.getAttribute("type");
                factVariableProperties[i] = factVariable.getAttribute("property");

//                System.out.println(factVariableNames[i]);
//                System.out.println(factVariableList.item(i).getAttribute());
            }

            String sql = "CREATE TABLE FactTable" + " (";
            for (int j = 0; j < dimensionTableNames.length; j++) {
                sql += dimensionKeyNames[j] + " " + "VARCHAR(255)" + " " + "NOT NULL";
//                if (j < dimensionTableNames.length - 1) {
                    sql += ", ";
//                }
            }
            for (int j = 0; j < total; j++) {
                sql += factVariableNames[j] + " " + factVariableTypes[j] + " " + factVariableProperties[j];
                if (j < total - 1) {
                    sql += ", ";
                }
            }
            sql += ")";

            System.out.println(sql);

            String[] sqlalter=new String[dimensionTableNames.length];

            for(int i=0;i<sqlalter.length;i++){
                sqlalter[i]="alter table FactTable add constraint fk_"+dimensionKeyNames[i]+" FOREIGN KEY ("+dimensionKeyNames[i]+") REFERENCES "+dimensionTableNames[i]+"("+dimensionKeyNames[i]+")";
            }

            try (Connection conn = DriverManager.getConnection(url, username, password);
                 Statement stmt = conn.createStatement()) {
                        stmt.executeUpdate(sql);
                System.out.println("[ Table FactTable created successfully... ]");
                for(int i=0;i<sqlalter.length;i++){
                    stmt.executeUpdate(sqlalter[i]);
                }
                System.out.println("[ Foreign Key Constraints added successfully... ]");

            } catch (SQLException e) {
                System.out.println(sqlalter[0]+" "+e.getMessage());
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }
}
