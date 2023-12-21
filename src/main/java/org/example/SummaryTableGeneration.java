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

public class SummaryTableGeneration {
    DBConfig dbConfig = new DBConfig();
    String url = dbConfig.getUrl();
    String username = dbConfig.getUsername();
    String password = dbConfig.getPassword();

    public SummaryTableGeneration() {
    }
    public void createSummaryTable() {
        try{
            File inputFile = new File("./dimensions/DMInstance.xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(inputFile);
            doc.getDocumentElement().normalize();

            NodeList summaryItemList = doc.getElementsByTagName("SummaryItem");
            String[] columnNames = new String[summaryItemList.getLength()];
            String[] columnTypes = new String[summaryItemList.getLength()];
            String[] columnProperties = new String[summaryItemList.getLength()];

            for (int i = 0; i < summaryItemList.getLength(); i++) {
                Element summaryItem = (Element) summaryItemList.item(i);
                columnNames[i] = summaryItem.getTextContent();
                columnTypes[i] = summaryItem.getAttribute("type");
                columnProperties[i] = summaryItem.getAttribute("property");
//                System.out.println(columnNames[i] + columnTypes[i]+ columnProperties[i]);
            }


            String sql = "CREATE TABLE IF not EXISTS SUMMARY " + " (";
            for (int j = 0; j < columnNames.length; j++) {
                sql += columnNames[j] + " " + columnTypes[j] + " " + columnProperties[j];
                if (j < columnNames.length - 1) {
                    sql += ", ";
                }
            }
            sql += ")";
            System.out.println(sql);


            NodeList queryList = doc.getElementsByTagName("Query");
            String[] queryIdList = new String[queryList.getLength()];
            String[] queryFactVariableList = new String[queryList.getLength()];
            String[] queryTypeList = new String[queryList.getLength()];
            String[] queryTableList = new String[queryList.getLength()];
            String[] queryLabelList = new String[queryList.getLength()];
            for(int i=0;i<queryList.getLength();i++){
                Element element=(Element) queryList.item(i);
                queryIdList[i]=element.getAttribute("id");
                queryLabelList[i] = element.getElementsByTagName("QueryLabel").item(0).getTextContent();
                queryFactVariableList[i] = element.getElementsByTagName("FactVariable").item(0).getTextContent();
                queryTypeList[i]=queryIdList[i].charAt(0)=='a'?"Aggregate":"Generic";
                queryTableList[i]="QUERY_RESULT_"+queryIdList[i];
            }

            String insert[] = new String[queryList.getLength()];
            String header = "";
            for (int i = 0; i < columnNames.length - 4; i++) {
                header += columnNames[i];
                if (i < columnNames.length - 5)
                    header += ",";
            }
            for(int i=0;i<insert.length;i++){
                insert[i]="INSERT INTO SUMMARY VALUES ('"+queryIdList[i]+"','"+queryTypeList[i]+"','"+queryLabelList[i]+"','"+queryTableList[i]+"')";
                System.out.println(insert[i]);
            }

            try (Connection conn = DriverManager.getConnection(url, username, password);
                 Statement stmt = conn.createStatement();
            ) {

                stmt.executeUpdate(sql);
              for(int i=0;i<insert.length;i++){
                  stmt.executeUpdate(insert[i]);
              }

            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        catch (Exception e){
            e.printStackTrace();
        }

    }
}
