package org.example;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class CubeProcessing {

    public static DBConfig dbConfig = new DBConfig();
    public static String url = dbConfig.getUrl();
    public static String username = dbConfig.getUsername();
    public static String password = dbConfig.getPassword();

    public static boolean createCubeSummaryTable(){
        String sql = "CREATE TABLE IF NOT EXISTS CubeSummary ( qid varchar(20) PRIMARY KEY , hash Varchar(100)) ";
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




    public static void start(String filePath) {

        try {
            // Parse the XML code
            createCubeSummaryTable();
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(filePath);
            ArrayList<String> dimensionsArray = new ArrayList<String>();
            ArrayList<String> aggregateFunctionsArray = new ArrayList<String>();
            ArrayList<String> factVariablesArray = new ArrayList<String>();


            //Fetching Data from XML

            // Get the CuboidProperties element
            Element cuboidProps = (Element) doc.getElementsByTagName("CuboidProperties").item(0);

            // Get the Dimensions element
            Element dimensions = (Element) cuboidProps.getElementsByTagName("Dimensions").item(0);

            // Get the list of Dimension elements
            NodeList dimList = dimensions.getElementsByTagName("Dimension");

            // Get the AggregateFunctions element
            Element aggFunc = (Element) cuboidProps.getElementsByTagName("AggregateFunctions").item(0);

            // Get the list of AggregateFunction elements
            NodeList funcList = aggFunc.getElementsByTagName("AggregateFunction");

            Element factVariables = (Element) cuboidProps.getElementsByTagName("FactVariables").item(0);

            // Get the list of AggregateFunction elements
            NodeList factVariablesList = factVariables.getElementsByTagName("FactVariable");

            // Create a new DocumentBuilderFactory
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

            // Use the factory to create a new DocumentBuilder
            DocumentBuilder builder = factory.newDocumentBuilder();

            // Create a new Document
            Document xmlDoc = builder.newDocument();

            // Create root element
            Element root = xmlDoc.createElement("Queries");
            xmlDoc.appendChild(root);

            System.out.println("CUBE PROCESSING");
            System.out.println("List of Dimensions:");
            for (int i = 0; i < dimList.getLength(); i++) {
                Element dim = (Element) dimList.item(i);
                String dimValue = dim.getTextContent();
                dimensionsArray.add(dimValue);
                System.out.println(dimValue);
                Collections.sort(dimensionsArray);
            }

            System.out.println("\nList of Aggregate Functions:");
            for (int i = 0; i < funcList.getLength(); i++) {
                Element func = (Element) funcList.item(i);
                String funcValue = func.getTextContent();
                System.out.println(funcValue);
                aggregateFunctionsArray.add(funcValue);
            }

            System.out.println("\nList of Fact variables:");
            for (int i = 0; i < factVariablesList.getLength(); i++) {
                String factVariable = factVariablesList.item(i).getTextContent();
                System.out.println(factVariable);
                factVariablesArray.add(factVariable);
            }
            System.out.println();

            ArrayList<String> dimCombinationsList = generateCombinations(dimensionsArray);

            System.out.println("Combinations LIST");
            for(String dm : dimCombinationsList){

                System.out.println(dm);
            }


            try (Connection conn = DriverManager.getConnection(url, username, password);
                 Statement stmt = conn.createStatement()) {

                conn.setAutoCommit(false); // Disable auto-commit mode

                int i = 1;
                for (String dimCombination : dimCombinationsList) {
                    for (String aggregateFunction : aggregateFunctionsArray) {
                        for (String factV : factVariablesArray) {
                            String hash = generateHash(dimCombination, aggregateFunction, factV);
                            String query = generateQueryString(dimCombination, aggregateFunction, factV);
                            String queryId = "s"+i;
                            Element xmlQuery = createQueryElement(xmlDoc, queryId, hash, aggregateFunction, factV,
                                    dimCombination.split(","),
                                    query, "System Genrerated");
                            root.appendChild(xmlQuery);
                            String tableName = "CubeSummary";
                            String sql = "INSERT INTO " + tableName +
                                    "(qid, hash)  VALUES ( '" + queryId + "' , '" + hash +"' )";
                            System.out.println(sql);
                            stmt.addBatch(sql);
                            i++;
                        }
                    }
                }
                int[] rowsAffected = stmt.executeBatch(); // Execute the batch

                conn.commit(); // Commit the transaction

                System.out.println(rowsAffected.length + " rows inserted successfully.");
            }
            catch (SQLException ex) {
                ex.printStackTrace();
            }

            File file = new File("./dimensions/Cuboids.xml");
            if (file.exists()) {
                file.delete();
                System.out.println("File deleted successfully.");
            }
            javax.xml.transform.TransformerFactory.newInstance().newTransformer()
                    .transform(new javax.xml.transform.dom.DOMSource(xmlDoc),
                            new javax.xml.transform.stream.StreamResult(file));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static  String generateQueryString(String dimCombination, String aggregateFunction , String factV){
        StringBuilder select = new StringBuilder();

        // Add the FROM and GROUP BY clauses
        if (dimCombination.length() < 1) {
            select.append("SELECT ");

            select.append(aggregateFunction + "(" + factV + ") , ");

            int lastSpaceIndex = select.lastIndexOf(" ,");
            if (lastSpaceIndex >= 0) { // Make sure the space was found
                select.setLength(lastSpaceIndex); // Delete everything after the last space
            }

            select.append(" as result FROM mergeview");
        } else {
            select.append("SELECT " + dimCombination + ", ");
            select.append(aggregateFunction + "(" + factV + ") , ");
            int lastSpaceIndex = select.lastIndexOf(" ,");
            if (lastSpaceIndex >= 0) { // Make sure the space was found
                select.setLength(lastSpaceIndex); // Delete everything after the last space
            }
            select.append(" as result FROM mergeview GROUP BY " + dimCombination);
        }

        //System.out.println(select);
        return  select.toString();
    }

    public static String generateHash(String dimCombination, String aggregateFunction , String factV) throws NoSuchAlgorithmException {

        String concatenated = "";
        if(dimCombination.length() > 0){
            concatenated = String.join(",",aggregateFunction , factV , dimCombination);
        }
        else{
            concatenated = String.join(",",aggregateFunction , factV );
        }
        System.out.println("concatenated string required for hash "+ concatenated);
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] hashBytes = md.digest(concatenated.getBytes());

        // Convert the hash bytes to a hex string
        StringBuilder sb = new StringBuilder();
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        String hash = sb.toString();
        //System.out.println(hash);
        return hash;

    }

    private static Element createQueryElement(Document doc, String id, String hash, String aggFunction, String factVariable,
                                              String[] dimVariables, String queryScript, String queryLabel) {
        // Create Query element with id attribute
        Element query = doc.createElement("Query");
        query.setAttribute("id", id);

        // Add AggregateFunction element to Query
        Element aggFunctionElement = doc.createElement("AggregateFunction");
        aggFunctionElement.appendChild(doc.createTextNode(aggFunction));
        query.appendChild(aggFunctionElement);

        Element hashElement = doc.createElement("Hash");
        hashElement.appendChild(doc.createTextNode(hash));
        query.appendChild(hashElement);

        // Add FactVariable element to Query
        Element factVariableElement = doc.createElement("FactVariable");
        factVariableElement.appendChild(doc.createTextNode(factVariable));
        query.appendChild(factVariableElement);

        // Add DimensionVariables element to Query
        Element dimVariablesElement = doc.createElement("DimensionVariables");
        query.appendChild(dimVariablesElement);


        for (String dimVar : dimVariables) {
            Element dimVariableElement = doc.createElement("DimensionVariable");
            dimVariableElement.appendChild(doc.createTextNode(dimVar));
            dimVariablesElement.appendChild(dimVariableElement);
        }

        // Add QueryScript element to Query
        Element queryScriptElement = doc.createElement("QueryScript");
        queryScriptElement.appendChild(doc.createTextNode(queryScript));
        query.appendChild(queryScriptElement);

        // Add QueryLabel element to Query
        Element queryLabelElement = doc.createElement("QueryLabel");
        queryLabelElement.appendChild(doc.createTextNode(queryLabel));
        query.appendChild(queryLabelElement);

        return query;
    }

    public static ArrayList<String> generateCombinations(ArrayList<String> input) {
        ArrayList<String> output = new ArrayList<String>();

        for (int i = 0; i < (1 << input.size()); i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < input.size(); j++) {
                if ((i & (1 << j)) > 0) {
                    sb.append(input.get(j)).append(",");
                }
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            output.add(sb.toString());
        }

        return output;
    }


}

