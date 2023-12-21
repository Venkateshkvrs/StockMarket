package org.example;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


public class QueriesProcessing {
    public static DBConfig dbConfig = new DBConfig();
    public static String url = dbConfig.getUrl();
    public static String username = dbConfig.getUsername();
    public static String password = dbConfig.getPassword();

    public QueriesProcessing() {
    }

    public QueriesProcessing(Connection conn) {
        this.conn = conn;
    }

    private Connection conn;

    public void updateQueryResult(String queryId, String aggregateFunction, ResultSet rs, String queryScript) throws SQLException {
        Statement stmt = conn.createStatement();
        String QUERY_TABLE_NAME = "QUERY_RESULT_" + queryId;
        ResultSetMetaData rsMetaData = rs.getMetaData();
        StringJoiner columnDefs = new StringJoiner(", ");

        int columnCount = rsMetaData.getColumnCount();
        String[] columnNames = new String[columnCount];

        //To get metadata for creating table
        for (int i = 1; i <= columnCount; i++) {
            columnNames[i - 1] = rsMetaData.getColumnName(i);
            String name = rsMetaData.getColumnName(i);
            String type = rsMetaData.getColumnTypeName(i);
            int length = rsMetaData.getColumnDisplaySize(i);
            if (type.contentEquals("DECIMAL")) {
                columnDefs.add(name + " " + type + "( 20,3 )");
            } else {
                columnDefs.add(name + " " + type + "(" + length + ")");
            }
        }
        String createTableQuery = "CREATE TABLE IF NOT EXISTS " + QUERY_TABLE_NAME + " ( id INT AUTO_INCREMENT PRIMARY KEY , " + columnDefs + ")";
        System.out.println(createTableQuery);
        Statement stmtCreate = conn.createStatement();
        stmtCreate.executeUpdate(createTableQuery);
        System.out.println("RS = " + queryScript);
        while (rs.next()) {
            String[] values = new String[columnCount];
            //getting current row values
            for (int i = 0; i < values.length; i++) {
                values[i] = rs.getString(i + 1);
                System.out.println("value " + (i + 1) + " : " + values[i]);
            }
             //System.out.println("SELECT * FROM " + QUERY_TABLE_NAME + " WHERE " + getWhereClause(values, rs));
            //This will generate a where clause basically all values with column name ex C1 = "a" and C2 ="b"
            String whereCondition = getWhereClause(values, rs);
            ResultSet rsOld;
            if(whereCondition.length() > 0){
                rsOld = stmt.executeQuery("SELECT * FROM " + QUERY_TABLE_NAME + " WHERE " + whereCondition );
            }
            else{
                rsOld = stmt.executeQuery("SELECT * FROM " + QUERY_TABLE_NAME );
            }
            //check if a row exists with where condition
            if (rsOld.next()) {
                System.out.println("Row found in query table: ");
                String result = rs.getString("result");
                System.out.println(result);
                int id = rsOld.getInt("id");
                String updateStmt = "";
                switch (aggregateFunction) {
                    case "SUM", "COUNT" -> updateStmt = "Result = Result + ?";
                    case "MIN" -> updateStmt = "Result = LEAST(Result , ?)";
                    case "MAX" -> updateStmt = "Result = GREATEST(Result , ?)";
                    case "AVG" -> {
                        generateTempTables(queryId, queryScript, getWhereClause(values, rs));
                        Double sum = 0.0, count = 0.0, avg;
                        Statement getSumQuery = conn.createStatement();
                        Statement getCountQuery = conn.createStatement();
                        String tempSumTable = "QUERY_RESULT_ts" + queryId;
                        String tempCountTable = "QUERY_RESULT_tc" + queryId;
                        String whereClause = getWhereClause(values, rs);
                        ResultSet rsSum = null;
                        ResultSet rsCount = null;
                        //Matching row from sum and count table
                        if(whereClause.length() > 0){
                            rsSum = getSumQuery.executeQuery("SELECT result FROM " + tempSumTable + " WHERE " + getWhereClause(values, rs));
                            rsCount = getCountQuery.executeQuery("SELECT result FROM " + tempCountTable + " WHERE " + getWhereClause(values, rs));
                        }
                        else{
                            rsSum = getSumQuery.executeQuery("SELECT result FROM " + tempSumTable );
                            rsCount = getCountQuery.executeQuery("SELECT result FROM " + tempCountTable );
                        }

                        if (rsSum.next()) {
                            sum = Double.parseDouble(rsSum.getString("result"));
                        }
                        if (rsCount.next()) {
                            count = Double.parseDouble(rsCount.getString("result"));
                        }
                        avg = sum / count;
                        System.out.println("SUM " + sum);
                        System.out.println("COUNT " + count);
                        System.out.println("AVG " + avg);
                        result = String.valueOf(avg);
                        updateStmt = "Result = ?";
                    }
                }
                PreparedStatement stmtUpdate = conn.prepareStatement("UPDATE " + QUERY_TABLE_NAME + " SET " + updateStmt + " WHERE id=?");
                stmtUpdate.setDouble(1, Double.parseDouble(result));
                stmtUpdate.setInt(2, id);
                System.out.println(stmtUpdate);
                stmtUpdate.executeUpdate();
            } else {
                System.out.println("Row not found in query table: ");
                if (aggregateFunction.contentEquals("AVG")) {
                    generateTempTables(queryId, queryScript, getWhereClause(values, rs));
                }
                StringBuilder insertQuery = new StringBuilder("INSERT INTO " + QUERY_TABLE_NAME + " (" + String.join(", ", columnNames) + ") VALUES (");
                for (int i = 0; i < values.length; i++) {
                    insertQuery.append("'").append(values[i]).append("'");
                    if (i < values.length - 1) {
                        insertQuery.append(",");
                    }
                }
                insertQuery.append(")");
                PreparedStatement stmtInsert = conn.prepareStatement(insertQuery.toString());
                System.out.println(stmtInsert.toString());
                stmtInsert.executeUpdate();

            }
        }
    }

    public void generateTempTables(String queryId, String queryScript, String whereClause) throws SQLException {
        System.out.println("START GENERATTING TEMP TABLES");
        Statement tempSumStmt = conn.createStatement();
        Statement tempCountStmt = conn.createStatement();
        String tempSumQuery = queryScript.replace("AVG", "SUM").toLowerCase();
        String tempCountQuery = queryScript.replace("AVG", "COUNT").toLowerCase();


        //adding where clause before group by script
        String groupByKeyword = "group by";
        int index = tempSumQuery.indexOf(groupByKeyword);
        if (index != -1) {
            index += tempSumQuery.substring(index).indexOf(groupByKeyword);
            tempSumQuery = tempSumQuery.substring(0, index) + "WHERE " + whereClause + " " + tempSumQuery.substring(index);
        }
        index = tempCountQuery.indexOf(groupByKeyword);
        if (index != -1) {
            index += tempCountQuery.substring(index).indexOf(groupByKeyword);
            tempCountQuery = tempCountQuery.substring(0, index) + "WHERE " + whereClause + " " + tempCountQuery.substring(index);
        }

        System.out.println("Start index of GROUP BY: " + index);
        System.out.println(tempSumQuery);
        System.out.println(tempCountQuery);
        ResultSet sumRs = tempSumStmt.executeQuery(tempSumQuery);
        ResultSet countRs = tempCountStmt.executeQuery(tempCountQuery);

        updateQueryResult("ts" + queryId, "SUM", sumRs, tempSumQuery);
        updateQueryResult("tc" + queryId, "COUNT", countRs, tempCountQuery);
        System.out.println("FINISH GENERATTING TEMP TABLES");
    }

    public void updateAggregationResult(String queryId, ResultSet rs) throws SQLException {
        Statement stmt = conn.createStatement();
        String QUERY_TABLE_NAME = "QUERY_RESULT_" + queryId;
        ResultSetMetaData rsMetaData = rs.getMetaData();
        StringJoiner columnDefs = new StringJoiner(", ");

        int columnCount = rsMetaData.getColumnCount();
        String[] columnNames = new String[columnCount];
        //getting metadata for table creation
        for (int i = 1; i <= columnCount; i++) {
            columnNames[i - 1] = rsMetaData.getColumnName(i);
            String name = rsMetaData.getColumnName(i);
            String type = rsMetaData.getColumnTypeName(i);
            if (type.contentEquals("BIGINT")) {
                columnDefs.add(name + " " + type + "( 20 )");
            } else {
                columnDefs.add(name + " " + type + "( 20,3 )");
            }
        }

//        DatabaseMetaData metadata = conn.getMetaData();
//        ResultSet resultSet = metadata.getTables(null, null, QUERY_TABLE_NAME, new String[] {"TABLE"}    );
//        boolean tableExists = resultSet.next();
//        System.out.println(tableExists);

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT count(*) "
                + "FROM information_schema.tables "
                + "WHERE table_name = ? and TABLE_SCHEMA= ? "
                + "LIMIT 1;");
        preparedStatement.setString(1, QUERY_TABLE_NAME);
        preparedStatement.setString(2, "STOCKMARKETSTRCUBE");

        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        boolean tableExists= resultSet.getInt(1) != 0;
        System.out.println(tableExists);
        if (!tableExists) {
            String createTableQuery = "CREATE TABLE IF NOT EXISTS " + QUERY_TABLE_NAME + " ( " + columnDefs + " )";
            System.out.println(createTableQuery);
            Statement stmtCreate = conn.createStatement();
            stmtCreate.executeUpdate(createTableQuery);
        }

        if (rs.next()) {
            String resultSum = rs.getString("sum");
            String resultCount = rs.getString("count");
            String resultMin = rs.getString("min");
            String resultMax = rs.getString("max");
            if (!tableExists) {
                String updateStmt = "( ? , ? , ( ?/? ) , ? ,? )";
                PreparedStatement stmtUpdate = conn.prepareStatement("INSERT INTO " + QUERY_TABLE_NAME + " VALUES " + updateStmt + "");
                stmtUpdate.setInt(1, Integer.parseInt(resultSum));
                stmtUpdate.setInt(2, Integer.parseInt(resultCount));
                stmtUpdate.setInt(3, Integer.parseInt(resultSum));
                stmtUpdate.setInt(4, Integer.parseInt(resultCount));
                stmtUpdate.setInt(5, Integer.parseInt(resultMin));
                stmtUpdate.setInt(6, Integer.parseInt(resultMax));
                System.out.println(stmtUpdate);
                stmtUpdate.executeUpdate();
                return;
            }
            String updateStmt = "sum = ? + sum , count = ? + count ,avg = ?, min = LEAST(min , ?) , max = GREATEST(max,?) ";
            PreparedStatement stmtUpdate = conn.prepareStatement("UPDATE " + QUERY_TABLE_NAME + " SET " + updateStmt + "");
            stmtUpdate.setInt(1, Integer.parseInt(resultSum));
            stmtUpdate.setInt(2, Integer.parseInt(resultCount));
            stmtUpdate.setInt(3, 0);
            stmtUpdate.setInt(4, Integer.parseInt(resultMin));
            stmtUpdate.setInt(5, Integer.parseInt(resultMax));
            System.out.println(stmtUpdate);
            stmtUpdate.executeUpdate();
            PreparedStatement stmtUpdateAvg = conn.prepareStatement("UPDATE " + QUERY_TABLE_NAME + " SET avg = (sum/count) ");
            stmtUpdateAvg.executeUpdate();
        }
    }


    public void generateLog(String queryId, Timestamp timestamp) throws SQLException {
        String QUERY_TABLE_NAME = "QUERY_RESULT_" + queryId;
        String LOG_TABLE_NAME = "QUERY_LOG_" + queryId;
        DatabaseMetaData metadata = conn.getMetaData();
//        ResultSet resultSet = metadata.getTables(null, null, LOG_TABLE_NAME, new String[] {"TABLE"}    );
//        boolean tableExists = resultSet.next();
        PreparedStatement preparedStatement = conn.prepareStatement("SELECT count(*) "
                + "FROM information_schema.tables "
                + "WHERE table_name = ? and TABLE_SCHEMA= ? "
                + "LIMIT 1;");
        preparedStatement.setString(1, LOG_TABLE_NAME);
        preparedStatement.setString(2, "STOCKMARKETSTRCUBE");

        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        boolean tableExists= resultSet.getInt(1) != 0;
        if (!tableExists) {
            String createTableQuery = "CREATE TABLE IF NOT EXISTS " + LOG_TABLE_NAME + " AS SELECT * , ? as timestamp FROM " + QUERY_TABLE_NAME;
            System.out.println(createTableQuery);
            PreparedStatement prepStmt = conn.prepareStatement(createTableQuery);
            prepStmt.setTimestamp(1, timestamp);
            prepStmt.executeUpdate();
            return;
        }

        String updateTableQuery = "INSERT INTO " + LOG_TABLE_NAME + "  SELECT * , ? as timestamp FROM " + QUERY_TABLE_NAME;
        System.out.println(updateTableQuery);
        PreparedStatement prepStmt = conn.prepareStatement(updateTableQuery);
        prepStmt.setTimestamp(1, timestamp);
        prepStmt.executeUpdate();

    }


    private String getWhereClause(String[] values, ResultSet rsNew) throws SQLException {
        //This will generate a where clause basically all values with column name ex C1 = "a" and C2 ="b"
        StringBuilder whereClause = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            String columnName = rsNew.getMetaData().getColumnName(i + 1);
            if (columnName.equals("result")) {
                continue; // skip "result" column
            }
            whereClause.append((whereClause.length() == 0) ? "" : " AND ").append(columnName).append("='").append(values[i]).append("'");
        }
        return whereClause.toString();
    }

    public static List<Element> getQueries(String fileName) {
        List<Element> queries = new ArrayList<>();
        try {
            File inputFile = new File(fileName);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(inputFile);
            doc.getDocumentElement().normalize();
            NodeList queryNodes = doc.getElementsByTagName("Query");
            for (int i = 0; i < queryNodes.getLength(); i++) {
                Element queryElem = (Element) queryNodes.item(i);
                queries.add(queryElem);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queries;
    }


    public static void generateSummary() {
        System.out.println("Generating Summary...");
        ResultSet rs = null;
        Statement stmt = null;
        Connection conn = null;
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        try {
            conn = DriverManager.getConnection(url, username, password);
            QueriesProcessing queriesProcessing = new QueriesProcessing(conn);
            stmt = conn.createStatement();
            List<Element> queries = getQueries("./dimensions/DMInstance.xml");
            for (Element query : queries) {
                Element queryRepoElem = (Element) query.getParentNode();
                String queryRepoType = queryRepoElem.getAttribute("type");
                if (queryRepoType.equals("generic")) {
                    String queryId = query.getAttribute("id");
                    String aggregateFunction = query.getElementsByTagName("AggregateFunction").item(0).getTextContent();
                    String factVariable = query.getElementsByTagName("FactVariable").item(0).getTextContent();
                    String queryScript = query.getElementsByTagName("QueryScript").item(0).getTextContent();
                    System.out.println("Query ID: " + queryId);
                    System.out.println("Aggregate Function: " + aggregateFunction);
                    System.out.println("Fact Variable: " + factVariable);
                    System.out.println("Query Script: " + queryScript);
                    rs = stmt.executeQuery(queryScript);
                    queriesProcessing.updateQueryResult(queryId, aggregateFunction, rs, queryScript);
                    queriesProcessing.generateLog(queryId, timestamp);
                    System.out.println();
                } else if (queryRepoType.equals("aggregate")) {
                    String queryId = query.getAttribute("id");
                    String factVariable = query.getElementsByTagName("FactVariable").item(0).getTextContent();
                    System.out.println("Query ID: " + queryId);
                    System.out.println("Aggregate Function: ALL");
                    System.out.println("Fact Variable: " + factVariable);
                    Statement queryStmt = conn.createStatement();
                    String queryScript = "SELECT SUM(" + factVariable + ") as sum, COUNT(" + factVariable + ") as count, AVG(" + factVariable + ") as avg, MIN(" + factVariable + ") as min, MAX(" + factVariable + ") as max FROM mergeview ;";
                    ResultSet queryRs = queryStmt.executeQuery(queryScript);
                    System.out.println(queryScript);
                    queriesProcessing.updateAggregationResult(queryId, queryRs);
                    queriesProcessing.generateLog(queryId, timestamp);
                }

            }
            List<Element> cubeQueries = getQueries("./dimensions/Cuboids.xml");
            for (Element query : cubeQueries) {
                String queryId = query.getAttribute("id");
                String aggregateFunction = query.getElementsByTagName("AggregateFunction").item(0).getTextContent();
                String factVariable = query.getElementsByTagName("FactVariable").item(0).getTextContent();
                String queryScript = query.getElementsByTagName("QueryScript").item(0).getTextContent();
                System.out.println("Query ID: " + queryId);
                System.out.println("Aggregate Function: " + aggregateFunction);
                System.out.println("Fact Variable: " + factVariable);
                System.out.println("Query Script: " + queryScript);
                rs = stmt.executeQuery(queryScript);
                queriesProcessing.updateQueryResult(queryId, aggregateFunction, rs, queryScript);
                queriesProcessing.generateLog(queryId, timestamp);
                System.out.println();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                assert rs != null;
                rs.close();
                stmt.close();
                conn.close();
            } catch (Exception ignored) {

            }

        }

    }
}
