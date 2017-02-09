package DBpack;

/**
 * Created by 140002949 on 15/10/16.
 */
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DB {

    String tableName=null;

    //fields required to access MySQL database
    private static String DB_URL;
    private static String DB_USERNAME;
    private static String DB_PASSWORD;

    public Connection connection = null;
    public Statement statement = null;



    public DB(File file, String table, String columns[]) throws IOException, SQLException {
        setLogin(file);

        connection = this.connectToDB();// establish connection to db
        this.statement = connection.createStatement();// setting up statements

        this.tableName =table;
        this.createTable(columns[0]);// create table

        // assigning columns to the table
        this.createColumns(columns);
    }

/// Essentials///
    // get login details
    private  void setLogin(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        String[] lines = new String[3];

        int i =0;
        while ( i<3 && (lines[i] = br.readLine()) != null) {
            i++;
        }
        br.close();

        DB_URL = lines[0];
        DB_USERNAME = lines[1];
        DB_PASSWORD = lines[2];
    }

    //connect to MySQL database
    public Connection connectToDB(){
        try {
            connection = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
        } catch (SQLException e) {
            e.getMessage();
            e.printStackTrace();
        }
        return connection;
    }

    //  -closes connection
    public  void closeConnection()throws SQLException{
        connection.close();
    }

    //add columns
    public  void createColumns(String[] headings){ // columns are : url, author, title, urlToImage, publishedAt, category, source. If you wish to add more use this method.
        try {
            for(int i=1; i<headings.length; i++){
                String query = "ALTER TABLE "+tableName+" ADD "+headings[i]+" varchar(500); ";
                statement.executeUpdate(query);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //create table
    public  void createTable(String firstColumn) throws SQLException {
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS "+tableName+"("+ firstColumn +" VARCHAR(500));");
    }

    //add record
    public void insertRecord(String[] attributes){
        PreparedStatement pS ;

        // prepare length of prepared statement
        String objects = "?";
        for(int i=1; i<attributes.length; i++){
            objects = objects +",?";
        }

        try {
            pS=connection.prepareStatement("INSERT INTO "+tableName+" VALUES("+objects+");");

            for(int i=0; i<attributes.length; i++){
                pS.setString(i+1, attributes[i]);
            }
            pS.execute();

        } catch (SQLException e) {
            e.getMessage();
            e.printStackTrace();
        }

    }


//Table parameter isn't necessary but Jack's part depends on it
    public JSONArray getRecordsBy(String Table, String column, String keyword) throws SQLException, ParseException {

        String query = null;
        if(!column.equals("publishedAt")) {
             query = "SELECT * FROM " + tableName + " WHERE " + column + " = '" + keyword + "';";
        }else{
             query = "SELECT * FROM "+ tableName;
        }
        ResultSet resultSet = statement.executeQuery(query);


        JSONArray jArray = new JSONArray();
        JSONObject obj;

        int nrOfColumns = resultSet.getMetaData().getColumnCount();// needed for the for loop below
        while(resultSet.next()){

            if(column.equals("publishedAt")){// in case times have to be compared
                if(!mostRecentComparison(resultSet.getString("publishedAt"), keyword)){
                    continue;// jump to next iteration
                }
            }

            obj = new JSONObject();
            for (int i = 1; i <= nrOfColumns; i++) {
                obj.put(resultSet.getMetaData().getColumnLabel(i), resultSet.getString(i));
                // object key = columnName:  object value = value currently being looked at
            }
            jArray.add(obj);
        }
        return jArray;
    }
    ///Essentials///

    //Helping Methods//
    private boolean mostRecentComparison(String current, String keyword) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        Date comparator = sdf.parse(keyword);
        Date currentDate = sdf.parse(current);

        if(currentDate.getTime()>=comparator.getTime()){
            return true;
        }
        return false;
    }

    // delete table functions
    public void clearTable() throws SQLException {
        statement.executeUpdate("DELETE FROM "+ tableName); // delete table entries
    }

    public void deleteTable() throws SQLException {
        statement.executeUpdate("DROP TABLE "+ tableName);  // delete entire table
    }




}

    

