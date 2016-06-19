package nl.anchormen.esjdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/**
 * Created by dinhnn on 6/11/16.
 */
public class EsJDBC {
  public static void main(String[] args) throws Exception {
    Class.forName("nl.anchormen.sql4es.jdbc.ESDriver");

    //Connection con = DriverManager.getConnection("jdbc:sql4es://10.60.102.177:9300/io-request-*?cluster.name=vtp-gw");
    Connection con = DriverManager.getConnection("jdbc:sql4es://127.0.0.1:9300/test-*");
    Statement st = con.createStatement();
// execute a query on mytype within myidx
    ResultSet rs = st.executeQuery("SELECT GROWTH(a,t,NOW('+5m')) FROM (SELECT \"@timestamp\" as t,avg(\"integerValue\")as a FROM gauge GROUP BY date_histogram(\"@timestamp\",'1m'))");
    ResultSetMetaData rsmd = rs.getMetaData();
    int nrCols = rsmd.getColumnCount();
// get other column information like type
    for (int i = 1; i <= nrCols; i++) {
      System.out.print(rsmd.getColumnName(i) + "\t");
    }
    System.out.println();
    
    while (rs.next()) {
      for (int i = 1; i <= nrCols; i++) {
        System.out.print(rs.getObject(i) + "\t");

      }
      System.out.println();
    }    
    rs.close();
    rs.getStatement().close();
    con.close();
  }
}
