/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.types;

/**
 *
 * @author Alecsandro
 */
public enum ConnectionType {

    ORACLE("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@"),
    POSTGRES("org.postgresql.Driver", "jdbc:postgresql://"),
    MYSQL("com.mysql.jdbc.Driver", "jdbc:mysql://"),
    DERBY("org.apache.derby.jdbc.ClientDriver", "jdbc:derby://");
    private String classforname = "";
    private String jdbcconn = "";

    private ConnectionType(String classforname, String connjdbc) {
        this.classforname = classforname;
        jdbcconn = connjdbc;
    }

    public String getClassforname() {
        return classforname;
    }

    public String getJdbcconn() {        
        return jdbcconn;
    }
}
