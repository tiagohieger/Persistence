package br.com.factory;

import br.com.types.ConnectionType;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class Connection {

    private final ConnectionType type;
    private final String password;
    private final String host;
    private final String baseName;
    private final String schema;
    private final String user;
    private final Integer port;

    private java.sql.Connection connection;

    private final Integer identifier;

    protected Connection(final ConnectionType type, final String password, final String host, final String baseName,
            final String schema, final String user, final Integer port, final Integer identifier) {

        this.type = type;
        this.password = password;
        this.host = host;
        this.baseName = baseName;
        this.schema = schema;
        this.user = user;
        this.port = port;
        this.identifier = identifier;
    }

    protected Integer getIdentifier() {
        return identifier;
    }

    private String getUrl() {
        return type.getJdbcconn() + host + ":" + port + "/" + baseName;
    }

    public java.sql.Connection getConnection() {
        return connection;
    }

    protected void connect() throws ClassNotFoundException, SQLException {
        if (connection == null || connection.isClosed()) {
            Class.forName(type.getClassforname());
            connection = DriverManager.getConnection(getUrl(), user, password);
            setSchema();
        }
    }

    protected void setSchema() throws SQLException {
        if (connection != null && baseName != null && !baseName.trim().isEmpty()) {
            connection.prepareStatement("SET schema '" + schema + "'").execute();
        }
    }

    protected boolean close() {
        try {
            if (connection != null && !inTransaction() && !connection.isClosed()) {
                connection.close();
                return true;
            }
        } catch (Exception ex) {
        }
        return false;
    }

    public boolean startTransaction() {
        try {
            if (connection != null) {
                connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
                connection.setAutoCommit(false);
                return true;
            }
        } catch (Exception ex) {
        }
        return false;
    }

    public boolean saveTransaction() {
        try {
            if (connection != null && inTransaction()) {
                connection.commit();
                connection.setAutoCommit(true);
                return true;
            }
        } catch (SQLException ex) {
        }
        return false;
    }

    public boolean cancelTransaction() {
        try {
            if (connection != null && inTransaction()) {
                connection.rollback();
                connection.setAutoCommit(true);
                return true;
            }
        } catch (Exception ex) {
        }
        return false;
    }

    protected boolean inTransaction() throws SQLException {
        return connection != null && !connection.getAutoCommit();
    }

    protected List<String> listTables(final String schema) throws SQLException {
        final List<String> tables = new LinkedList<>();
        if (connection == null) {
            return tables;
        }
        final ResultSet result = connection.getMetaData().getTables(null, schema, null, null);
        while (result.next()) {
            tables.add(result.getString(3).toLowerCase());
        }
        return tables;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + Objects.hashCode(this.identifier);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Connection other = (Connection) obj;
        return Objects.equals(this.identifier, other.identifier);
    }

}
