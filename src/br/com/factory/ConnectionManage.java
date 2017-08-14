package br.com.factory;

import br.com.commons.util.ThreadUtil;
import br.com.types.ConnectionType;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ConnectionManage {

    private final ThreadUtil threadUtil = ThreadUtil.newInstance();

    private Integer identifier = 1;

    private ConnectionType TYPE = ConnectionType.POSTGRES;
    private String SCHEMA = "public";
    private String PASSWORD = "";
    private String HOST = "";
    private String BASE_NAME = "";
    private String USER = "";
    private Integer PORT;

    private final Integer TIME_SCHEDULE_IN_SEC = 30;
    private final Integer MAX_CONNECTIONS = 80;

    private final Map<Integer, DataControl> connectionsAvailable = new LinkedHashMap<>();
    private final Map<Integer, DataControl> connectionsBusy = new LinkedHashMap<>();

    private ScheduledFuture scheduleCloseConnections;

    private ConnectionManage() {
        threadUtil.setQtdeSchedulePoolThreads(1);
        startSChedule();
    }

    public static ConnectionManage newInstance() {
        return new ConnectionManage();
    }

    public void setConnectionData(final String host, final Integer port, final String user, final String password,
            final String baseName) {

        setConnectionData(host, port, user, password, baseName, SCHEMA);
    }

    public void setConnectionData(final String host, final Integer port, final String user, final String password,
            final String baseName, final ConnectionType type) {

        setConnectionData(host, port, user, password, baseName, SCHEMA, type);
    }

    public void setConnectionData(final String host, final Integer port, final String user, final String password,
            final String baseName, final String schema) {

        setConnectionData(host, port, user, password, baseName, schema, TYPE);
    }

    public void setConnectionData(final String host, final Integer port, final String user, final String password,
            final String baseName, final String schema, final ConnectionType type) {

        HOST = host;
        PORT = port;
        USER = user;
        PASSWORD = password;
        BASE_NAME = baseName;
        SCHEMA = schema;
        TYPE = type;
        closeConnections();
    }

    private void startSChedule() {
        if (scheduleCloseConnections == null) {
            scheduleCloseConnections = threadUtil.getScheduleExecutor(() -> {
                closeConnectionsIfNecessary();
            }, TIME_SCHEDULE_IN_SEC, TIME_SCHEDULE_IN_SEC, TimeUnit.SECONDS);
        }
    }

    private void closeConnectionsIfNecessary() {

        if (!connectionsAvailable.isEmpty()) {
            synchronized (connectionsAvailable) {
                final List<DataControl> connectionsToRemove = new LinkedList<>();
                connectionsAvailable.keySet().stream().map((key) -> connectionsAvailable.get(key)).forEach((manage) -> {
                    final Long currenttime = Calendar.getInstance().getTimeInMillis();
                    final Long timeRegra = 60 * 1000l * 15; // 15 minutos
                    if ((currenttime - manage.getCalendar().getTimeInMillis()) > timeRegra) {
                        connectionsToRemove.add(manage);
                    }
                });
                connectionsToRemove.stream().forEach((dataControl) -> {
                    dataControl.getConnection().close();
                    connectionsAvailable.remove(dataControl.getConnection().getIdentifier());
                });
            }
        }
    }

    private void closeConnections() {

        if (!connectionsAvailable.isEmpty()) {
            synchronized (connectionsAvailable) {
                connectionsAvailable.keySet().stream().map((key) -> connectionsAvailable.get(key)).forEach((dataControl) -> {
                    dataControl.getConnection().close();
                });
                connectionsAvailable.clear();
            }
        }
        if (!connectionsBusy.isEmpty()) {
            synchronized (connectionsBusy) {
                connectionsBusy.keySet().stream().map((key) -> connectionsBusy.get(key)).forEach((dataControl) -> {
                    dataControl.getConnection().close();
                });
                connectionsBusy.clear();
            }
        }
    }

    private class DataControl {

        private final Connection connection;

        private Calendar calendar;

        public Calendar getCalendar() {
            return calendar;
        }

        public void setCalendar(Calendar calendar) {
            this.calendar = calendar;
        }

        public Connection getConnection() {
            return connection;
        }

        public DataControl(final Connection conection) {
            this.connection = conection;
        }
    }

    public void closeConnection(final Connection connection) {

        if (connection == null) {
            throw new IllegalArgumentException("Conection can´t be null");
        }

        final DataControl manage;
        synchronized (connectionsBusy) {
            manage = connectionsBusy.remove(connection.getIdentifier());
        }

        Boolean isClosed;
        try {
            isClosed = connection.getConnection() == null || connection.getConnection().isClosed();
        } catch (SQLException ex) {
            connection.close();
            return;
        }

        if (!isClosed) {
            synchronized (connectionsAvailable) {
                connectionsAvailable.put(manage.getConnection().getIdentifier(), manage);
            }
            closeConnectionsIfNecessary();
        }
    }

    public synchronized Connection getConnection() throws SQLException, ClassNotFoundException {

        while (connectionsBusy.size() == MAX_CONNECTIONS) {
            threadUtil.sleep(100);
        }

        final Connection c;

        if (!connectionsAvailable.isEmpty()) {
            synchronized (connectionsAvailable) {
                final DataControl manage = connectionsAvailable.values().iterator().next();
                connectionsAvailable.remove(manage.getConnection().getIdentifier());
                c = manage.getConnection();
            }
        } else {
            c = new Connection(TYPE, PASSWORD, HOST, BASE_NAME, SCHEMA, USER, PORT, identifier++);
        }

        if (c.getConnection() == null || c.getConnection().isClosed()) {
            try {
                c.connect();
            } catch (ClassNotFoundException ex) {
                throw ex;
            } catch (SQLException ex) {
                throw new SQLException("Não foi possível se conectar com o banco de dados. " + ex.getMessage(), ex);
            }
        }

        final DataControl manage = new DataControl(c);
        manage.setCalendar(Calendar.getInstance());

        connectionsBusy.put(manage.getConnection().getIdentifier(), manage);

        return c;
    }

}
