package br.com.factory;

import br.com.annotations.Fk;
import br.com.annotations.Transient;
import static br.com.generic.GenericDAO.DATE_FORMATTER;
import static br.com.generic.GenericDAO.DATE_TIME_FORMATTER;
import br.com.utils.DateUtils;
import br.com.utils.SQLUtils;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Date;

public class Query implements Serializable {

    private static final long serialVersionUID = 4702648733274262886L;

    private final StringBuffer SQL;
    private String sqlFull;
    private PreparedStatement pstm;
    private ResultSet rs;
    private Connection conn;
    private final List<Parametros> listaparams;
    private static boolean showsql = false;
    private Integer indexParam = 0;
    private Integer count = 1;

    //Procedimento Text para receber um Texto SQL
    public Query(Connection conn) {
        this.SQL = new StringBuffer();
        this.listaparams = new LinkedList<>();
        this.conn = conn;
        indexParam = 0; // Giovani Moura - 12/08/11
        count = 1;
    }

    public static void setShowSQL(boolean show) {
        showsql = false;
    }

    public Query(String teste) {
        this.SQL = new StringBuffer();
        this.listaparams = new LinkedList<>();
        count = 1;
        //this.values = new HashMap<Integer, Object>();
    }

    public void setText(String text) {
        this.SQL.delete(0, this.SQL.toString().length());
        this.SQL.append(text);
    }

    public String getText() {
        return this.SQL.toString();
    }

    //Procedimento para Adicionar SQL
    public void addSql(String SQL) {
        this.SQL.append(SQL).append(" ");
    }

    public void clear() {
        this.SQL.delete(0, this.SQL.toString().length());
        //this.values.clear();
        this.listaparams.clear();
        indexParam = 0; // Giovani Moura - 12/08/11
        count = 1;
    }

    public void addParam(Integer index, Object Value) {
        listaparams.add(new Parametros(index, Value));
        //this.values.put(index, Value);
    }

    /**
     * Giovani Moura - 12/08/11 - Adiciona os parâmetos da query sem necessidade
     * de informar o Index do param. - A variável indexParam controla o Index do
     * parâmetro
     *
     * @param Value
     */
    public void addParam(Object Value) {
        indexParam = indexParam + 1;
        listaparams.add(new Parametros(indexParam, Value));
        //this.values.put(indexParam, Value);
    }

    private void preExecution() throws SQLException, ClassNotFoundException {
        if (!conn.inTransaction() && conn.getConnection().isClosed()) {
            conn.connect();
        }

        this.conn.setSchema();

        this.pstm = this.conn.getConnection().prepareStatement(this.SQL.toString(), ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

        final Iterator<Parametros> it = listaparams.iterator();

        while (it.hasNext()) {
            final Parametros param = it.next();
            final Integer key = param.getIndex();
            if (param.getValor() != null) {

                final int types = classToTypes(param.getValor());

                if (types == Types.OTHER) {
                    pstm.setObject(key, param.getValor());
                } else {
                    pstm.setObject(key, param.getValor(), types);
                }

            } else {
                pstm.setObject(key, param.getValor(), Types.NULL);
            }
        }
    }

    public boolean open() throws SQLException, ClassNotFoundException {
        preExecution();
        rs = pstm.executeQuery();
        boolean ret = rs.getFetchSize() > 0;
        listaparams.clear();
        indexParam = 0;
        return ret;
    }

    public int execute() throws SQLException, ClassNotFoundException {
        preExecution();
        int ret = pstm.executeUpdate();
        this.listaparams.clear();
        return ret;
    }

    public List<Parametros> getParamtrosList() {
        return listaparams;
    }

    private void preExecution(String sql, Object[] values) throws SQLException, ClassNotFoundException {
        if (this.conn.getConnection() == null || this.conn.getConnection().isClosed()) {
            this.conn.connect();
        }
        this.pstm = this.conn.getConnection().prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

        for (int index = 0; index < values.length; index++) {
            if (values[index] == null) {
                pstm.setObject(index + 1, null, Types.NULL);
            } else {
                final int types = classToTypes(values[index]);
                if (types == Types.OTHER) {
                    pstm.setObject(index + 1, values[index]);
                } else {
                    pstm.setObject(index + 1, values[index], types);
                }
            }
        }
    }

    public String getTextFull() {

        sqlFull = getText();
        listaparams.sort((item1, item2) -> item1.getIndex().compareTo(item2.getIndex()));
        listaparams.forEach(parametros -> {

            final Object valor = parametros.getValor();

            if (valor instanceof String) {

                final String valorString = String.valueOf(valor);
                sqlFull = sqlFull.replaceFirst("[?]", "'" + valorString + "'");

            } else if ((valor instanceof Integer) || (valor instanceof Long)
                    || (valor instanceof Double) || (valor instanceof Float)) {

                final String valorString = String.valueOf(valor);
                sqlFull = sqlFull.replaceFirst("[?]", valorString);

            } else if (valor instanceof Boolean) {

                final String valorString = String.valueOf(valor);
                sqlFull = sqlFull.replaceFirst("[?]", valorString);

            } else if ((valor instanceof Date)) {

                String valorString = DateUtils.ofTime((Date) valor).format(DATE_TIME_FORMATTER);
                sqlFull = sqlFull.replaceFirst("[?]", "'" + valorString + "'");

            } else if (valor instanceof Calendar) {

                String valorString = DateUtils.ofTime((Calendar) valor).format(DATE_TIME_FORMATTER);
                sqlFull = sqlFull.replaceFirst("[?]", "'" + valorString + "'");

            } else if (valor instanceof LocalDate) {

                String valorString = ((LocalDate) valor).format(DATE_FORMATTER);
                sqlFull = sqlFull.replaceFirst("[?]", "'" + valorString + "'");

            } else if (valor instanceof LocalDateTime) {

                String valorString = ((LocalDateTime) valor).format(DATE_TIME_FORMATTER);
                sqlFull = sqlFull.replaceFirst("[?]", "'" + valorString + "'");
            }

        });
        return sqlFull;
    }

    public int executeCommand(String sql, Object[] values) throws Exception {
        preExecution(sql, values);
        int ret = pstm.executeUpdate();
        return ret;
    }

    public ResultSet executeCommandReturn(String sql, Object[] values) throws Exception {
        preExecution(sql, values);
        return pstm.executeQuery();
    }

    public int getRowCount() {
        try {
            rs.last();
            int totrow = rs.getRow();
            rs.beforeFirst();
            return totrow;
        } catch (SQLException ex) {
            return 0;
        }
    }

    public void setConnection(Connection conn) {
        this.conn = conn;
    }

    /**
     * Metodo que faz a paginação no resultset instanciado com a pesquisa aberta
     *
     * @return
     */
    @SuppressWarnings("CallToPrintStackTrace")
    public boolean next() {
        boolean ret = false;
        try {
            ret = rs.next();

        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return ret;
    }

    public Object getValue(String fieldname) throws SQLException { // Giovani Moura - 11/08/11 - Adicionado throws para tratar exceção
        return rs.getObject(fieldname);
    }

    public Object getValue(int index) throws SQLException { // Giovani Moura - 11/08/11 - Adicionado throws para tratar exceção
        return rs.getObject(index);
    }

    public Object getValue(String fieldname, Object defaultValue) throws SQLException {

        final Object value = rs.getObject(fieldname);

        if (value == null) {
            return defaultValue;
        }

        return value;
    }

    public Object getValue(int index, Object defaultValue) throws SQLException {

        final Object value = rs.getObject(index);

        if (value == null) {
            return defaultValue;
        }

        return value;
    }

    public String getValueAsString(String fieldname) throws SQLException {

        final Object value = getValue(fieldname);

        if (value == null) {
            return null;
        }

        return String.valueOf(value);
    }

    public String getValueAsString(int index) throws SQLException {

        final Object value = getValue(index);

        if (value == null) {
            return null;
        }

        return String.valueOf(value);
    }

    public Integer getValueAsInteger(String fieldname) throws SQLException {

        final String value = getValueAsString(fieldname);

        if (value == null) {
            return null;
        }

        return Integer.valueOf(value);
    }

    public Integer getValueAsInteger(int index) throws SQLException {

        final String value = getValueAsString(index);

        if (value == null) {
            return null;
        }

        return Integer.valueOf(value);
    }

    public Long getValueAsLong(String fieldname) throws SQLException {

        final String value = getValueAsString(fieldname);

        if (value == null) {
            return null;
        }

        return Long.valueOf(value);
    }

    public Long getValueAsLong(int index) throws SQLException {

        final String value = getValueAsString(index);

        if (value == null) {
            return null;
        }

        return Long.valueOf(value);
    }

    public Double getValueAsDouble(String fieldname) throws SQLException {

        final String value = getValueAsString(fieldname);

        if (value == null) {
            return null;
        }

        return Double.valueOf(value);
    }

    public Double getValueAsDouble(int index) throws SQLException {

        final String value = getValueAsString(index);

        if (value == null) {
            return null;
        }

        return Double.valueOf(value);
    }

    public Date getValueAsDate(String fieldname) throws SQLException {

        final Object value = getValue(fieldname);

        if (value == null) {
            return null;
        }

        return (Date) value;
    }

    public Date getValueAsDate(int index) throws SQLException {

        final Object value = getValue(index);

        if (value == null) {
            return null;
        }

        return (Date) value;
    }

    public ResultSet getResultSet() {
        return rs;
    }

    public PreparedStatement getStatment() {
        return pstm;
    }

    public int getColumnCount() {
        try {
            return rs.getMetaData().getColumnCount();
        } catch (SQLException ex) {
            return 0;
        }
    }

    public String getColumnName(int index) {
        try {
            return rs.getMetaData().getColumnName(index);
        } catch (SQLException ex) {
            return "";
        }
    }

    public String getColumnLabel(int index) {
        try {
            return rs.getMetaData().getColumnLabel(index);
        } catch (SQLException ex) {
            return "";
        }
    }

    @SuppressWarnings("CallToPrintStackTrace")
    public String getType(int indexParam) {
        try {
            return rs.getMetaData().getColumnTypeName(indexParam);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Connection getConnection() {
        return this.conn;
    }

    public class ParamSql {

        private final Object value;
        private final int typesql;

        public ParamSql(Object value, int typesql) {
            this.value = value;
            this.typesql = typesql;
        }

        /**
         * @return the value
         */
        public Object getValue() {
            return value;
        }

        /**
         * @return the typesql
         */
        public int getTypesql() {
            return typesql;
        }
    }

    /**
     * Metodo para executar functions na base de dados com o retorno void
     *
     * @param parametros
     * @throws SQLException
     */
    public void executeProcedure(ParametrosFuncao parametros) throws Exception {
        String rowsFuncao = "("; // Organiza os rows (parametros de entrada) da função      
        String sep = "";
        for (Object param : parametros.getListParametros()) {
            if (param instanceof List) {
                // Add parametro ARRAY
                List entidade = (List) param;
                if (entidade.isEmpty()) {
                    rowsFuncao += sep + "null";
                } else {
                    rowsFuncao += sep + "ARRAY[";
                    String sep2 = "";
                    for (Object obj : entidade) {
                        // Realiza um CAST dos parametros para o TYPE do banco
                        rowsFuncao += sep2 + addParamFuncao(obj).replaceAll("row", "") + "::vw" + obj.getClass().getSimpleName().toLowerCase();
                        sep2 = ",";
                    }
                    rowsFuncao += "] ";
                }
            } else if (param instanceof Integer) {
                rowsFuncao += sep + "?";
                addParam(count, Integer.valueOf(param.toString()));
                count++;
            } else if (param instanceof String) {
                rowsFuncao += sep + "?";
                addParam(count, String.valueOf(param));
                count++;
            } else if (param instanceof java.util.Date) {
                rowsFuncao += sep + "?";
                Calendar dt = Calendar.getInstance();
                dt.setTime((java.util.Date) param);
                addParam(count, dt.getTime());
                count++;
            } else if (param instanceof Serializable) {
                // Add parametro ENTIDADE
                rowsFuncao += sep + addParamFuncao(param);
            } else {
                rowsFuncao += sep + "?";
                addParam(count, param);
                count++;
            }
            sep = ", ";
        }
        rowsFuncao += ")";
        addSql(parametros.getSqlVoid() + rowsFuncao); // Add SQL da funçao        
        CallableStatement cs;
        cs = conn.getConnection().prepareCall("{" + getText().trim() + "}");
        Iterator<Parametros> it = listaparams.iterator();//values.keySet().iterator();        
        while (it.hasNext()) {
            Parametros param = it.next();
            Integer key = param.getIndex();

            if (param.getValor() != null) {

                final int types = classToTypes(param.getValor());

                if (types == Types.OTHER) {
                    pstm.setObject(key, param.getValor());
                } else {
                    pstm.setObject(key, param.getValor(), types);
                }

            } else {
                cs.setObject(key, param.getValor(), Types.NULL);
            }
        }
        listaparams.clear();
        indexParam = 0; // Giovani Moura - 12/08/11
        rs = cs.executeQuery();
        //cs.close();
    }

    @SuppressWarnings("CallToPrintStackTrace")
    public void execFileSQL(File file) throws Exception {
        File[] arrayfile = file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().contains(".sql");
            }
        });
        for (File f : arrayfile) {
            try (Scanner sc = new Scanner(new FileInputStream(f))) {
                String asql = "";
                while (sc.hasNext()) {
                    String linha = sc.nextLine();
                    if (!linha.endsWith(";")) {
                        asql += linha;
                        continue;
                    } else {
                        asql += linha;
                    }
                    linha = asql;
                    asql = "";
                    pstm = conn.getConnection().prepareStatement(linha);
                    pstm.executeUpdate();
                }
            } catch (FileNotFoundException | SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Giovani Moura - 18/05/2012 Monta a query e executa a função do postgres
     *
     * @param parametros
     * @throws Exception
     */
    public void executaFuncao(ParametrosFuncao parametros) throws Exception {
        String rowsFuncao = "("; // Organiza os rows (parametros de entrada) da função
        String sep = "";
        for (Object param : parametros.getListParametros()) {
            if (param instanceof List) {
                // Add parametro ARRAY
                List entidade = (List) param;
                rowsFuncao += sep + "ARRAY[";
                String sep2 = "";
                for (Object obj : entidade) {
                    // Realiza um CAST dos parametros para o TYPE do banco
                    rowsFuncao += sep2 + addParamFuncao(obj).replaceAll("row", "") + "::vw" + obj.getClass().getSimpleName().toLowerCase();
                    sep2 = ",";
                }
                rowsFuncao += "]";
            } else if (param instanceof Integer) {
                rowsFuncao += sep + "?";
                addParam(count, Integer.valueOf(param.toString()));
                count++;
            } else if (param instanceof String) {
                rowsFuncao += sep + "?";
                addParam(count, String.valueOf(param));
                count++;
            } else if (param instanceof java.util.Date) {
                rowsFuncao += sep + "?";
                Calendar dt = Calendar.getInstance();
                dt.setTime((java.util.Date) param);
                addParam(count, dt.getTime());
                count++;
            } else {
                // Add parametro ENTIDADE
                rowsFuncao += sep + addParamFuncao(param);
            }
            sep = ", ";
        }
        rowsFuncao += ")";
        addSql(parametros.getSqlFuncao() + rowsFuncao); // Add SQL da funçao
        if (showsql) {
            System.out.println("EXEC SQL: " + getText());
        }
        open(); // Executa função
    }

    /**
     * Monta a query e executa a função do postgres para a aplicação android
     *
     * @param parametros
     * @throws Exception
     */
    public void executaFuncaoAndroid(ParametrosFuncao parametros) throws Exception {
        String rowsFuncao = "("; // Organiza os rows (parametros de entrada) da função
        String sep = "";
        for (Object param : parametros.getListParametros()) {
            if (param instanceof List) {
                // Add parametro ARRAY
                List entidade = (List) param;
                rowsFuncao += sep + "ARRAY[";
                String sep2 = "";
                for (Object obj : entidade) {
                    // Realiza um CAST dos parametros para o TYPE do banco
                    rowsFuncao += sep2 + addParamFuncao(obj).replaceAll("row", "") + "::ent_" + obj.getClass().getSimpleName().toLowerCase();
                    sep2 = ",";
                }
                rowsFuncao += "]";
            } else if (param instanceof Integer) {
                rowsFuncao += sep + "?";
                addParam(count, Integer.valueOf(param.toString()));
                count++;
            } else if (param instanceof String) {
                rowsFuncao += sep + "?";
                addParam(count, String.valueOf(param));
                count++;
            } else if (param instanceof java.util.Date) {
                rowsFuncao += sep + "?";
                Calendar dt = Calendar.getInstance();
                dt.setTime((java.util.Date) param);
                addParam(count, dt.getTime());
                count++;
            } else {
                // Add parametro ENTIDADE
                rowsFuncao += sep + addParamFuncao(param);
            }
            sep = ", ";
        }
        rowsFuncao += ")";
        addSql(parametros.getSqlFuncao() + rowsFuncao); // Add SQL da funçao
        if (showsql) {
            System.out.println("EXEC SQL: " + getText());
        }
        open(); // Executa função
    }

    /**
     * Giovani Moura - 15/05/2012 Adiciona os parâmetros que serão enviados para
     * a função
     *
     * @param param
     * @param count
     * @throws Exception
     */
    private String addParamFuncao(Object param) throws Exception {
        //count++;
        String row = "row(";
        String sep = "";
        if (param == null) {
            return null;
        }
        Field[] campos = param.getClass().getDeclaredFields();
        for (Field campo : campos) {
            Object valor;
            // Somente campos que existem no banco de dados
            if (!campo.isAnnotationPresent(Transient.class)) {
                campo.setAccessible(true);
                if (campo.getType().isEnum()) {
                    //Enum
                    Class c = Class.forName(campo.getType().getName());
                    if (campo.get(param) != null) {
                        valor = String.valueOf(Enum.valueOf(c, String.valueOf(campo.get(param))));
                        addParam(count, valor);
                    } else {
                        addParam(count, null);
                    }
                } else if (campo.isAnnotationPresent(Fk.class)) {
                    // Fk
                    valor = SQLUtils.getValuePK(campo.get(param));
                    addParam(count, valor);

                } else {
                    // Outros
                    valor = campo.get(param);
                    addParam(count, valor);
                }
                row += sep + "?";
                sep = ", ";
                count++;
            }
        }
        return row += ")";
    }

    private int classToTypes(final Object object) {

        final String className = object.getClass().getSimpleName();

        if (className.equalsIgnoreCase("character") || className.equalsIgnoreCase("char")) {
            return Types.CHAR;
        } else if (className.equalsIgnoreCase("number") || className.equalsIgnoreCase("double")) {
            return Types.NUMERIC;
        } else if (className.equalsIgnoreCase("Integer") || className.equalsIgnoreCase("int")) {
            return Types.INTEGER;
        } else if (className.equalsIgnoreCase("string") || object.getClass().isEnum()) {
            return Types.VARCHAR;
        } else if (className.equalsIgnoreCase("boolean")) {
            return Types.BOOLEAN;
        } else if (className.equalsIgnoreCase("date") || className.equalsIgnoreCase("GregorianCalendar")) {
            return Types.TIMESTAMP;
        } else if (object.getClass().isArray()) {
            return Types.ARRAY;
        } else {
            return Types.OTHER;
        }
    }
}
