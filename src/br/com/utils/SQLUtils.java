package br.com.utils;

import br.com.annotations.Column;
import br.com.annotations.Fk;
import br.com.annotations.Id;
import br.com.annotations.Table;
import br.com.annotations.Transient;
import br.com.factory.Builder;
import br.com.factory.Connection;
import br.com.factory.ConnectionManage;
import br.com.factory.Query;
import br.com.types.ConnectionType;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import java.util.stream.Collectors;

public class SQLUtils {

    public static final ConnectionManage DB = ConnectionManage.newInstance();

    public static String rowFuncao = "row(";

    public static Map<String, Object[]> getInsertSQL(Object entity) throws IllegalArgumentException, IllegalAccessException {

        final String tableName = PersistenceUtils.tableName(entity.getClass());
        final List<Field> tableFields = PersistenceUtils.listFields(entity.getClass());
        final StringBuilder insert = new StringBuilder("INSERT INTO " + tableName + "( ");
        final StringBuilder values = new StringBuilder(") VALUES (");

        final List<String> listRetorns = new ArrayList<>();

        String sep = "";

        final List<Object> fieldsValues = new ArrayList<>();

        for (Field field : tableFields) {

            field.setAccessible(true);
            if (field.getDeclaredAnnotations().length == 0) {
                continue;
            }
            if (field.isAnnotationPresent(Id.class)) {
                listRetorns.add(PersistenceUtils.columnName(field));
                continue;
            }
            if (field.get(entity) == null) {
                if (field.isAnnotationPresent(Column.class)
                        && !field.getAnnotation(Column.class).defaultValue().equals("")) {
                    listRetorns.add(PersistenceUtils.columnName(field));
                }
                continue;
            }
            if (field.isAnnotationPresent(Fk.class)) {
                insert.append(sep).append(field.getAnnotation(Fk.class).name());
                values.append(sep).append("?");
                fieldsValues.add(getValuePK(field.get(entity)));
                sep = ", ";
            } else if (field.isAnnotationPresent(Column.class)) {
                if (!field.getAnnotation(Column.class).defaultValue().equals("")) {
                    listRetorns.add(PersistenceUtils.columnName(field));
                }
                //if (f.get(entity) == null && f.isAnnotationPresent(Column.class) && f.getAnnotation(Column.class).defaultValue().equals("")) {
                String columnName = PersistenceUtils.columnName(field);
                insert.append(sep).append(columnName);
                values.append(sep).append("?");
                if (field.getType().isInterface()) {
                    fieldsValues.add(field.get(entity).getClass().getTypeName() + "." + field.get(entity));
                } else {
                    fieldsValues.add(field.get(entity));
                }
                sep = ", ";
            }
        }
        values.append(") RETURNING ");
        values.append(PersistenceUtils.concat(listRetorns));
        values.append(";");
        Map<String, Object[]> mp = new HashMap<>();
        mp.put(insert.append(values.toString()).toString(), fieldsValues.toArray());
        return mp;
    }

    public static Map<String, Object[]> getUpdateSQL(Object entity) throws Exception {

        final String tableName = PersistenceUtils.tableName(entity.getClass());

        final List<Field> tableFields = PersistenceUtils.listFields(entity.getClass());

        final StringBuilder updateQuery = new StringBuilder("UPDATE " + tableName + " SET ");

        final List<Object> values = new ArrayList<>();

        // Popula todos os set's
        updateQuery.append(tableFields.stream()
                .filter(field -> PersistenceUtils.isUpdatable(entity, field))
                .map(field -> {

                    field.setAccessible(true);

                    if (field.isAnnotationPresent(Fk.class)) {

                        try {
                            values.add(getValuePK(field.get(entity)));
                        } catch (IllegalAccessException | IllegalArgumentException ignore) {
                        }

                        return !field.getAnnotation(Fk.class).isReference()
                                ? (PersistenceUtils.columnName(field) + " = ?")
                                : (field.getAnnotation(Fk.class).name()) + " = ?";

                    } else if (field.isAnnotationPresent(Column.class)) {

                        try {
                            values.add(field.get(entity));
                        } catch (IllegalAccessException | IllegalArgumentException ignore) {
                        }
                        return PersistenceUtils.columnName(field) + " = ?";

                    } else {
                        return "";
                    }

                }).collect(Collectors.joining(", ")));

        // Popula dados para claúsula where
        updateQuery.append(" WHERE ");

        updateQuery.append(tableFields.stream()
                .filter(field -> field.isAnnotationPresent(Id.class))
                .map(field -> {

                    try {
                        values.add(field.get(entity));
                    } catch (IllegalAccessException | IllegalArgumentException ignore) {
                    }

                    return PersistenceUtils.columnName(field) + " = ?";
                })
                .collect(Collectors.joining(" AND ")));

        final Map<String, Object[]> result = new HashMap<>();
        result.put(updateQuery.toString(), values.toArray());
        return result;
    }

    public static String getTruncate(Class classe) throws Exception {
        final String tableName = PersistenceUtils.tableName(classe);
        final StringBuilder select = new StringBuilder("TRUNCATE TABLE ");
        select.append(tableName);
        select.append(" CASCADE ");
        return select.toString();
    }

    public static String getDeleteAll(Class classe) throws Exception {
        final String tableName = PersistenceUtils.tableName(classe);
        StringBuilder select = new StringBuilder("DELETE FROM ");
        select.append(tableName);
        return select.toString();
    }

    public static String getDeleteById(Class classe) throws Exception {
        String tableName = PersistenceUtils.tableName(classe);
        String idName = PersistenceUtils.getIdField(classe).getName();
        StringBuilder select = new StringBuilder("DELETE FROM ");
        select.append(tableName);
        select.append(" WHERE ");
        select.append(idName);
        select.append(" = ? ");
        return select.toString();
    }

    public static String getDeleteByParams(Class classe, Object[]... params) throws Exception {
        String tableName = PersistenceUtils.tableName(classe);
        StringBuilder select = new StringBuilder("DELETE FROM ");
        select.append(tableName);
        select.append(" WHERE ");
        for (Object[] param : params) {
            select.append(param[0]);
            select.append(" = ? ");
        }
        return select.toString();
    }

    public static String getSelectFull(Class classe, boolean isDependences) throws InstantiationException, IllegalAccessException {
        String tableName = PersistenceUtils.tableName(classe);
        List<Field> camposTabela = PersistenceUtils.listFields(classe);

        StringBuilder select = new StringBuilder("");
        if (!isDependences) {
            select.append("SELECT ");
            String sep = "";
            for (Field f : camposTabela) {
                f.setAccessible(true);

                if (f.getDeclaredAnnotations().length == 0) {
                    continue;
                }

                if (!f.isAnnotationPresent(Fk.class) && !f.isAnnotationPresent(Transient.class)) {
                    select.append(sep).append(PersistenceUtils.columnName(f));
                    sep = ", ";
                } else if (!f.isAnnotationPresent(Transient.class)) {
                    select.append(sep).append(f.getAnnotation(Fk.class).id());
                    sep = ", ";
                }
            }
            select.append(" FROM ").append(tableName);
        } else {
            select.append(SQLUtils.getSelectJoin(classe));
        }
        return select.toString();
    }

    public static String getSelectJoin(Class classe) throws InstantiationException, IllegalAccessException {
        String tableName = PersistenceUtils.tableName(classe);
        List<Field> camposTabela = PersistenceUtils.listFields(classe);
        List<String> listatabelas = new LinkedList<>();
        StringBuilder select = new StringBuilder("SELECT * FROM ");
        select.append(tableName);
        listatabelas.add(tableName);
        for (Field f : camposTabela) {
            f.setAccessible(true);

            if (f.getDeclaredAnnotations().length == 0) {
                continue;
            }

            if (f.isAnnotationPresent(Fk.class)) {
                List<Field> ff = PersistenceUtils.listFields(classe);
                String tabela = f.getAnnotation(Fk.class).table().toLowerCase();
                String join = " LEFT JOIN " + tabela + " ON " + tabela + ".";

                if (!listatabelas.contains(tabela)) {
                    listatabelas.add(tabela);
                    for (Field fi : ff) {
                        fi.setAccessible(true);
                        if (fi.isAnnotationPresent(Id.class)) {
                            join += fi.getName().toLowerCase() + " = " + tableName + "." + f.getAnnotation(Fk.class).name();
                            break;
                        }
                    }
                    select.append(join);
                }
            }
        }
        return select.toString();
    }

    public static String getSQLCampos(String[] campos, String table, String delimitador) {
        String sql = "COPY " + table + "(";
        String sep = "";
        for (String campo : campos) {
            if (campo != null) {
                sql += sep + campo;
                sep = ", ";
            }
        }
        sql += ") from STDIN WITH CSV HEADER DELIMITER AS '" + delimitador + "';";
        return sql;
    }

    public static String getSelectByID(final Class clazz, final boolean isDependences) throws Exception {
        final String tableName = PersistenceUtils.tableName(clazz);
        final String idName = PersistenceUtils.getIdField(clazz).getName();
        final String fullSelect = getSelectFull(clazz, isDependences);
        final StringBuilder select = new StringBuilder(fullSelect);
        select.append(" WHERE ");
        select.append(tableName).append(".").append(idName).append(" = ?");
        return select.toString();
    }

    public static Map<String, List<Field>> getSelectPesquisaPorAnotacao(Class classe, boolean isdependences) throws Exception {

        String tableName = PersistenceUtils.tableName(classe);
        final List<Field> fields = PersistenceUtils.listFields(classe);

        final List<Field> listFields = new LinkedList<>();
        StringBuilder select = new StringBuilder("SELECT ");

        if (classe.isAnnotationPresent(Table.class)) {
            tableName = ((Table) classe.getAnnotation(Table.class)).name();
        }
        select.append(" FROM ").append(tableName);
        Map<String, List<Field>> toreturn = new HashMap<>();
        toreturn.put(select.toString(), listFields);
        return toreturn;
    }

    public static String getSelectFullAlias(Class classe, String alias, boolean isdependences) throws Exception {
        String tableName = PersistenceUtils.tableName(classe);
        List<Field> fields = PersistenceUtils.listFields(classe);
        StringBuilder select = new StringBuilder("SELECT ");
        String sep = "";
        for (Field f : fields) {
            f.setAccessible(true);

            if (f.getDeclaredAnnotations().length == 0) {
                continue;
            }

            if (!f.isAnnotationPresent(Fk.class) && !f.isAnnotationPresent(Transient.class)) {
                select.append(sep).append(alias).append(".").append(PersistenceUtils.columnName(f));
                sep = ", ";
            } else if (isdependences && !f.isAnnotationPresent(Transient.class)) {
                select.append(sep).append(alias).append(".").append(f.getAnnotation(Fk.class).id());
                sep = ", ";
            }
        }

        if (classe.isAnnotationPresent(Table.class)) {
            tableName = ((Table) classe.getAnnotation(Table.class)).name();
        }

        select.append(" FROM ").append(tableName).append(" ").append(alias);
        return select.toString();
    }

    public static <T> T entityPopulate(final Query query, final Class classe) throws InstantiationException, IllegalAccessException, SQLException {
        final Object object = entityPopulate(query, classe, null);
        return object == null ? null : (T) object;
    }

    public static <T> T entityPopulate(final Query query, final Class classe, final Integer tablePosition) throws InstantiationException, IllegalAccessException, SQLException {
        final Object entity = classe.newInstance();
        final List<Field> camposTabela = PersistenceUtils.listFields(classe);
        final ResultSetMetaData meta = query.getResultSet().getMetaData();
        final Map<String, Integer> mpcolumtable = new HashMap<>();

        final String tableName = PersistenceUtils.tableName(classe);

        if (tableName == null) {
            throw new IllegalArgumentException("A classe passada não possui a annotation Tabela!");
        }

        final String nomeTabela = (tablePosition == null || tablePosition == 0 ? "" : tablePosition + ":") + tableName + ":";

        for (int i = 1; i < meta.getColumnCount() + 1; i++) {

            final String originalColumn = meta.getTableName(i) + ":" + meta.getColumnName(i).toLowerCase();

            String column = originalColumn;

            for (int tableIndex = 1; mpcolumtable.containsKey(column); tableIndex++) {
                column = tableIndex + ":" + originalColumn;
            }
            mpcolumtable.put(column, i);

        }

        final Field primary = PersistenceUtils.getIdField(classe);
        if (query.getValue(mpcolumtable.get(nomeTabela + primary.getName().toLowerCase())) == null) {
            return null;
        }

        for (Field f : camposTabela) {
            f.setAccessible(true);

            if (f.getDeclaredAnnotations().length == 0) {
                continue;
            }

            if (f.isAnnotationPresent(Fk.class)) {
                try {
                    final Object entidade = f.getType().newInstance();
                    final String fieldnamepk = f.getAnnotation(Fk.class).id();
                    final Field Fieldpk = entidade.getClass().getDeclaredField(fieldnamepk);
                    if (query.getValue(fieldnamepk) != null) {
                        Fieldpk.setAccessible(true);
                        Fieldpk.set(entidade, query.getValue(fieldnamepk));
                        f.set(entity, entidade);
                    } else {
                        f.set(entity, null);
                    }
                } catch (NoSuchFieldException ex) {
                    if (!f.getType().isPrimitive()) {
                        f.set(entity, null);
                    }
                }
            } else if (!f.isAnnotationPresent(Fk.class) && !f.isAnnotationPresent(Transient.class)) {

                final String colName = PersistenceUtils.columnName(f);

                if (mpcolumtable.get(nomeTabela + colName) == null) {
                    if (!f.getType().isPrimitive()) {
                        f.set(entity, null);
                    }
                } else if (query.getValue(mpcolumtable.get(nomeTabela + colName)) == null) {
                    if (!f.getType().isPrimitive()) {
                        f.set(entity, null);
                    }
                } else if (!f.isAnnotationPresent(Transient.class)) {
                    f.set(entity, PersistenceUtils.getValue(f, query.getValue(mpcolumtable.get(nomeTabela + colName))));
                }
            }
        }
        return (T) entity;
    }

    public static <T> T classPopulate(final Query query, final Class classe) throws Exception {
        final Object entity = classe.newInstance();
        final List<Field> camposTabela = PersistenceUtils.listFields(classe);
        final ResultSetMetaData meta = query.getResultSet().getMetaData();
        final Map<String, Integer> mpcolumtable = new HashMap<>();

        for (int i = 1; i < meta.getColumnCount() + 1; i++) {
            mpcolumtable.put(meta.getColumnName(i).toLowerCase(), i);
        }

        final Field primary = PersistenceUtils.getIdField(classe);
        if (query.getValue(mpcolumtable.get(primary.getName().toLowerCase())) == null) {
            return null;
        }

        for (Field f : camposTabela) {
            f.setAccessible(true);

            if (f.getDeclaredAnnotations().length == 0) {
                continue;
            }

            if (f.isAnnotationPresent(Fk.class)) {
                try {
                    final Object entidade = f.getType().newInstance();
                    final String fieldnamepk = f.getAnnotation(Fk.class).id();
                    final Field Fieldpk = entidade.getClass().getDeclaredField(fieldnamepk);
                    if (query.getValue(fieldnamepk) != null) {
                        Fieldpk.setAccessible(true);
                        Fieldpk.set(entidade, query.getValue(fieldnamepk));
                        f.set(entity, entidade);
                    } else {
                        f.set(entity, null);
                    }

                } catch (NoSuchFieldException ex) {
                    if (!f.getType().isPrimitive()) {
                        f.set(entity, null);
                    }
                }
            } else if (!f.isAnnotationPresent(Fk.class) && !f.isAnnotationPresent(Transient.class)) {

                final String colName = PersistenceUtils.columnName(f);

                if (mpcolumtable.get(colName) == null) {
                    if (!f.getType().isPrimitive()) {
                        f.set(entity, null);
                    }
                } else if (query.getValue(mpcolumtable.get(colName)) == null) {
                    if (!f.getType().isPrimitive()) {
                        f.set(entity, null);
                    }
                } else if (!f.isAnnotationPresent(Transient.class)) {
                    f.set(entity, PersistenceUtils.getValue(f, query.getValue(mpcolumtable.get(colName))));
                }
            }
        }
        return (T) entity;
    }

    public static Integer getValuePK(Object obj) throws IllegalArgumentException, IllegalAccessException {
        if (obj != null) {
            return (Integer) PersistenceUtils.getIdField(obj.getClass()).get(obj);
        }
        return null;
    }

    public static String createOrReplaceView(Class classe) {
        StringBuilder sqlvw = new StringBuilder("CREATE OR REPLACE VIEW ");
        String tableName = PersistenceUtils.tableName(classe);

        if (classe.isAnnotationPresent(Table.class)) {
            return null;
        }

        if (classe.isAnnotationPresent(Table.class)) {
            tableName = ((Table) classe.getAnnotation(Table.class)).name();
        }

        sqlvw.append("vw").append(tableName).append(" AS ");
        Field[] campos = classe.getDeclaredFields();
        String sep = "";
        sqlvw.append("select ");
        for (Field f : campos) {
            f.setAccessible(true);

            if (f.getDeclaredAnnotations().length == 0) {
                continue;
            }

            if (f.isAnnotationPresent(Deprecated.class)) {
                continue;
            }

            if (f.isAnnotationPresent(Fk.class)) {
                sqlvw.append(sep).append(!f.getAnnotation(Fk.class).isReference() ? PersistenceUtils.columnName(f) : f.getAnnotation(Fk.class).id().toLowerCase());
                sep = ", ";
            } else if (f.isAnnotationPresent(Id.class) || f.isAnnotationPresent(Column.class)) {
                sqlvw.append(sep).append(PersistenceUtils.columnName(f));
                sep = ", ";
            }
        }

        sqlvw.append(" FROM ").append(classe.getSimpleName().toLowerCase()).append(";");
        return sqlvw.toString();
    }

    public static String dropView(Class classe) {
        String tableName = PersistenceUtils.tableName(classe);
        StringBuilder sqlvw = new StringBuilder("DROP VIEW IF EXISTS ");

        if (classe.isAnnotationPresent(Table.class)) {
            tableName = ((Table) classe.getAnnotation(Table.class)).name();
        }

        sqlvw.append("vw").append(tableName).append(" CASCADE;");
        return sqlvw.toString();
    }

    /*
     * Retorna uma lista com o name dos arquivos para conexão no diretorio padrão
     */
    public static List<String> listaNomeConexao() {
        File f = new File(System.getProperty("user.home"));
        String[] prop = f.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".properties");
            }
        });
        List<String> conn = new ArrayList<>();
        for (String s : prop) {
            conn.add(s.substring(0, s.indexOf(".")));
        }
        return conn;
    }

    private static Long setBlobBase(byte[] bytes, Connection conn) throws IOException {
        try {
            LargeObjectManager lobj = ((org.postgresql.PGConnection) conn.getConnection()).getLargeObjectAPI();
            int n = LargeObjectManager.READ - LargeObjectManager.WRITE;
            Long oid = lobj.createLO(n);
            LargeObject obj = lobj.open(oid, LargeObjectManager.WRITE);
            obj.write(bytes);
            obj.close();
            return oid;
        } catch (SQLException ex) {
            Logger.getLogger(SQLUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static void inserirDadosCopy(final String sqlCopy, final String dados, Connection conexao) throws SQLException, IOException {
        final InputStream dadosInputStream = new ByteArrayInputStream(dados.getBytes());
        final CopyManager copy = new CopyManager((BaseConnection) conexao.getConnection());
        copy.copyIn(sqlCopy, dadosInputStream);
    }

    public static void executarSql(final String sql, Connection conexao) throws Exception {
        final Query query = new Query(conexao);
        query.addSql(sql);
        query.execute();
    }

    public static void resetDataBase(final boolean isCreate, final ClassLoader classLoader,
            final String user, final String password, final String host, final int port, final String dataBase,
            final ConnectionType connectionType, final String pacoteEntidades) throws Exception {

        resetDataBase(isCreate, classLoader, user, password, host, port, dataBase, connectionType, pacoteEntidades, null);
    }

    @SuppressWarnings({"CallToPrintStackTrace", "UseSpecificCatch"})
    public static void resetDataBase(final boolean isCreate, final ClassLoader classLoader,
            final String user, final String password, final String host, final int port, final String dataBase,
            final ConnectionType connectionType, final String pacoteEntidades, final List<String> arquivos) throws Exception {

        resetDataBase(isCreate, classLoader, user, password, host, port, dataBase, connectionType, pacoteEntidades, "public", arquivos);
    }

    @SuppressWarnings({"CallToPrintStackTrace", "UseSpecificCatch"})
    public static void resetDataBase(final boolean isCreate, final ClassLoader classLoader,
            final String user, final String password, final String host, final int port, final String dataBase,
            final ConnectionType connectionType, final String pacoteEntidades, final String schema,
            final List<String> arquivos) throws Exception {

        DB.setConnectionData(host, port, user, password, "", schema, connectionType);

        final boolean baseExiste = isExistsDataBase(user, password, host, port, dataBase, connectionType);

        Connection conexao = DB.getConnection();

        if (isCreate && !baseExiste) {
            try {
                final Query query = new Query(conexao);
                // OBS: ESTE COMANDO NÃO PODE SER EXECUTADO DENTRO DE UM BLOCO DE TRANSAÇÃO
                query.addSql("CREATE DATABASE " + dataBase + ";");
                query.execute();
            } catch (SQLException | ClassNotFoundException ex) {
                throw ex;
            } finally {
                DB.closeConnection(conexao);
            }
        }

        DB.setConnectionData(host, port, user, password, dataBase, schema, connectionType);

        conexao = DB.getConnection();

        try {
            conexao.startTransaction();

            final Builder builder = new Builder(conexao);
            builder.setPacote(pacoteEntidades);
            builder.setClassLoader(classLoader);
            // Atualiza tabelas
            builder.atualizarBanco("public");
            // Atualiza funções
            if (arquivos != null && !arquivos.isEmpty()) {
                builder.executaArquivoSQL(arquivos);
            }
            conexao.saveTransaction();
        } catch (Throwable ex) {
            conexao.cancelTransaction();
            throw ex;
        } finally {
            DB.closeConnection(conexao);
        }

    }

    public static void deleteDataBase(final String user, final String password, final String host, final int port,
            final String dataBase, final ConnectionType connectionType) throws SQLException, ClassNotFoundException {

        DB.setConnectionData(host, port, user, password, "", connectionType);

        final Connection conexao = DB.getConnection();

        try {
            final Query query = new Query(conexao);

            // Encerra todas as conexões
            query.addSql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=?;");
            query.addParam(dataBase);
            query.open();
            query.clear();
            // Remove a base de dados caso ela já exista
            query.addSql("DROP DATABASE IF EXISTS " + dataBase + ";");
            query.execute();

        } catch (SQLException | ClassNotFoundException ex) {
        } finally {
            DB.closeConnection(conexao);
        }
    }

    public static boolean isExistsDataBase(final String user, final String password, final String host, final int port,
            final String dataBase, final ConnectionType connectionType) throws ClassNotFoundException, SQLException {

        DB.setConnectionData(host, port, user, password, "", connectionType);

        final Connection conexao = DB.getConnection();

        try {
            final Query query = new Query(conexao);

            query.addSql("SELECT COUNT(1) AS count FROM pg_database WHERE datname=?;");
            query.addParam(dataBase);
            query.open();

            if (query.next()) {
                return (Long) query.getValue("count") > 0;
            }

        } catch (SQLException | ClassNotFoundException ex) {
        } finally {
            DB.closeConnection(conexao);
        }

        return false;
    }
}
