package br.com.generic;

import br.com.annotations.Column;
import br.com.annotations.Fk;
import br.com.annotations.Id;
import br.com.factory.FunctionParam;
import br.com.factory.Query;
import br.com.constants.IDAO;
import br.com.factory.Connection;
import br.com.utils.PersistenceUtils;
import br.com.utils.SQLUtils;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class GenericDao<T> implements IDAO<T> {

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    protected Query qrysql = null;
    protected Connection connection = null;
    protected Class specificClass = null;

    public GenericDao(Connection conexao) {
        this.connection = conexao;
        specificClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        qrysql = new Query(this.connection);
    }

    public GenericDao(Connection conexao, Class especificClass) {
        this.connection = conexao;
        specificClass = especificClass;
        qrysql = new Query(this.connection);
    }

    public void setClasse(Class especificClass) {
        this.specificClass = especificClass;
    }

    public void executeFunction(final String functionName, Object... param) throws Exception {

        final Query query = new Query(connection);
        query.executeFunction(new FunctionParam(functionName, param));
    }

    @Override
    public void insert(T entity) throws IllegalArgumentException, IllegalAccessException, Exception {
        qrysql.clear();
        final Map<String, Object[]> values = SQLUtils.getInsertSQL(entity);
        for (Entry<String, Object[]> entry : values.entrySet()) {
            final ResultSet resultSet = qrysql.executeCommandReturn(entry.getKey(), entry.getValue());
            setValuesEntity(entity, resultSet);
        }

    }

    @Override
    public int update(T entity) throws Exception {
        qrysql.clear();
        Map<String, Object[]> values = SQLUtils.getUpdateSQL(entity);
        String keysql = values.keySet().iterator().next();
        return qrysql.executeCommand(keysql, values.get(keysql));
    }

    @Override
    public T save(final T entity) throws Exception {

        final List<Field> fields = PersistenceUtils.listFields(entity.getClass());

        for (Field field : fields) {

            if (field.isAnnotationPresent(Id.class)) {

                field.setAccessible(true);

                final Object value = field.get(entity);

                if (value == null) {
                    insert(entity);
                } else {
                    update(entity);
                }

                break;
            }
        }

        return entity;

    }

    @Override
    public void remove(int id) throws Exception {
        qrysql.clear();
        qrysql.setText(SQLUtils.getDeleteById(specificClass));
        qrysql.addParam(1, id);
        qrysql.execute();
    }

    @Override
    public void removeAll() throws Exception {
        qrysql.clear();
        qrysql.setText(SQLUtils.getDeleteAll(specificClass));
        qrysql.execute();
    }

    @Override
    public void truncate() throws Exception {
        qrysql.clear();
        qrysql.setText(SQLUtils.getDeleteAll(specificClass));
        qrysql.execute();
    }

    @Override
    public void deleteByParams(Object[]... params) throws Exception {
        int index = 1;
        qrysql.clear();
        qrysql.setText(SQLUtils.getDeleteByParams(specificClass, params));
        for (Object[] param : params) {
            qrysql.addParam(index, param[1]);
            index++;
        }
        qrysql.execute();
    }

    @Override
    public T getEntity(int id, boolean isdepenteces) throws Exception {
        qrysql.setText(SQLUtils.getSelectByID(specificClass, isdepenteces));
        qrysql.addParam(1, id);
        qrysql.open();
        T t = null;
        if (qrysql.next()) {
            if (isdepenteces) {
                Object toreturn = SQLUtils.entityPopulate(qrysql, specificClass);
                loadDependeces((T) toreturn, qrysql);
                t = (T) toreturn;
            } else {
                t = (T) SQLUtils.entityPopulate(qrysql, specificClass);
            }
        }
        return t;
    }

    protected void loadDependeces(Object ent, Query qrydep) throws IllegalArgumentException, IllegalAccessException, InstantiationException, SQLException {
        Field[] fields = ent.getClass().getDeclaredFields();
        for (Field f : fields) {
            f.setAccessible(true);
            if (f.isAnnotationPresent(Fk.class)) {
                f.set(ent, SQLUtils.entityPopulate(qrydep, f.getType()));
            }
        }
    }

    public Object getEntityByID(Class entity, int id, boolean isdepenteces) throws Exception {
        Query qry = new Query(connection);
        qry.setText(SQLUtils.getSelectByID(entity, isdepenteces));
        qry.addParam(1, id);
        qry.open();
        Object toreturn = null;
        if (qry.next()) {
            if (isdepenteces) {
                toreturn = SQLUtils.entityPopulate(qry, entity);
                loadDependeces(toreturn, qry);
            } else {
                toreturn = SQLUtils.entityPopulate(qry, entity);
            }
        }
        return toreturn;
    }

    @Override
    public List<T> listEntity(boolean orderbyId, boolean isdepenteces) throws InstantiationException, IllegalAccessException, SQLException, ClassNotFoundException {
        final List<T> listEntity = new LinkedList<>();
        qrysql.clear();
        String orderby = "";
        if (orderbyId) {
            orderby = " order by ";
            String sep = "";
            final List<Field> camposTabela = new LinkedList<>();
            camposTabela.addAll(PersistenceUtils.listFields(specificClass));
            for (Field f : camposTabela) {
                f.setAccessible(true);
                if (f.isAnnotationPresent(Id.class)) {
                    orderby += sep + PersistenceUtils.columnName(f);
                    sep = ", ";
                }
            }
        }
        qrysql.setText(SQLUtils.getSelectFull(specificClass, isdepenteces) + orderby);
        qrysql.open();
        while (qrysql.next()) {
            if (isdepenteces) {
                Object toreturn = SQLUtils.entityPopulate(qrysql, specificClass);
                loadDependeces((T) toreturn, qrysql);
                listEntity.add((T) toreturn);
            } else {
                listEntity.add((T) SQLUtils.entityPopulate(qrysql, specificClass));
            }
        }
        return listEntity;
    }

    @Override
    public List<T> listEntityWhere(String where, boolean isdepenteces) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException {
        List<T> listEntity = new LinkedList<>();
        qrysql.clear();
        qrysql.setText(SQLUtils.getSelectFull(specificClass, isdepenteces) + " " + where);
        qrysql.open();
        while (qrysql.next()) {
            if (isdepenteces) {
                Object toreturn = SQLUtils.entityPopulate(qrysql, specificClass);
                loadDependeces((T) toreturn, qrysql);
                listEntity.add((T) toreturn);
            } else {
                listEntity.add((T) SQLUtils.entityPopulate(qrysql, specificClass));
            }
        }
        return listEntity;
    }

    @Override
    public T getEntityWhere(String where, boolean isdepenteces) throws InstantiationException, IllegalAccessException, SQLException, ClassNotFoundException {
        T entity = null;
        qrysql.clear();
        qrysql.setText(SQLUtils.getSelectFull(specificClass, isdepenteces) + " " + where);
        qrysql.open();
        while (qrysql.next()) {
            if (isdepenteces) {
                Object toreturn = SQLUtils.entityPopulate(qrysql, specificClass);
                loadDependeces((T) toreturn, qrysql);
                entity = (T) toreturn;
            } else {
                entity = (T) SQLUtils.entityPopulate(qrysql, specificClass);
            }
        }
        return entity;
    }

    protected void setValuesEntity(T entity, ResultSet resultSet) throws Exception {

        List<Field> camposTabela = new LinkedList<>();
        camposTabela.addAll(PersistenceUtils.listFields(entity.getClass()));

        if (resultSet.next()) {
            for (Field field : camposTabela) {
                field.setAccessible(true);
                if (field.isAnnotationPresent(Id.class)) {
                    final Object valor = resultSet.getObject(PersistenceUtils.columnName(field));
                    field.set(entity, PersistenceUtils.getValue(field, valor));
                } else if (field.isAnnotationPresent(Column.class)
                        && !field.getAnnotation(Column.class).defaultValue().equals("")) {
                    final String columnName = PersistenceUtils.columnName(field);

                    final Object valor = resultSet.getObject(columnName);
                    field.set(entity, PersistenceUtils.getValue(field, valor));

                }
            }
        }
    }
}
