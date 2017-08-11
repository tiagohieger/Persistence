package br.com.utils;

import br.com.annotations.Column;
import br.com.annotations.Fk;
import br.com.annotations.Id;
import br.com.annotations.Table;
import br.com.annotations.Transient;
import br.com.factory.Connection;
import static br.com.utils.SQLUtils.DB;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.imageio.ImageIO;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

public class PersistenceUtils {

    public static <T> String concat(T... params) {
        return concat(null, Arrays.asList(params));
    }

    public static <T> String concat(List<T> params) {
        return concat(null, params);
    }

    public static <T> String concat(final T defaultParam, T... params) {
        return concat(defaultParam, Arrays.asList(params));
    }

    public static <T> String concat(final T defaultParam, List<T> params) {

        final StringBuilder builder = new StringBuilder();
        if (params.isEmpty() && defaultParam != null) {
            builder.append(defaultParam.toString());
        } else {
            builder.append(params.stream().map(item -> item == null ? "" : item.toString()).collect(Collectors.joining(", ")));
        }

        return builder.toString();
    }

    public static List<Field> listFields(final Class clazz) {

        final List<Field> fields = new LinkedList<>();

        if (clazz == null) {
            return fields;
        }

        final Class parent = clazz.getSuperclass();

        if (parent != null && parent != Object.class) {
            fields.addAll(listFields(parent));
        }

        fields.addAll(Arrays.asList(clazz.getDeclaredFields()));

        return fields;
    }

    public static String getIdColumn(final Class clazz) {

        final String columnName = PersistenceUtils.columnName(PersistenceUtils.getIdField(clazz));
        final String tableName = PersistenceUtils.tableName(clazz);
        final String fullColumnName = (tableName == null ? "" : tableName + ".") + columnName;

        return fullColumnName;
    }

    public static Field getIdField(final Class clazz) {

        final List<Field> tableFields = listFields(clazz);

        for (Field f : tableFields) {
            f.setAccessible(true);

            if (f.getDeclaredAnnotations().length == 0) {
                continue;
            }

            if (f.isAnnotationPresent(Id.class)) {
                return f;
            }
        }

        throw new IllegalArgumentException("A classe " + clazz.getName() + " não possuí atributo com annotaion @Id.");
    }

    public static boolean isUpdatable(final Object entity, final Field field) {

        field.setAccessible(true);

        final int declarations = field.getDeclaredAnnotations().length;
        final boolean isId = field.isAnnotationPresent(Id.class);
        final boolean isTransient = field.isAnnotationPresent(Transient.class);

        if (declarations == 0 || isId || isTransient) {
            return false;
        }

        final boolean isFk = field.isAnnotationPresent(Fk.class);
        final boolean isColumn = field.isAnnotationPresent(Column.class);

        if (!isFk && !isColumn) {
            return false;
        }

        if (isColumn) {
            try {
                if (field.getAnnotation(Column.class).notNull() && field.get(entity) == null) {
                    return false;
                }
            } catch (IllegalAccessException | IllegalArgumentException ignore) {
                return false;
            }
        }

        return true;

    }

    public static String tableName(final Class clazz) {

        if (clazz == null) {
            return null;
        }

        if (!clazz.isAnnotationPresent(Table.class)) {
            return tableName(clazz.getSuperclass());
        }

        final Table table = (Table) clazz.getAnnotation(Table.class);

        return (table.name() != null ? table.name() : clazz.getSimpleName().toLowerCase());
    }

    public static String columnName(final Field field) {

        final String fieldName;

        if (field.isAnnotationPresent(Column.class)) {

            final Column annotation = field.getAnnotation(Column.class);

            if (annotation.name().isEmpty()) {
                fieldName = field.getName().toLowerCase();
            } else {
                fieldName = annotation.name();
            }

        } else if (field.isAnnotationPresent(Fk.class)) {

            final Fk annotation = field.getAnnotation(Fk.class);

            if (annotation.name().isEmpty()) {
                fieldName = field.getName().toLowerCase();
            } else {
                fieldName = annotation.name();
            }

        } else {
            fieldName = field.getName().toLowerCase();
        }

        return fieldName;

    }

    /**
     *
     * @param field
     * @param value
     * @return
     */
    @SuppressWarnings("UseSpecificCatch")
    public static Object getValue(Field field, Object value) {
        Object v = value;
        if (field.getType().getSimpleName().equalsIgnoreCase("Boolean")) {
            v = value != null && (value.toString().equalsIgnoreCase("true") || value.equals("1"));
        } else if (field.getType().getSimpleName().equalsIgnoreCase("char") || field.getType().getName().equalsIgnoreCase("Character")) {
            v = String.valueOf(value).charAt(0);
        } else if (field.getType().getSimpleName().equalsIgnoreCase("BigDecimal")) {
            if (value != null) {
                v = new BigDecimal(String.valueOf(v));
            } else {
                v = new BigDecimal(0);
            }
        } else if (field.getType().getSimpleName().equalsIgnoreCase("Double")) {
            if (value != null) {
                v = new BigDecimal(String.valueOf(v)).doubleValue();
            } else {
                v = new BigDecimal(0).doubleValue();
            }
        } else if (field.getType().isEnum()) {
            try {
                Class c = Class.forName(field.getType().getName());
                v = Enum.valueOf(c, String.valueOf(value));
            } catch (Exception e) {
                v = null;
            }
        } else if (value != null && field.getType().isInterface()) {
            try {
                String className = String.valueOf(value);
                int last = className.lastIndexOf(".");
                Class c = Class.forName(className.substring(0, last));
                v = Enum.valueOf(c, className.substring(last + 1));
            } catch (Exception e) {
                v = null;
            }
        } else if (field.getType().getSimpleName().equalsIgnoreCase("Integer")) {
            if (value != null && !String.valueOf(value).equalsIgnoreCase("null")) {
                v = Integer.valueOf(value.toString());
            } else {
                v = null;
            }
        } else if (field.getType().getSimpleName().equalsIgnoreCase("Date")) {

            if (value != null && value instanceof java.sql.Timestamp) {
                final Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(((java.sql.Timestamp) value).getTime());
                v = calendar.getTime();
            } else if (value != null && value instanceof java.util.Date) {
                v = (java.util.Date) value;
            } else {
                v = null;
            }

        } else if (field.getType().getSimpleName().equalsIgnoreCase("Calendar")) {

            Calendar cal = Calendar.getInstance();
            try {
                String data = value.toString().replaceAll(" ", "T").replaceAll("\"", "");
                String masc1 = "yyyy-MM-dd'T'HH:mm:ss";
                String masc2 = "yyyy-MM-dd'T'HH:mm:ss.S";
                final SimpleDateFormat sdf;
                if (data.length() == 19) {
                    sdf = new SimpleDateFormat(masc1);
                } else {
                    sdf = new SimpleDateFormat(masc2);
                }
                Date dt = sdf.parse(data);
                cal.setTime(dt);
                v = cal;
            } catch (ParseException ex) {
            }
        } else if (isFileType(field)) {
            Connection con;
            try {
                con = DB.getConnection();
            } catch (SQLException | ClassNotFoundException ex) {
                Logger.getLogger(PersistenceUtils.class.getName()).log(Level.SEVERE, null, ex);
                return null;
            }
            try {
                Long l = null;
                if (value != null) {
                    if (!String.valueOf(value).equalsIgnoreCase("null")) {
                        l = Long.valueOf(value.toString());
                    }
                    ByteArrayInputStream bais = new ByteArrayInputStream(loadBLOBBase(l, con));
                    v = ImageIO.read(bais);
                }
            } catch (SQLException ex) {
                Logger.getLogger(SQLUtils.class.getName()).log(Level.SEVERE, null, ex);
            } catch (Exception ex) {
                Logger.getLogger(SQLUtils.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                DB.closeConnection(con);
            }
        }
        return v;
    }

    private static boolean isFileType(Field f) {
        return f.getType().getSimpleName().equalsIgnoreCase("BufferedImage")
                || f.getType().getSimpleName().equalsIgnoreCase("Image")
                || f.getType().getSimpleName().equalsIgnoreCase("ImageIcon");
    }

    /**
     * Metodo que retorna um arquivo gravado no banco de dados retornando em um
     * array de bytes
     *
     * @param oid codigo do arquivo
     * @param conn conexão com a base
     * @return retorna um array de bytes com o arquivo carregado para
     * manipulação
     * @throws FileNotFoundException
     * @throws IOException
     * @throws SQLException
     */
    private static byte[] loadBLOBBase(Long oid, Connection conn) throws FileNotFoundException, IOException, SQLException, Exception {
        try {
            conn.getConnection().setAutoCommit(false);
            LargeObjectManager lobj = ((org.postgresql.PGConnection) conn.getConnection()).getLargeObjectAPI();
            LargeObject obj = lobj.open(oid, LargeObjectManager.READ);
            int s = obj.size();
            byte[] ret = obj.read(s);
            obj.close();
            return ret;
        } finally {
            conn.getConnection().setAutoCommit(true);
        }
    }
}
