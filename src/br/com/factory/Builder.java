package br.com.factory;

import br.com.annotations.Column;
import br.com.annotations.Fk;
import br.com.annotations.Id;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.atatec.trugger.scan.ClassScan;
import br.com.utils.PersistenceUtils;
import java.beans.Transient;

public class Builder {

    private final Connection conexao;
    private final Map<String, LinkedList<Field>> mapCamposTabela;
    private final BuilderDAO builderDAO;
    private String pacote;
    private ClassLoader loader;
    private final List<String> listFK;
    private final List<String> listPK;

    public Builder(Connection conexao) {
        this.conexao = conexao;
        this.mapCamposTabela = new HashMap<>();
        this.builderDAO = new BuilderDAO(conexao);
        this.listFK = new ArrayList<>();
        this.listPK = new ArrayList<>();
    }

    public void setPacote(String pacote) {
        this.pacote = pacote;
    }

    public void setClassLoader(ClassLoader loader) {
        this.loader = loader;
    }

    public Set<Class> carregaEntidades() {
        if (pacote == null) {
            throw new IllegalArgumentException("pacote is null");
        }
        if (loader == null) {
            throw new IllegalArgumentException("loader is null");
        }
        return ClassScan.newScan().with(loader).findAll().in(pacote);
    }

    public static Set<Class> listClass(final String pacote) throws IOException, ClassNotFoundException {
        return ClassScan.newScan().with(Builder.class.getClassLoader()).findAll().in(pacote);
    }

    public void atualizarBanco(final String esquema) throws SQLException, ClassNotFoundException {
        deletarTodasFuncoes();
        deletarTodasViews();
        criarAtualizarTabelas(esquema);
    }

    public boolean criarAtualizarTabelas(final String esquema) throws SQLException, ClassNotFoundException {
        //carregar lista de tabelas da base de dados
        final List<String> listTabelas = listarTabelas(esquema);
        // Carrega as entidades a partir do pacote setado
        final Set<Class> setEntidades = carregaEntidades();
        if (setEntidades != null) {
            System.out.println("Gerando SQL's...");
            final List<String> listSQLTabelasEColunas = gerarSQLTabelasEColunas(listTabelas, setEntidades);
            System.out.println("Criando tabelas...");
            criarTabelasEColunas(listSQLTabelasEColunas);
            System.out.println("Tabelas criadas com sucesso!");
            System.out.println("Adicionando chaves primarias...");
            criarConstraint(listPK);
            System.out.println("Adicionando chaves estrangeiras...");
            criarConstraint(listFK);
            System.out.println("Adicionado!");
            return true;
        }
        return false;
    }

    private void popularMapCamposTabela(final Set<Class> entities) {

        entities.stream().filter(entityClass -> PersistenceUtils.tableName(entityClass) != null)
                .forEach(entity -> {

                    final String tableName = PersistenceUtils.tableName(entity);

                    // Lista todos os atributos da classe, além de, poder retornar inclusive atributos da super classe,
                    // caso necessário
                    final List<Field> fields = PersistenceUtils.listFields(entity);

                    if (this.mapCamposTabela.get(tableName) == null) {
                        this.mapCamposTabela.put(tableName, new LinkedList<>());
                    }

                    fields.stream()
                    .filter(field -> !campoExiste(this.mapCamposTabela.get(tableName), field))
                    .forEachOrdered(field -> this.mapCamposTabela.get(tableName).add(field));
                });
    }

    public void criarTabelasEColunas(final List<String> sqlTabelasEColunas) throws SQLException, ClassNotFoundException {

        for (final String sqlTabelasEColuna : sqlTabelasEColunas) {

            if (!sqlTabelasEColuna.trim().equals("")) {
                this.builderDAO.executaSQL(sqlTabelasEColuna);
            }
        }
    }

    public void criarConstraint(final List<String> listConstraint) throws SQLException, ClassNotFoundException {

        for (final String constraint : listConstraint) {

            if (!constraint.trim().equals("")) {
                this.builderDAO.executaSQL(constraint);
            }
        }
    }

    private boolean campoExiste(final List<Field> campos, final Field campo) {

        if (campo == null) {
            // Retorna true para que campos não seja inserido na lista
            return true;
        }

        for (final Field c : campos) {
            if (c.equals(campo)) {
                return true;
            }
        }

        return false;
    }

    private String gerarSQLTabela(final String nomeTabela) {

        final List<Field> listAtributos = this.mapCamposTabela.get(nomeTabela);

        final StringBuilder builder = new StringBuilder();
        builder.append(" CREATE TABLE ").append(nomeTabela).append(" ( ");

        listAtributos.forEach((atributo) -> {
            final String SQLColuna = gerarSQLAddColuna(atributo);
            if (SQLColuna != null && !SQLColuna.trim().isEmpty()) {
                builder.append(SQLColuna).append(", ");

                // Gera uma constraint para este atributo, caso for preciso
                gerarSQLConstraint(nomeTabela, atributo);
            }
        });

        final int lastIndex = builder.lastIndexOf(",");
        if (lastIndex > -1) {
            builder.deleteCharAt(lastIndex);
        }
        builder.append(" ); ");

        return builder.toString();
    }

    private String gerarSQLRemoveCampos(final String nomeTabela) throws SQLException, ClassNotFoundException {

        // Campos da table
        final List<String> camposTabela = this.builderDAO.buscaCamposTabela(nomeTabela);
        // Campos da classe
        final List<Field> listAtributos = this.mapCamposTabela.get(nomeTabela);

        final StringBuilder builder = new StringBuilder();

        camposTabela.stream().filter(column -> !column.isEmpty()).forEach(column -> {

            // Verifica se a coluna ainda existe
            boolean contains = false;
            for (final Field field : listAtributos) {

                final String fieldName = PersistenceUtils.columnName(field);
                final String oldFieldName;

                if (field.isAnnotationPresent(Column.class)) {
                    final Column annotation = field.getAnnotation(Column.class);
                    oldFieldName = annotation.itWas();
                } else {
                    oldFieldName = "";
                }

                if (column.equals(fieldName)) {
                    contains = true;
                    break;
                } else if (column.equals(oldFieldName)) {
                    // Caso a nova coluna já exista, esta pode ser removida
                    if (!camposTabela.contains(fieldName)) {
                        // Se não existe será renomeada, então não podesso excluir,
                        // por isso o contains = true
                        contains = true;
                        break;
                    }
                }
            }

            if (!contains) {
                // Cria o SQL que remove a coluna que já não existe mais
                builder.append(" ALTER TABLE ");
                builder.append(nomeTabela);
                builder.append(" DROP COLUMN ");
                builder.append(column);
                builder.append("; ");
            }

        });

        return builder.toString();
    }

    private String gerarSQLRenomeiaCampos(final String nomeTabela) throws SQLException, ClassNotFoundException {

        // Colunas na base
        final List<String> camposTabela = this.builderDAO.buscaCamposTabela(nomeTabela);
        // Campos da classe
        final List<Field> listAtributos = this.mapCamposTabela.get(nomeTabela);

        if (listAtributos == null || listAtributos.isEmpty()) {
            return "";
        }

        final StringBuilder builder = new StringBuilder();

        listAtributos.stream().filter(field
                -> field.getDeclaredAnnotations().length > 0
                && !field.isAnnotationPresent(Transient.class)
                && field.isAnnotationPresent(Column.class)
                && !field.getAnnotation(Column.class).itWas().isEmpty()
                && camposTabela.contains(field.getAnnotation(Column.class).itWas())
                && !camposTabela.contains(PersistenceUtils.columnName(field))).forEachOrdered(field -> {

                    final Column annotation = field.getAnnotation(Column.class);
                    final String fieldName = PersistenceUtils.columnName(field);
                    final String oldFieldName = annotation.itWas();

                    builder.append(" ALTER TABLE ");
                    builder.append(nomeTabela);
                    builder.append(" RENAME COLUMN ");
                    builder.append(oldFieldName);
                    builder.append(" TO ");
                    builder.append(fieldName);
                    builder.append("; ");

                });

        return builder.toString();
    }

    private String gerarSQLNovosCampos(final String nomeTabela) throws SQLException, ClassNotFoundException {

        // Busca no banco de dados os campos da table
        final List<String> listCamposTabela = this.builderDAO.buscaCamposTabela(nomeTabela);

        // Campos da classe
        final List<Field> listAtributos = this.mapCamposTabela.get(nomeTabela);

        final StringBuilder builder = new StringBuilder();

        for (final Field field : listAtributos) {

            if (field.getDeclaredAnnotations().length == 0) {
                continue;
            }

            if (field.isAnnotationPresent(Transient.class)) {
                continue;
            }

            /*
             * Como a coluna será renomeada ela não será criada 
             */
            if (field.isAnnotationPresent(Column.class)) {
                if (!field.getAnnotation(Column.class).itWas().isEmpty()) {
                    continue;
                }
            }

            String nomeCampo = PersistenceUtils.columnName(field);

            if (field.isAnnotationPresent(Fk.class)) {
                nomeCampo = ((Fk) field.getAnnotation(Fk.class)).name();
            }

            if (listCamposTabela.indexOf(nomeCampo) == -1) {
                // Cria o SQL que adiciona a nova coluna na table
                builder.append(" ALTER TABLE ");
                builder.append(nomeTabela);
                builder.append(" ADD COLUMN ");
                // Gera o SQL especifico da coluna
                final String sql = gerarSQLAddColuna(field);

                // Gera uma constraint para este que será adicionado, caso for preciso
                gerarSQLConstraint(nomeTabela, field);

                if (sql != null) {
                    builder.append(sql).append("; ");
                }
            }
        }

        return builder.toString();
    }

    private String gerarSQLUpdateNotNull(final String nomeTabela) throws SQLException, ClassNotFoundException {

        // Campos da classe
        final List<Field> listAtributos = this.mapCamposTabela.get(nomeTabela);

        if (listAtributos == null || listAtributos.isEmpty()) {
            return "";
        }

        final StringBuilder builder = new StringBuilder();

        listAtributos.stream().filter(field
                -> field.getDeclaredAnnotations().length > 0
                && field.isAnnotationPresent(Column.class)).forEachOrdered(field -> {

                    final Column annotation = field.getAnnotation(Column.class);
                    final String fieldName = PersistenceUtils.columnName(field);

                    if (annotation.notNull() && annotation.defaultValue() != null && !annotation.defaultValue().isEmpty()) {
                        builder.append(" UPDATE ");
                        builder.append(nomeTabela);
                        builder.append(" SET ");
                        builder.append(fieldName);
                        builder.append(" = ");
                        if (isText(field)) {
                            builder.append("'");
                            builder.append(annotation.defaultValue());
                            builder.append("'");
                        } else {
                            builder.append(annotation.defaultValue());
                        }
                        builder.append(" WHERE ");
                        builder.append(fieldName);
                        builder.append(" IS NULL; ");
                    }
                    
                    builder.append(" ALTER TABLE ");
                    builder.append(nomeTabela);
                    builder.append(" ALTER COLUMN ");
                    builder.append(fieldName);

                    if (annotation.notNull()) {
                        builder.append(" SET NOT NULL; ");
                    } else {
                        builder.append(" DROP NOT NULL; ");
                    }

                });

        return builder.toString();
    }

    private String gerarSQLUpdateTipo(final String nomeTabela) throws SQLException, ClassNotFoundException {

        // Campos da classe
        final List<Field> listAtributos = this.mapCamposTabela.get(nomeTabela);

        if (listAtributos == null || listAtributos.isEmpty()) {
            return "";
        }

        final StringBuilder builder = new StringBuilder();

        listAtributos.stream().filter(field
                -> field.getDeclaredAnnotations().length > 0
                && field.isAnnotationPresent(Column.class)).forEachOrdered(field -> {

                    final Column annotation = field.getAnnotation(Column.class);
                    final String fieldName = PersistenceUtils.columnName(field);

                    builder.append(" ALTER TABLE ");
                    builder.append(nomeTabela);
                    builder.append(" ALTER COLUMN ");
                    builder.append(fieldName);
                    builder.append(" TYPE ");
                    builder.append(getTipo(field));
                    builder.append("; ");

                });

        return builder.toString();
    }

    private String gerarSQLUpdateUnique(final String nomeTabela) throws SQLException, ClassNotFoundException {

        // Campos da classe
        final List<Field> listAtributos = this.mapCamposTabela.get(nomeTabela);

        if (listAtributos == null || listAtributos.isEmpty()) {
            return "";
        }

        final StringBuilder builder = new StringBuilder();

        listAtributos.stream().filter(field
                -> field.getDeclaredAnnotations().length > 0
                && field.isAnnotationPresent(Column.class)).forEachOrdered(field -> {

                    final Column annotation = field.getAnnotation(Column.class);
                    final String fieldName = PersistenceUtils.columnName(field);
                    final String constraintName = nomeTabela + "_" + fieldName + "_key";

                    // Primeiro remove a constraint
                    // Isso para não ter que ir no banco verificar se já existe
                    builder.append(" ALTER TABLE ");
                    builder.append(nomeTabela);
                    builder.append(" DROP CONSTRAINT IF EXISTS ");
                    builder.append(constraintName);
                    builder.append("; ");
                    // E se necessário for adiciona novamente
                    if (annotation.unique()) {
                        builder.append(" ALTER TABLE ");
                        builder.append(nomeTabela);
                        builder.append(" ADD CONSTRAINT ");
                        builder.append(constraintName);
                        builder.append(" UNIQUE (");
                        builder.append(fieldName);
                        builder.append("); ");
                    }

                });

        return builder.toString();
    }

    private String gerarSQLUpdateDefault(final String nomeTabela) throws SQLException, ClassNotFoundException {

        // Campos da classe
        final List<Field> listAtributos = this.mapCamposTabela.get(nomeTabela);

        if (listAtributos == null || listAtributos.isEmpty()) {
            return "";
        }

        final StringBuilder builder = new StringBuilder();

        listAtributos.stream().filter(field
                -> field.getDeclaredAnnotations().length > 0
                && field.isAnnotationPresent(Column.class)).forEachOrdered(field -> {

                    final Column annotation = field.getAnnotation(Column.class);
                    final String fieldName = PersistenceUtils.columnName(field);

                    builder.append(" ALTER TABLE ");
                    builder.append(nomeTabela);
                    builder.append(" ALTER COLUMN ");
                    builder.append(fieldName);

                    if (annotation.defaultValue() == null || annotation.defaultValue().isEmpty()) {
                        builder.append(" DROP DEFAULT; ");
                    } else {
                        builder.append(" SET DEFAULT ");
                        if (isText(field)) {
                            builder.append("'");
                            builder.append(annotation.defaultValue());
                            builder.append("'");
                        } else {
                            builder.append(annotation.defaultValue());
                        }
                        builder.append("; ");
                    }

                });

        return builder.toString();
    }

    private String gerarSQLFkConstraint(final String nomeTabela) throws SQLException, ClassNotFoundException {

        // Campos da classe
        final List<Field> listAtributos = this.mapCamposTabela.get(nomeTabela);

        if (listAtributos == null || listAtributos.isEmpty()) {
            return "";
        }

        final StringBuilder builder = new StringBuilder();

        listAtributos.stream().filter(field
                -> field.getDeclaredAnnotations().length > 0
                && field.isAnnotationPresent(Fk.class)).forEachOrdered(field -> {

                    builder.append(getConstraint(nomeTabela, field));
                    builder.append("; ");

                });

        return builder.toString();
    }

    private String gerarSQLAddColuna(final Field field) {

        if (field.isAnnotationPresent(Transient.class)) {
            return null;
        }
        final StringBuilder sqlColuna = new StringBuilder();

        if (field.isAnnotationPresent(Column.class)) {

            final Column coluna = (Column) field.getAnnotation(Column.class);
            // Adiciona o name da coluna
            sqlColuna.append(PersistenceUtils.columnName(field));
            sqlColuna.append(" ");
            // Gera o SQL especifico do tipo da coluna, ex: character varying(50)
            sqlColuna.append(getTipo(field));

            if (coluna.unique()) {
                sqlColuna.append(" UNIQUE ");
            }
            if (coluna.notNull()) {
                sqlColuna.append(" NOT NULL ");
            }
            if (coluna.defaultValue() != null && !coluna.defaultValue().trim().isEmpty()) {
                sqlColuna.append(" DEFAULT ");
                if (isText(field)) {
                    sqlColuna.append("'");
                    sqlColuna.append(coluna.defaultValue());
                    sqlColuna.append("'");
                } else {
                    sqlColuna.append(coluna.defaultValue());
                }
            }

        } else if (field.isAnnotationPresent(Fk.class)) {

            final Fk fk = (Fk) field.getAnnotation(Fk.class);
            // Caso seja uma FK, o name da coluna será o name definido na propriedade 'columnName'.
            sqlColuna.append(((Fk) field.getAnnotation(Fk.class)).name().toLowerCase());
            sqlColuna.append(" INTEGER ");
            if (fk.notNull()) {
                sqlColuna.append(" NOT NULL ");
            }

        } else if (field.isAnnotationPresent(Id.class)) {

            sqlColuna.append(PersistenceUtils.columnName(field));
            sqlColuna.append(" SERIAL ");
        }

        return sqlColuna.toString();
    }

    private void gerarSQLConstraint(final String nomeTabela, final Field field) {

        if (field.isAnnotationPresent(Transient.class)) {
            return;
        }

        if (field.isAnnotationPresent(Id.class)) {
            this.listPK.add(getConstraint(nomeTabela, field));
        } else if (field.isAnnotationPresent(Fk.class)) {
            this.listFK.add(getConstraint(nomeTabela, field));
        }
    }

    public List<String> gerarSQLTabelasEColunas(final List<String> listTabelas, Set<Class> setEntidades) throws SQLException, ClassNotFoundException {

        // O map vai conter todas os campos que devem ser criados em uma determinada table,
        // tendo como chave o name da table.
        // Nome do map: mapCamposTabela
        popularMapCamposTabela(setEntidades);

        final List<String> sqlTabelasEColunas = new ArrayList<>();

        // este map abaixo é o map que foi populado na chamada de método acima, sendo ele: populaMapCamposTabela
        for (final String nomeTabela : this.mapCamposTabela.keySet()) {

            if (listTabelas.indexOf(nomeTabela) == -1) {
                // Adiciona na lista para não ser criado novamente
                listTabelas.add(nomeTabela);
                // Cria uma nova table
                sqlTabelasEColunas.add(gerarSQLTabela(nomeTabela));
            } else {
                // Renomeia os campos com itWas
                final String sqlRename = gerarSQLRenomeiaCampos(nomeTabela);
                if (!sqlRename.isEmpty()) {
                    sqlTabelasEColunas.add(sqlRename);
                }
                // Campos que não existem mais
                final String sqlDelete = gerarSQLRemoveCampos(nomeTabela);
                if (!sqlDelete.isEmpty()) {
                    sqlTabelasEColunas.add(sqlDelete);
                }
                // Cria novas colunas na table, caso seja necessário
                final String sqlAdd = gerarSQLNovosCampos(nomeTabela);
                if (!sqlAdd.isEmpty()) {
                    sqlTabelasEColunas.add(sqlAdd);
                }
                // Atualiza o not null das colunas
                final String sqlNotNull = gerarSQLUpdateNotNull(nomeTabela);
                if (!sqlNotNull.isEmpty()) {
                    sqlTabelasEColunas.add(sqlNotNull);
                }
                // Atualiza o tipo das colunas
                final String sqlTipo = gerarSQLUpdateTipo(nomeTabela);
                if (!sqlTipo.isEmpty()) {
                    sqlTabelasEColunas.add(sqlTipo);
                }
                // Atualiza se a coluna é Unique ou não
                final String sqlUnique = gerarSQLUpdateUnique(nomeTabela);
                if (!sqlUnique.isEmpty()) {
                    sqlTabelasEColunas.add(sqlUnique);
                }
                // Atualiza o valor DEFAULT das colunas
                final String sqlDefault = gerarSQLUpdateDefault(nomeTabela);
                if (!sqlDefault.isEmpty()) {
                    sqlTabelasEColunas.add(sqlDefault);
                }

                // Atualiza as constraints de FK's principalmente por causa dos
                // DELETE ON CASCADE
                final String sqlConstraint = gerarSQLFkConstraint(nomeTabela);
                if (!sqlConstraint.isEmpty()) {
                    listFK.add(sqlConstraint);
                }
            }

        }

        return sqlTabelasEColunas;
    }

    private String gerarSQLView(final Class classe) {

        final StringBuilder vwSQL = new StringBuilder(" CREATE OR REPLACE VIEW ");
        // Busca o mesmo name ao qual a table foi criada
        final String nomeTabela = PersistenceUtils.tableName(classe);

        if (nomeTabela == null) {
            return "";
        }
        vwSQL.append(" vw");
        vwSQL.append(nomeTabela);
        vwSQL.append(" AS ");
        vwSQL.append(" SELECT * ");
        vwSQL.append(" FROM ");
        vwSQL.append(nomeTabela);
        vwSQL.append(";");

        return vwSQL.toString();
    }

    public static Field buscaCampoEntidade(final Class clazz, String nomeCampo) throws NoSuchFieldException {
        final List<Field> fields = PersistenceUtils.listFields(clazz);
        for (Field campo : fields) {
            if (campo.getName().equalsIgnoreCase(nomeCampo)) {
                return campo;
            }
        }
        throw new NoSuchFieldException("O campo " + nomeCampo + " não existe.");
    }

    public List<String> listarTabelas(String esquema) throws SQLException {
        return this.conexao.listTables(esquema);
    }

    public List<String> listarTodasViews() throws SQLException, ClassNotFoundException {
        return this.builderDAO.listaTodasViews();
    }

    public void deletarTodasFuncoes() throws SQLException, ClassNotFoundException {

        System.out.println("\nDeletando as todas as funcoes da base....");
        // lista todas as funções do banco
        final List<String> dropFuncoes = this.builderDAO.listaDropFuncoes();
        String funcao = "";

        try {
            for (final String dropFuncao : dropFuncoes) {
                funcao = dropFuncao;
                this.builderDAO.executaSQL(dropFuncao);
            }
            System.out.println("\nFunções deletadas com sucesso!");
        } catch (SQLException | ClassNotFoundException ex) {
            System.err.println("Erro ao tentar excluir função:" + funcao);
            Logger.getLogger(Builder.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
    }

    public void deletarTodasViews() throws SQLException, ClassNotFoundException {

        System.out.println("Deletando as todas as Views da base...");

        // lista todas a views do banco
        final List<String> views = listarTodasViews();
        String vw = "";

        try {
            for (final String view : views) {
                vw = view;
                this.builderDAO.deletaView(view);
            }

            System.out.println("Views deletadas com sucesso!");

        } catch (SQLException | ClassNotFoundException ex) {
            System.err.println("Erro ao tentar excluir view:" + vw);
            Logger.getLogger(Builder.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
    }

    public void criarTodasViews() throws SQLException, ClassNotFoundException {

        // Lista todas as entidades existente no pacote
        Set<Class> listaEntidades = carregaEntidades();
        if (listaEntidades == null) {
            return;
        }
        System.out.println("Criando todas views...");
        String view = "";

        try {
            for (final Class c : listaEntidades) {
                final String vwSQL = gerarSQLView(c);

                if (vwSQL != null) {
                    view = PersistenceUtils.tableName(c);
                    this.builderDAO.executaSQL(vwSQL);
                }
            }
            System.out.println("Views criadas com sucesso!");
        } catch (SQLException | ClassNotFoundException ex) {
            System.err.println("Erro ao criar view: " + view);
            Logger.getLogger(Builder.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
    }

    public void executaArquivoSQL(final List<String> listaArquivosSQL) throws SQLException, ClassNotFoundException {

        System.out.println("\nCriando escripts SQLs...");
        for (final String file : listaArquivosSQL) {

            String content;

            try {
                content = readOnDesktop(file);
            } catch (Exception ex) {
                content = readOnWebService(file);
            }

            this.builderDAO.executaSQL(content);
        }
        System.out.println("\nEscripts criados com sucesso!");
    }

    private String readOnDesktop(final String file) throws IOException {

        String content = "";
        final InputStream inputStream = ClassLoader.getSystemClassLoader().getResource(file).openStream();
        try (BufferedReader leitor = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))) {
            String linha;
            while ((linha = leitor.readLine()) != null) {
                content += linha + "\n";
            }
            leitor.close();
        }
        return content;
    }

    private String readOnWebService(final String file) {
        String content = "";
        final Scanner sc = new Scanner(getClass().getResourceAsStream((file.startsWith("/") ? "" : "/") + file), "UTF-8");
        while (sc.hasNextLine()) {
            final String line = sc.nextLine();
            content += line + "\n";
        }
        return content;
    }

    private String getTipo(final Field field) {

        final StringBuilder sqlTipoColuna = new StringBuilder();
        final String atributo = field.getType().getSimpleName().toLowerCase();
        final Column coluna = (Column) field.getAnnotation(Column.class);

        switch (atributo) {
            case "double":
                sqlTipoColuna.append(" NUMERIC (8,3) ");
                break;
            case "long":
                 sqlTipoColuna.append(" BIGINT ");
                break;
            case "bufferedimage":
            case "image":
            case "imageicon":
                sqlTipoColuna.append(" OID ");
                break;
            case "date":
            case "calendar":
                sqlTipoColuna.append(" TIMESTAMP ");
                break;
            case "boolean":
                sqlTipoColuna.append(" BOOLEAN ");
                break;
            case "integer":
            case "int":
                if (coluna.isBigInt()) {
                    sqlTipoColuna.append(" BIGINT ");
                } else {
                    sqlTipoColuna.append(" INTEGER ");
                }
                break;
            case "string":
            case "char":
            default:
                if (coluna.isText()) {
                    sqlTipoColuna.append(" TEXT ");
                } else {
                    sqlTipoColuna.append(" CHARACTER VARYING( ");
                    sqlTipoColuna.append(coluna.length());
                    sqlTipoColuna.append(" ) ");
                }
        }

        return sqlTipoColuna.toString();
    }

    private boolean isText(final Field field) {

        final String atributo = field.getType().getSimpleName().toLowerCase();

        switch (atributo) {
            case "double":
            case "long":
            case "bufferedimage":
            case "image":
            case "imageicon":
            case "date":
            case "calendar":
            case "boolean":
            case "integer":
            case "int":
                return false;
            case "string":
            case "char":
            default:
                return true;
        }
    }

    private String getConstraint(final String nomeTabela, final Field field) {

        if (field.isAnnotationPresent(Transient.class)) {
            return "";
        }

        final StringBuilder builder = new StringBuilder();

        if (field.isAnnotationPresent(Id.class)) {

            final String nomeAtributo = PersistenceUtils.columnName(field);
            final String constraintName = "pk_" + nomeTabela + "_" + nomeAtributo;

            builder.append(" ALTER TABLE ");
            builder.append(nomeTabela).append(" ADD CONSTRAINT ");
            builder.append(constraintName);
            builder.append(" PRIMARY KEY (");
            builder.append(nomeAtributo).append(") ");

        } else if (field.isAnnotationPresent(Fk.class)) {

            final String tabelaReferencia = field.getAnnotation(Fk.class).table();
            final String colunaReferencia = field.getAnnotation(Fk.class).id();
            final String nomeColuna = PersistenceUtils.columnName(field);
            final boolean deleteCascade = field.getAnnotation(Fk.class).deleteOnCascade();
            final String constraintName = "fk_" + nomeTabela + "_" + nomeColuna + "_" + tabelaReferencia;

            builder.append(" ALTER TABLE ");
            builder.append(nomeTabela);
            builder.append(" DROP CONSTRAINT IF EXISTS ");
            builder.append(constraintName);
            builder.append(" ,ADD CONSTRAINT ");
            builder.append(constraintName);
            builder.append(" FOREIGN KEY (");
            builder.append(nomeColuna);
            builder.append(") REFERENCES ");
            builder.append(tabelaReferencia).append(" ( ");
            builder.append(colunaReferencia);
            builder.append(" ) ");
            if (deleteCascade) {
                builder.append(" ON DELETE CASCADE ");
            } else {
                builder.append(" ON DELETE NO ACTION ");
            }
        }

        return builder.toString();
    }
}
