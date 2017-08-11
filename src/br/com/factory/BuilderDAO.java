package br.com.factory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class BuilderDAO {

    private final Connection conexao;

    public BuilderDAO(Connection conexao) {
        this.conexao = conexao;
    }

    public List<String> buscaCamposTabela(final String nomeTabela) throws SQLException, ClassNotFoundException {
        List<String> listCampos = new ArrayList<>();
        Query query = new Query(conexao);
        query.addSql("SELECT * FROM " + nomeTabela + " LIMIT 0;");
        query.open();
        for (int i = 1; i <= query.getColumnCount(); i++) {
            final String nomeCampo = (String) query.getColumnLabel(i);
            listCampos.add(nomeCampo);
        }
        return listCampos;
    }

    public void executaSQL(final String sql) throws SQLException, ClassNotFoundException {
        final Query query = new Query(conexao);
        query.addSql(sql);
        query.execute();
    }

    public void deletaView(String nomeView) throws SQLException, ClassNotFoundException {
        Query query = new Query(conexao);
        query.addSql("DROP VIEW IF EXISTS " + nomeView + " CASCADE;");
        query.execute();
    }

    public List<String> listaTodasViews() throws SQLException, ClassNotFoundException {
        List<String> views = new ArrayList<>();
        Query query = new Query(conexao);
        query.addSql("SELECT table_name FROM INFORMATION_SCHEMA.views WHERE table_schema = ANY (current_schemas(false));");
        query.open();
        while (query.next()) {
            views.add(String.valueOf(query.getValue("table_name")));
        }
        return views;
    }

    public List<String> listaDropFuncoes() throws SQLException, ClassNotFoundException {
        List<String> dropFuncoes = new ArrayList<>();
        Query query = new Query(conexao);
        query.addSql("SELECT 'DROP ' ");
        query.addSql(" || CASE WHEN p.proisagg THEN 'AGGREGATE ' ELSE 'FUNCTION ' END ");
        query.addSql(" ||  quote_ident(p.proname) || '(' ");
        query.addSql(" || pg_catalog.pg_get_function_identity_arguments(p.oid) || ') cascade;' AS funcao ");
        query.addSql(" FROM   pg_catalog.pg_proc p ");
        query.addSql(" JOIN   pg_catalog.pg_namespace n ON n.oid = p.pronamespace ");
        query.addSql(" WHERE  n.nspname = 'public' ");
        query.addSql(" ORDER  BY 1; ");
        query.open();
        while (query.next()) {
            dropFuncoes.add(String.valueOf(query.getValue("funcao")));
        }
        return dropFuncoes;
    }

}
