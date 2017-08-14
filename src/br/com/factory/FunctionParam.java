/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.factory;

import java.util.Arrays;
import java.util.List;

/**
 *
 * @author giovanimoura
 */
public class FunctionParam {
    
    private String nomeFuncao = "";
    private List listParam = null;
    
    public FunctionParam(String nomeFuncao, Object... param) {
        this.nomeFuncao = nomeFuncao;
        listParam = Arrays.asList(param);
    }
    
    public List getListParametros() {
        return listParam;
    }
    
    /**
     * Giovani Moura - 15/05/2012 Retorna script para chamar a função, incluindo
     * os parametros de entrada da função
     * @return
     */
    public String getSqlFuncao() {
        return "SELECT * FROM " + nomeFuncao;// + "(" + getRowFuncao(null, null) + ")";
    }
    
    public String getSqlVoid() {
        return "call " + nomeFuncao;// + "(" + getRowFuncao(null, null) + ")";
    }
    
    
}
