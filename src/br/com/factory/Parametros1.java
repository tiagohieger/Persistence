/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.factory;

/**
 *
 * @author Alecsandro
 */
public class Parametros1 {

    private Integer index;
    private Object valor;

    public Parametros1(Integer index, Object valor) {
        this.index = index;
        this.valor = valor;
    }
            
    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public Object getValor() {
        return valor;
    }

    public void setValor(Object valor) {
        this.valor = valor;
    }
}
