/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.constants;

/**
 *Classe responsavel para implementar os tipos criados para categorias de tipos distintos para gravação
 *
 * deve se adicinor o construtor para adicionar a descrição do campos
 *
 * public constructor(String description){
 *   super(description);
 * }
 *
 *
 * @author Alecsandro
 */
public interface IGenericType {

    /**
     * Metodo declarado na interface para poder pegar a descrição passada para um Enumtype
     * @return
     */
    public String getDescription();

    /**
     * Retorna um array com as descrições do Enum
     * @return
     */
    public String[] getAllDescriptions();
}
