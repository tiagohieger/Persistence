package br.com.utils;

import java.text.Normalizer;

public class StringUtils {

    /**
     * Realiza este comando: Normalizer.normalize(valor,
     * Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
     *
     * @param valor
     * @return
     */
    //--------------------------------------------------------------------------
    public static String delCaracteresEspecias(final String valor) {
        if (isValorNuloOuVazio(valor)) {
            return valor;
        }
        return Normalizer.normalize(valor, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
    }

    /**
     *
     * @param string
     * @return
     */
    //--------------------------------------------------------------------------
    public static Boolean isValorNuloOuVazio(final String string) {

        return (string == null || string.trim().isEmpty());
    }

}
