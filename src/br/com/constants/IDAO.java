package br.com.constants;

import java.util.List;

public interface IDAO<T> {

    public void insert(T entity) throws Exception;

    public int update(T entity) throws Exception;

    public T save(T entity) throws Exception;

    public void remove(int id) throws Exception;

    public void removeAll() throws Exception;

    public void truncate() throws Exception;

    public T getEntity(int id, boolean isdepenteces) throws Exception;

    public T getEntityWhere(String where, boolean isdepenteces) throws Exception;

    public List<T> listEntity(boolean orderbyId, boolean isdepenteces) throws Exception;

    public List<T> listEntityWhere(String where, boolean isdepenteces) throws Exception;

    public void deleteByParams(Object[]... params) throws Exception;
}
