package com.sziov.gacnev.datasource.option;

/**
 * 数据源 Option 标记接口，约束 DataSourceApi 泛型上界，
 * 并提供 resource 的 getter/setter 契约。
 *
 * @param <O> 实现类自身类型
 * @author maikou
 * @since 2026-06-11
 */
public interface DataSourceOption<O extends DataSourceOption<O>> {

    String getResource();

    O setResource(String resource);
}
