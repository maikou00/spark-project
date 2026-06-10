package com.sziov.gacnev.datasource.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 数据源注册中心，管理数据源类型的注册与实例创建。
 *
 * @author maikou
 * @since 2026-06-10
 */
public final class DataSourceRegistry {

    private static final Map<DataSourceType, RegistryEntry<?>> REGISTRY = new ConcurrentHashMap<>();

    private DataSourceRegistry() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    /**
     * 注册数据源类型。
     *
     * @param type         数据源类型
     * @param configClass  配置类
     * @param sourceSupplier Source 实例创建工厂
     * @param sinkSupplier   Sink 实例创建工厂
     * @param <C>           配置类型
     */
    public static <C> void register(DataSourceType type,
                                     Class<C> configClass,
                                     Supplier<DataSource<C>> sourceSupplier,
                                     Supplier<DataSink<C>> sinkSupplier) {
        if (REGISTRY.containsKey(type)) {
            throw new IllegalStateException("数据源类型已注册: " + type);
        }
        REGISTRY.put(type, new RegistryEntry<>(configClass, sourceSupplier, sinkSupplier));
    }

    /**
     * 根据类型创建 Source 实例。
     *
     * @param type 数据源类型
     * @param <C>  配置类型
     * @return Source 实例
     */
    @SuppressWarnings("unchecked")
    public static <C> DataSource<C> createSource(DataSourceType type) {
        RegistryEntry<?> entry = REGISTRY.get(type);
        if (entry == null) {
            throw new IllegalArgumentException("未注册的数据源类型: " + type);
        }
        return (DataSource<C>) entry.sourceSupplier.get();
    }

    /**
     * 根据类型创建 Sink 实例。
     *
     * @param type 数据源类型
     * @param <C>  配置类型
     * @return Sink 实例
     */
    @SuppressWarnings("unchecked")
    public static <C> DataSink<C> createSink(DataSourceType type) {
        RegistryEntry<?> entry = REGISTRY.get(type);
        if (entry == null) {
            throw new IllegalArgumentException("未注册的数据源类型: " + type);
        }
        return (DataSink<C>) entry.sinkSupplier.get();
    }

    private static final class RegistryEntry<C> {
        final Class<C> configClass;
        final Supplier<DataSource<C>> sourceSupplier;
        final Supplier<DataSink<C>> sinkSupplier;

        RegistryEntry(Class<C> configClass,
                      Supplier<DataSource<C>> sourceSupplier,
                      Supplier<DataSink<C>> sinkSupplier) {
            this.configClass = configClass;
            this.sourceSupplier = sourceSupplier;
            this.sinkSupplier = sinkSupplier;
        }
    }
}
