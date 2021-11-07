/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb;


/**
 * @author zhou <br/>
 * <p>
 * 数据库配置类
 */
public class DBConfig {

    /**
     * 默认同步时间
     */
    private static final int DEFAULT_SYNC_PERIOD = 60;
    /**
     * 默认清理时间
     */
    private static final int DEFAULT_CLEAN_PERIOD = 30;
    /**
     * 默认数据库个数
     */
    private static final int DEFAULT_DATABASES = 10;
    /**
     * RDB 持久化文件
     */
    private static final String DUMP_FILE = "dump.rdb";
    /**
     * AOF 持久化文件
     */
    private static final String REDO_FILE = "redo.aof";
    /**
     * 数据库个数
     */
    private int numDatabases = DEFAULT_DATABASES;
    /**
     * 是否开启持久化
     */
    private boolean persistenceActive;
    /**
     * 是否开启通知
     */
    private boolean notificationsActive;
    /**
     * 堆外空间激活
     */
    private boolean offHeapActive;
    /**
     * RDF 持久化文件
     */
    private String rdbFile;
    /**
     * AOF 持久化文件
     */
    private String aofFile;
    /**
     * 同步时间
     */
    private int syncPeriod = DEFAULT_SYNC_PERIOD;
    /**
     * 清理时间
     */
    private int cleanPeriod = DEFAULT_CLEAN_PERIOD;

    public boolean isPersistenceActive() {
        return persistenceActive;
    }

    public void setPersistenceActive(boolean persistenceActive) {
        this.persistenceActive = persistenceActive;
    }

    public boolean isNotificationsActive() {
        return notificationsActive;
    }

    public void setNotificationsActive(boolean notificationsActive) {
        this.notificationsActive = notificationsActive;
    }

    public boolean isOffHeapActive() {
        return offHeapActive;
    }

    public void setOffHeapActive(boolean offHeapActive) {
        this.offHeapActive = offHeapActive;
    }

    public String getRdbFile() {
        return rdbFile;
    }

    public void setRdbFile(String rdbFile) {
        this.rdbFile = rdbFile;
    }

    public String getAofFile() {
        return aofFile;
    }

    public void setAofFile(String aofFile) {
        this.aofFile = aofFile;
    }

    public int getSyncPeriod() {
        return syncPeriod;
    }

    public void setSyncPeriod(int syncPeriod) {
        this.syncPeriod = syncPeriod;
    }

    public int getNumDatabases() {
        return numDatabases;
    }

    public void setNumDatabases(int numDatabases) {
        this.numDatabases = numDatabases;
    }

    public long getCleanPeriod() {
        return this.cleanPeriod;
    }

    public void setCleanPeriod(int cleanPeriod) {
        this.cleanPeriod = cleanPeriod;
    }

    public static Builder builder() {
        return new Builder();
    }


    /**
     * 配置类构建器
     */
    public static class Builder {

        private final DBConfig config = new DBConfig();

        /**
         * 关闭持久化
         *
         * @return
         */
        public Builder withoutPersistence() {
            config.setPersistenceActive(false);
            return this;
        }

        /**
         * 开启持久化
         *
         * @return
         */
        public Builder withPersistence() {
            config.setPersistenceActive(true);
            config.setRdbFile(DUMP_FILE);
            config.setAofFile(REDO_FILE);
            return this;
        }

        /**
         * 关闭堆外空间
         *
         * @return
         */
        public Builder withOffHeapCache() {
            config.setOffHeapActive(true);
            return this;
        }

        /**
         * 开启事件通知
         *
         * @return
         */
        public Builder withNotifications() {
            config.setNotificationsActive(true);
            return this;
        }

        public DBConfig build() {
            return config;
        }
    }
}
