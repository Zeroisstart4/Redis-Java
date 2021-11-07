/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb;

import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseFactory;
import com.github.tonivade.claudb.data.DatabaseKey;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.claudb.persistence.RDBInputStream;
import com.github.tonivade.claudb.persistence.RDBOutputStream;
import com.github.tonivade.purefun.data.ImmutableList;
import com.github.tonivade.purefun.data.ImmutableMap;
import com.github.tonivade.purefun.data.ImmutableSet;
import com.github.tonivade.purefun.type.Option;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.*;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.*;
import static com.github.tonivade.resp.protocol.SafeString.safeString;


/**
 * @author zhou
 * <p>
 * 数据库服务器状态
 */
public class DBServerState {

    /**
     * RDB 版本
     */
    private static final int RDB_VERSION = 6;
    /**
     * Redis slaves 命令
     */
    private static final SafeString SLAVES = safeString("slaves");
    /**
     * slaves 命令键
     */
    private static final DatabaseKey SLAVES_KEY = safeKey("slaves");
    /**
     * 脚本键
     */
    private static final DatabaseKey SCRIPTS_KEY = safeKey("scripts");
    /**
     * 设为主库
     */
    private boolean master = true;
    /**
     * 数据库列表
     */
    private final List<Database> databases = new ArrayList<>();
    /**
     * 当前数据库
     */
    private final Database admin;
    /**
     * 数据库工厂
     */
    private final DatabaseFactory factory;
    /**
     * 命令队列
     */
    private final Queue<RedisToken> queue = new LinkedList<>();

    public DBServerState(DatabaseFactory factory, int numDatabases) {
        this.factory = factory;
        this.admin = factory.create("admin");
        for (int i = 0; i < numDatabases; i++) {
            this.databases.add(factory.create("db-" + i));
        }
    }

    public void append(RedisToken command) {
        queue.offer(command);
    }

    public void setMaster(boolean master) {
        this.master = master;
    }

    public boolean isMaster() {
        return master;
    }

    public Database getAdminDatabase() {
        return admin;
    }

    public Database getDatabase(int id) {
        return databases.get(id);
    }

    public void clear() {
        databases.clear();
        factory.clear();
    }

    public boolean hasSlaves() {
        return !admin.getSet(SLAVES).isEmpty();
    }

    public void exportRDB(OutputStream output) throws IOException {
        RDBOutputStream rdb = new RDBOutputStream(output);
        rdb.preamble(RDB_VERSION);
        for (int i = 0; i < databases.size(); i++) {
            Database db = databases.get(i);
            if (!db.isEmpty()) {
                rdb.select(i);
                rdb.dabatase(db);
            }
        }
        rdb.end();
    }

    public void importRDB(InputStream input) throws IOException {
        RDBInputStream rdb = new RDBInputStream(input);

        Map<Integer, Map<DatabaseKey, DatabaseValue>> load = rdb.parse();
        for (Map.Entry<Integer, Map<DatabaseKey, DatabaseValue>> entry : load.entrySet()) {
            databases.get(entry.getKey()).overrideAll(ImmutableMap.from(entry.getValue()));
        }
    }

    public void saveScript(SafeString sha1, SafeString script) {
        DatabaseValue value = hash(entry(sha1, script));
        admin.merge(SCRIPTS_KEY, value, (oldValue, newValue) -> {
            Map<SafeString, SafeString> merge = new HashMap<>();
            merge.putAll(oldValue.getHash().toMap());
            merge.putAll(newValue.getHash().toMap());
            return hash(ImmutableMap.from(merge));
        });
    }

    public Option<SafeString> getScript(SafeString sha1) {
        DatabaseValue value = admin.getOrDefault(SCRIPTS_KEY, EMPTY_HASH);
        return value.getHash().get(sha1);
    }

    public void cleanScripts() {
        admin.remove(SCRIPTS_KEY);
    }

    public ImmutableSet<SafeString> getSlaves() {
        return getAdminDatabase().getSet(SLAVES);
    }

    public void addSlave(String id) {
        getAdminDatabase().merge(SLAVES_KEY, set(safeString(id)),
                (oldValue, newValue) -> set(oldValue.getSet().appendAll(newValue.getSet())));
    }

    public void removeSlave(String id) {
        getAdminDatabase().merge(SLAVES_KEY, set(safeString(id)),
                (oldValue, newValue) -> set(oldValue.getSet().difference(newValue.getSet())));
    }

    public ImmutableList<RedisToken> getCommandsToReplicate() {
        ImmutableList<RedisToken> list = ImmutableList.from(queue);
        queue.clear();
        return list;
    }

    public void evictExpired(Instant now) {
        for (Database database : databases) {
            database.evictableKeys(now).forEach(database::remove);
        }
    }
}
