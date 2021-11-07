/*
 * Copyright (c) 2015-2021, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command;

import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.command.bitset.BitCountCommand;
import com.github.tonivade.claudb.command.bitset.GetBitCommand;
import com.github.tonivade.claudb.command.bitset.SetBitCommand;
import com.github.tonivade.claudb.command.hash.*;
import com.github.tonivade.claudb.command.key.*;
import com.github.tonivade.claudb.command.list.*;
import com.github.tonivade.claudb.command.pubsub.*;
import com.github.tonivade.claudb.command.scripting.EvalCommand;
import com.github.tonivade.claudb.command.scripting.EvalShaCommand;
import com.github.tonivade.claudb.command.scripting.ScriptCommands;
import com.github.tonivade.claudb.command.server.*;
import com.github.tonivade.claudb.command.set.*;
import com.github.tonivade.claudb.command.string.*;
import com.github.tonivade.claudb.command.transaction.DiscardCommand;
import com.github.tonivade.claudb.command.transaction.ExecCommand;
import com.github.tonivade.claudb.command.transaction.MultiCommand;
import com.github.tonivade.claudb.command.zset.*;
import com.github.tonivade.resp.command.CommandSuite;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * @author zhou <br/>
 * <p>
 * 数据库命令套件
 */
public class DBCommandSuite extends CommandSuite {

    /**
     * 命令黑名单
     */
    private static final Set<String> COMMAND_BLACK_LIST = new HashSet<>(asList("ping", "echo", "quit", "time"));

    /**
     * 注册数据库命令
     */
    public DBCommandSuite() {
        super(new DBCommandWrapperFactory());

        // connection
        addCommand(SelectCommand::new);
        addCommand(SyncCommand::new);
        addCommand(SlaveOfCommand::new);

        // server
        addCommand(FlushDBCommand::new);
        addCommand(InfoCommand::new);
        addCommand(RoleCommand::new);
        addCommand(DatabaseSizeCommand::new);

        // strings
        addCommand(GetCommand::new);
        addCommand(MultiGetCommand::new);
        addCommand(SetCommand::new);
        addCommand(MultiSetCommand::new);
        addCommand(GetSetCommand::new);
        addCommand(IncrementCommand::new);
        addCommand(IncrementByCommand::new);
        addCommand(DecrementCommand::new);
        addCommand(DecrementByCommand::new);
        addCommand(StringLengthCommand::new);
        addCommand(SetExpiredCommand::new);
        addCommand(BitCountCommand::new);
        addCommand(SetBitCommand::new);
        addCommand(GetBitCommand::new);
        addCommand(SetIfNotExistsCommand::new);
        addCommand(MultiSetIfNotExistsCommand::new);

        // keys
        addCommand(DeleteCommand::new);
        addCommand(ExistsCommand::new);
        addCommand(TypeCommand::new);
        addCommand(RenameCommand::new);
        addCommand(KeysCommand::new);
        addCommand(ExpireCommand::new);
        addCommand(PersistCommand::new);
        addCommand(TimeToLiveMillisCommand::new);
        addCommand(TimeToLiveSecondsCommand::new);

        // hash
        addCommand(HashSetCommand::new);
        addCommand(HashGetCommand::new);
        addCommand(HashGetAllCommand::new);
        addCommand(HashExistsCommand::new);
        addCommand(HashDeleteCommand::new);
        addCommand(HashKeysCommand::new);
        addCommand(HashLengthCommand::new);
        addCommand(HashMultiGetCommand::new);
        addCommand(HashMultiSetCommand::new);
        addCommand(HashValuesCommand::new);

        // list
        addCommand(LeftPushCommand::new);
        addCommand(LeftPopCommand::new);
        addCommand(RightPushCommand::new);
        addCommand(RightPopCommand::new);
        addCommand(ListLengthCommand::new);
        addCommand(ListRangeCommand::new);
        addCommand(ListIndexCommand::new);
        addCommand(ListSetCommand::new);

        // set
        addCommand(SetAddCommand::new);
        addCommand(SetMembersCommand::new);
        addCommand(SetCardinalityCommand::new);
        addCommand(SetIsMemberCommand::new);
        addCommand(SetRemoveCommand::new);
        addCommand(SetUnionCommand::new);
        addCommand(SetIntersectionCommand::new);
        addCommand(SetDifferenceCommand::new);

        // sorted set
        addCommand(SortedSetAddCommand::new);
        addCommand(SortedSetCardinalityCommand::new);
        addCommand(SortedSetRemoveCommand::new);
        addCommand(SortedSetRangeCommand::new);
        addCommand(SortedSetRangeByScoreCommand::new);
        addCommand(SortedSetReverseRangeCommand::new);
        addCommand(SortedSetIncrementByCommand::new);

        // pub & sub
        addCommand(PublishCommand::new);
        addCommand(SubscribeCommand::new);
        addCommand(UnsubscribeCommand::new);
        addCommand(PatternSubscribeCommand::new);
        addCommand(PatternUnsubscribeCommand::new);

        // transactions
        addCommand(MultiCommand::new);
        addCommand(ExecCommand::new);
        addCommand(DiscardCommand::new);

        // scripting
        addCommand(EvalCommand::new);
        addCommand(EvalShaCommand::new);
        addCommand(ScriptCommands::new);
    }

    /**
     * 是否为只读状态
     *
     * @param command
     * @return
     */
    public boolean isReadOnly(String command) {
        return COMMAND_BLACK_LIST.contains(command) || isPresent(command, ReadOnly.class);
    }
}
