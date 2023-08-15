package com.summer.kvstore.model.command;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * 抽象命令
 */
@Setter
@Getter
public abstract class AbstractCommand implements Command, Serializable {

    /**
     * 命令类型
     */
    private CommandTypeEnum type;

    public AbstractCommand(CommandTypeEnum type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
