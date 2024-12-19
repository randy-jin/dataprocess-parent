package com.tl.hades.persist;

import com.ls.athena.message.redis.utils.BytesCoverUtil;
import com.zzuli.taskcdw.serialize.MineSerialize;


public class DefaultSeri implements MineSerialize {

    @Override
    public byte[] serialize(Object obj) throws Exception {
        return BytesCoverUtil.converToDataBySelf(obj);
    }

    @Override
    public Object deSerialize(byte[] data) throws Exception {
        return BytesCoverUtil.coverToValueBySelf(data);
    }
}
