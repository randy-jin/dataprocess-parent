package com.tl.dataprocess.utils;

import com.tl.datahub.DataHubShardCache;
import com.tl.datahub.DataHubTopic;

import java.io.*;
import java.util.List;

/**
 * Created by huangchunhuai on 2021/11/26.
 */
public class CommonMethod {

    /**
     * 添加到datahub
     *
     * @param businessDataitemId
     * @param indexKey
     * @param dataListFinal
     * @param refreshKey
     * @param listDataObj
     * @throws Exception
     */
    public static void putToDataHub(String businessDataitemId, String indexKey, List<Object> dataListFinal, Object[] refreshKey, List<DataObject> listDataObj, DataHubShardCache dataHubShardCache) throws Exception {
        DataHubTopic dataHubTopic = new DataHubTopic(businessDataitemId,dataHubShardCache);
        int index = Math.abs(indexKey.hashCode()) % dataHubTopic.getShardCount();
        String shardId = dataHubTopic.getActiveShardList().get(index);
        DataObject dataObj = new DataObject(dataListFinal, refreshKey, dataHubTopic.topic(), shardId);//给这个数据类赋值:list key classname (fn改为161)
        listDataObj.add(dataObj);//将组好的数据类添加到数据列表
    }


    /***
     * 方法一对集合进行深拷贝 注意需要对泛型类进行序列化(实现Serializable)
     *
     * @param srcList
     * @param <T>
     * @return
     */
    public static <T> List<T> depCopy(List<T> srcList) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try {
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeObject(srcList);

            ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
            ObjectInputStream inStream = new ObjectInputStream(byteIn);
            List<T> destList = (List<T>) inStream.readObject();
            return destList;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

}
