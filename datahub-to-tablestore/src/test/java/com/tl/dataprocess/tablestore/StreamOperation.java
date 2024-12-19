package com.tl.dataprocess.tablestore;

import com.alicloud.openservices.tablestore.SyncClient;

public class StreamOperation {

    private static final String TABLE_NAME = "sampleTable_pk";
    private static final String PRIMARY_KEY_NAME_1 = "pk1";
    private static final String PRIMARY_KEY_NAME_2 = "pk2";

    public static void main(String[] args) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        SyncClient client = new SyncClient(endPoint, accessId, accessKey,
                instanceName);


    }
}
