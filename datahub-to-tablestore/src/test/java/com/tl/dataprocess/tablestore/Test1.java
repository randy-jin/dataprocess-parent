package com.tl.dataprocess.tablestore;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangchunhuai on 2021/10/25.
 */
public class Test1 {
    public static void main(String[] args) {
        String a="002";

        List<String> trueList=new ArrayList<>();
        for(char c:a.toCharArray()){
            if(c=='2'){
                trueList.add("0");
                trueList.add("1");
            }else if(c=='0'){
                trueList.add("0");
                trueList.add("0");
            }else{
                trueList.add("1");
                trueList.add("1");
            }
        }

        System.out.println(trueList.toString());

        String setZone = trueList.get(0);
        String setSec = trueList.get(2);
        String setPrc = trueList.get(4);
        String setZoneTime = trueList.get(1);
        String setSecTime = trueList.get(3);
        String setPrcTime = trueList.get(5);

        int zone=Integer.valueOf(setZone)+Integer.valueOf(setZoneTime);
        int sec=Integer.valueOf(setSec)+Integer.valueOf(setSecTime);
        int prc=Integer.valueOf(setPrc)+Integer.valueOf(setPrcTime);

        String execMode=zone+""+sec+""+prc;

        System.out.println(execMode);
        if(execMode.contains("2")){
            System.out.println(0);
        }else{
            int index=execMode.indexOf("1")+1;
            System.out.println(index+6);
        }

    }
}
