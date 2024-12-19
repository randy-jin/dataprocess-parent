package com.tl.hades.persist;

import com.tl.easb.utils.DateUtil;

import java.util.*;

public class VoluntrayDataUtils {

    public static List FN_LIST = new ArrayList();
    private static Integer[] curveArray = new Integer[]{Integer.valueOf(92), Integer.valueOf(93), Integer.valueOf(94), Integer.valueOf(95), Integer.valueOf(89), Integer.valueOf(90), Integer.valueOf(91), Integer.valueOf(73), Integer.valueOf(74), Integer.valueOf(75), Integer.valueOf(76), Integer.valueOf(138), Integer.valueOf(105), Integer.valueOf(106), Integer.valueOf(107), Integer.valueOf(108), Integer.valueOf(81), Integer.valueOf(82), Integer.valueOf(83), Integer.valueOf(84), Integer.valueOf(85), Integer.valueOf(86), Integer.valueOf(87), Integer.valueOf(88), Integer.valueOf(101), Integer.valueOf(102), Integer.valueOf(103), Integer.valueOf(104), Integer.valueOf(145), Integer.valueOf(146), Integer.valueOf(147), Integer.valueOf(148), Integer.valueOf(97), Integer.valueOf(98), Integer.valueOf(99), Integer.valueOf(100)};
    public static List<Integer> curveList = new ArrayList<Integer>(){{addAll(Arrays.asList(curveArray));}};


    public static boolean allNull(List list) {
        boolean allNull = true;
        Iterator var2 = list.iterator();

        while(var2.hasNext()) {
            Object o = var2.next();
            if(o instanceof List) {
                allNull((List)o);
            } else if(o != null && !(o instanceof Date) && !(o instanceof Integer)) {
                allNull = false;
                return allNull;
            }
        }

        return allNull;
    }

    public static List getDataList(List dataList, int afn, int fn, String mpedId) {
        ArrayList newDataList = null;
        if(afn == 13) {
            Object var15;
            Object var18;
            int var19;
            int var21;
            List var29;
            if(fn >= 161 && fn <= 168) {
                newDataList = new ArrayList();
                newDataList.add(Integer.valueOf(1));
                if(fn == 161) {
                    newDataList.add("1");
                } else if(fn == 162) {
                    newDataList.add("2");
                } else if(fn == 165) {
                    newDataList.add("3");
                } else if(fn == 168) {
                    newDataList.add("4");
                } else if(fn == 163) {
                    newDataList.add("5");
                } else if(fn == 164) {
                    newDataList.add("6");
                } else if(fn == 166) {
                    newDataList.add("7");
                } else if(fn == 167) {
                    newDataList.add("8");
                } else {
                    newDataList.add("0");
                }

                var19 = ((Integer)dataList.remove(2)).intValue();
                var29 = (List)((List)dataList.get(3)).get(0);
                var15 = dataList.get(1);
                if(null != var15 && var15 instanceof Date) {
                    newDataList.add(var15);
                } else {
                    newDataList.add((Object)null);
                }

                newDataList.add(dataList.get(2));
                newDataList.addAll(var29);

                for(var21 = var19; var21 < 14; ++var21) {
                    newDataList.add((Object)null);
                }

                newDataList.add(1, mpedId);
                var18 = dataList.get(0);
                if(null == var18 || !(var18 instanceof Date)) {
                    return null;
                }

                Date var26 = (Date)var18;
                newDataList.add(DateUtil.format(var26, "yyyyMMdd"));
                if(DateFilter.isLateWeek(var26, Integer.valueOf(-45))) {
                    return null;
                }

                newDataList.set(0, mpedId + "_" + newDataList.get(newDataList.size() - 1));
            } else {
                Object date;
                int var16;
                if(fn >= 5 && fn <= 8) {
                    newDataList = new ArrayList();
                    newDataList.add(Integer.valueOf(1));
                    if(fn == 5) {
                        newDataList.add("1");
                    } else if(fn == 6) {
                        newDataList.add("2");
                    } else if(fn == 7) {
                        newDataList.add("5");
                    } else if(fn == 8) {
                        newDataList.add("6");
                    }

                    date = dataList.get(0);
                    var16 = ((Integer)dataList.remove(1)).intValue();
                    List var37 = (List)((List)dataList.get(2)).get(0);
                    newDataList.add(date);
                    newDataList.add(dataList.get(1));
                    newDataList.addAll(var37);

                    for(var21 = 0; var21 < 4 - var37.size(); ++var21) {
                        newDataList.add((Object)null);
                    }

                    for(var21 = var16; var21 < 14; ++var21) {
                        newDataList.add((Object)null);
                    }

                    newDataList.set(0, mpedId);
                    if(null != date && date instanceof Date) {
                        newDataList.add(DateUtil.format((Date)date, "yyyyMMdd"));
                    }
                } else {
                    Calendar num;
                    int dl;
                    if((fn < 185 || fn > 188) && (fn < 193 || fn > 196)) {
                        if(fn == 43) {
                            newDataList = new ArrayList();
                            newDataList.add(mpedId);
                            newDataList.add(dataList.get(1));
                            newDataList.add(dataList.get(2));
                            newDataList.add(dataList.get(3));
                            date = dataList.get(0);
                            if(null != date && date instanceof Date) {
                                newDataList.add(DateUtil.format((Date)date, "yyyyMMdd"));
                            }
                        } else {
                            String var20;
                            ArrayList var34;
                            if(fn == 19 || fn == 20 || fn == 4 || fn == 3) {
                                var34 = new ArrayList();
                                byte var31 = 4;
                                byte var36 = 6;
                                var20 = "1";
                                if(fn == 20 || fn == 4) {
                                    var20 = "5";
                                }

                                for(dl = 0; dl < 2; ++dl) {
                                    newDataList = new ArrayList();
                                    Date var27 = (Date)dataList.get(0);
                                    newDataList.add(mpedId);
                                    newDataList.add(var20);
                                    newDataList.add(var27);
                                    newDataList.add(dataList.get(var31 - 1));
                                    newDataList.add(dataList.get(var36 - 1));
                                    List var28 = (List)((List)dataList.get(var31)).get(0);
                                    List var33 = (List)((List)dataList.get(var36)).get(0);

                                    for(int i1 = 0; i1 < var28.size(); ++i1) {
                                        newDataList.add(var28.get(i1));
                                        newDataList.add(var33.get(i1));
                                    }

                                    newDataList.add(DateUtil.format(var27, "yyyyMMdd"));
                                    var31 = 8;
                                    var36 = 10;
                                    var20 = "2";
                                    if(fn == 20 || fn == 4) {
                                        var20 = "6";
                                    }

                                    var34.add(newDataList);
                                }

                                return var34;
                            }

                            Calendar var14;
                            if(fn == 25) {
                                newDataList = new ArrayList();
                                newDataList.add(mpedId);
                                var14 = Calendar.getInstance();
                                num = Calendar.getInstance();
                                num.setTime((Date)dataList.get(0));
                                newDataList.add(dataList.get(1));
                                var15 = dataList.get(2);
                                Date var17;
                                if(var15 != null) {
                                    var14.setTime((Date)var15);
                                    var14.set(1, num.get(1));
                                    var14.set(2, num.get(2));
                                    var17 = var14.getTime();
                                } else {
                                    var17 = null;
                                }

                                newDataList.add(var17);
                                newDataList.add(dataList.get(3));
                                var15 = dataList.get(4);
                                if(var15 != null) {
                                    var14.setTime((Date)var15);
                                    var14.set(1, num.get(1));
                                    var14.set(2, num.get(2));
                                    var17 = var14.getTime();
                                } else {
                                    var17 = null;
                                }

                                newDataList.add(var17);
                                newDataList.add(dataList.get(5));
                                var15 = dataList.get(6);
                                if(var15 != null) {
                                    var14.setTime((Date)var15);
                                    var14.set(1, num.get(1));
                                    var14.set(2, num.get(2));
                                    var17 = var14.getTime();
                                } else {
                                    var17 = null;
                                }

                                newDataList.add(var17);
                                newDataList.add(dataList.get(7));
                                var15 = dataList.get(8);
                                if(var15 != null) {
                                    var14.setTime((Date)var15);
                                    var14.set(1, num.get(1));
                                    var14.set(2, num.get(2));
                                    var17 = var14.getTime();
                                } else {
                                    var17 = null;
                                }

                                newDataList.add(var17);
                                var18 = dataList.get(0);
                                if(null != var18 && var18 instanceof Date) {
                                    newDataList.add(DateUtil.format((Date)var18, "yyyyMMdd"));
                                }
                            } else if(fn == 29) {
                                newDataList = new ArrayList();
                                newDataList.add(mpedId);
                                newDataList.add(dataList.get(1));
                                newDataList.add(dataList.get(2));
                                newDataList.add(dataList.get(3));
                                newDataList.add(dataList.get(4));
                                newDataList.add(dataList.get(5));
                                newDataList.add(dataList.get(6));
                                newDataList.add(dataList.get(7));
                                newDataList.add(dataList.get(8));
                                var14 = Calendar.getInstance();
                                num = Calendar.getInstance();
                                num.setTime((Date)dataList.get(0));
                                new Object();
                                var15 = dataList.get(9);
                                if(var15 != null) {
                                    var14.setTime((Date)var15);
                                    var14.set(1, num.get(1));
                                    var14.set(2, num.get(2));
                                    var15 = var14.getTime();
                                }

                                newDataList.add(var15);
                                newDataList.add(dataList.get(10));
                                var15 = dataList.get(11);
                                if(var15 != null) {
                                    var14.setTime((Date)var15);
                                    var14.set(1, num.get(1));
                                    var14.set(2, num.get(2));
                                    var15 = var14.getTime();
                                }

                                newDataList.add(var15);
                                newDataList.add(dataList.get(12));
                                var15 = dataList.get(13);
                                if(var15 != null) {
                                    var14.setTime((Date)var15);
                                    var14.set(1, num.get(1));
                                    var14.set(2, num.get(2));
                                    var15 = var14.getTime();
                                }

                                newDataList.add(var15);
                                newDataList.add(dataList.get(14));
                                var15 = dataList.get(15);
                                if(var15 != null) {
                                    var14.setTime((Date)var15);
                                    var14.set(1, num.get(1));
                                    var14.set(2, num.get(2));
                                    var15 = var14.getTime();
                                }

                                newDataList.add(var15);
                                var18 = dataList.get(0);
                                if(null != var18 && var18 instanceof Date) {
                                    newDataList.add(DateUtil.format((Date)var18, "yyyyMMdd"));
                                }
                            } else if(fn >= 177 && fn <= 184) {
                                newDataList = new ArrayList();
                                newDataList.add(Integer.valueOf(1));
                                if(fn == 177) {
                                    newDataList.add("1");
                                } else if(fn == 178) {
                                    newDataList.add("2");
                                } else if(fn == 181) {
                                    newDataList.add("3");
                                } else if(fn == 184) {
                                    newDataList.add("4");
                                } else if(fn == 179) {
                                    newDataList.add("5");
                                } else if(fn == 180) {
                                    newDataList.add("6");
                                } else if(fn == 182) {
                                    newDataList.add("7");
                                } else if(fn == 183) {
                                    newDataList.add("8");
                                } else {
                                    newDataList.add("0");
                                }

                                var19 = ((Integer)dataList.remove(2)).intValue();
                                var29 = (List)((List)dataList.get(3)).get(0);
                                var15 = dataList.get(1);
                                if(null != var15 && var15 instanceof Date) {
                                    newDataList.add(var15);
                                } else {
                                    newDataList.add((Object)null);
                                }

                                newDataList.add(dataList.get(2));
                                newDataList.addAll(var29);

                                for(var21 = var19; var21 < 14; ++var21) {
                                    newDataList.add((Object)null);
                                }

                                newDataList.add(1, mpedId);
                                var18 = dataList.get(0);
                                if(null != var18 && var18 instanceof Date) {
                                    newDataList.add(DateUtil.format((Date)var18, "yyyyMMdd"));
                                }

                                newDataList.set(0, mpedId + "_" + newDataList.get(newDataList.size() - 1));
                            } else {
                                int var35;
                                if(fn >= 21 && fn <= 24) {
                                    newDataList = new ArrayList();
                                    newDataList.add(Integer.valueOf(1));
                                    if(fn == 21) {
                                        newDataList.add("1");
                                    } else if(fn == 22) {
                                        newDataList.add("2");
                                    } else if(fn == 23) {
                                        newDataList.add("5");
                                    } else if(fn == 24) {
                                        newDataList.add("6");
                                    } else {
                                        newDataList.add("0");
                                    }

                                    var19 = ((Integer)dataList.remove(1)).intValue();
                                    var29 = (List)((List)dataList.get(2)).get(0);
                                    newDataList.add((Object)null);
                                    newDataList.add(dataList.get(1));
                                    if(var29.size() < 4) {
                                        newDataList.addAll(var29);

                                        for(var35 = 0; var35 < 4 - var29.size(); ++var35) {
                                            newDataList.add((Object)null);
                                        }
                                    }

                                    for(var35 = var19; var35 < 14; ++var35) {
                                        newDataList.add((Object)null);
                                    }

                                    newDataList.set(0, mpedId);
                                    var15 = dataList.get(0);
                                    if(null != var15 && var15 instanceof Date) {
                                        newDataList.add(DateUtil.format((Date)var15, "yyyyMMdd"));
                                    }
                                } else if(fn == 44) {
                                    newDataList = new ArrayList();
                                    newDataList.add(mpedId);
                                    date = dataList.get(0);
                                    if(null != date && date instanceof Date) {
                                        newDataList.add(DateUtil.format((Date)date, "yyyyMMdd"));
                                    }

                                    newDataList.add(dataList.get(1));
                                    newDataList.add(dataList.get(2));
                                    newDataList.add(dataList.get(3));
                                } else if(fn == 33) {
                                    newDataList = new ArrayList();
                                    newDataList.add(mpedId);
                                    newDataList.add(dataList.get(1));
                                    newDataList.add(dataList.get(2));
                                    newDataList.add(dataList.get(3));
                                    newDataList.add(dataList.get(4));
                                    newDataList.add(dataList.get(5));
                                    newDataList.add(dataList.get(6));
                                    newDataList.add(dataList.get(7));
                                    newDataList.add(dataList.get(8));
                                    date = dataList.get(0);
                                    if(null != date && date instanceof Date) {
                                        newDataList.add(DateUtil.format((Date)date, "yyyyMMdd"));
                                    }
                                } else if(fn == 37) {
                                    newDataList = new ArrayList();
                                    newDataList.add(mpedId);
                                    date = dataList.get(0);
                                    if(null != date && date instanceof Date) {
                                        newDataList.add(DateUtil.format((Date)date, "yyyyMMdd"));
                                    }

                                    newDataList.add(dataList.get(1));
                                    newDataList.add(dataList.get(2));
                                    newDataList.add(dataList.get(3));
                                    newDataList.add(dataList.get(4));
                                    newDataList.add(dataList.get(5));
                                    newDataList.add(dataList.get(6));
                                    newDataList.add(dataList.get(7));
                                    newDataList.add(dataList.get(8));
                                    newDataList.add(dataList.get(9));
                                    newDataList.add(dataList.get(10));
                                    newDataList.add(dataList.get(11));
                                    newDataList.add(dataList.get(12));
                                    newDataList.add(dataList.get(13));
                                    newDataList.add(dataList.get(14));
                                    newDataList.add(dataList.get(15));
                                } else if(fn == 27) {
                                    newDataList = new ArrayList();
                                    var14 = Calendar.getInstance();
                                    num = Calendar.getInstance();
                                    num.setTime((Date)dataList.get(0));
                                    newDataList.add(mpedId);
                                    newDataList.add(dataList.get(1));
                                    newDataList.add(dataList.get(2));
                                    newDataList.add(dataList.get(3));
                                    newDataList.add(dataList.get(4));
                                    newDataList.add(dataList.get(5));
                                    newDataList.add(dataList.get(6));
                                    newDataList.add(dataList.get(7));
                                    newDataList.add(dataList.get(8));
                                    newDataList.add(dataList.get(9));
                                    newDataList.add(dataList.get(10));
                                    newDataList.add(dataList.get(11));
                                    newDataList.add(dataList.get(12));
                                    newDataList.add(dataList.get(13));
                                    newDataList.add(dataList.get(14));
                                    newDataList.add(dataList.get(15));
                                    newDataList.add(dataList.get(16));
                                    if(dataList.get(17) != null) {
                                        var14.setTime((Date)dataList.get(17));
                                        var14.set(1, num.get(1));
                                        var14.set(2, num.get(2));
                                        newDataList.add(var14.getTime());
                                    } else {
                                        newDataList.add(dataList.get(17));
                                    }

                                    newDataList.add(dataList.get(18));
                                    if(dataList.get(19) != null) {
                                        var14.setTime((Date)dataList.get(19));
                                        var14.set(1, num.get(1));
                                        var14.set(2, num.get(2));
                                        newDataList.add(var14.getTime());
                                    } else {
                                        newDataList.add(dataList.get(19));
                                    }

                                    newDataList.add(dataList.get(20));
                                    if(dataList.get(21) != null) {
                                        var14.setTime((Date)dataList.get(21));
                                        var14.set(1, num.get(1));
                                        var14.set(2, num.get(2));
                                        newDataList.add(var14.getTime());
                                    } else {
                                        newDataList.add(dataList.get(21));
                                    }

                                    newDataList.add(dataList.get(22));
                                    if(dataList.get(23) != null) {
                                        var14.setTime((Date)dataList.get(23));
                                        var14.set(1, num.get(1));
                                        var14.set(2, num.get(2));
                                        newDataList.add(var14.getTime());
                                    } else {
                                        newDataList.add(dataList.get(23));
                                    }

                                    newDataList.add(dataList.get(24));
                                    if(dataList.get(25) != null) {
                                        var14.setTime((Date)dataList.get(25));
                                        var14.set(1, num.get(1));
                                        var14.set(2, num.get(2));
                                        newDataList.add(var14.getTime());
                                    } else {
                                        newDataList.add(dataList.get(25));
                                    }

                                    newDataList.add(dataList.get(26));
                                    if(dataList.get(27) != null) {
                                        var14.setTime((Date)dataList.get(27));
                                        var14.set(1, num.get(1));
                                        var14.set(2, num.get(2));
                                        newDataList.add(var14.getTime());
                                    } else {
                                        newDataList.add(dataList.get(27));
                                    }

                                    newDataList.add(dataList.get(28));
                                    newDataList.add(dataList.get(29));
                                    newDataList.add(dataList.get(30));
                                    if(dataList.size() < 32) {
                                        newDataList.add((Object)null);
                                        newDataList.add((Object)null);
                                        newDataList.add((Object)null);
                                        newDataList.add((Object)null);
                                        newDataList.add((Object)null);
                                        newDataList.add((Object)null);
                                        newDataList.add((Object)null);
                                        newDataList.add((Object)null);
                                        newDataList.add((Object)null);
                                    } else {
                                        newDataList.add(dataList.get(31));
                                        newDataList.add(dataList.get(34));
                                        newDataList.add(dataList.get(37));
                                        newDataList.add(dataList.get(32));
                                        newDataList.add(dataList.get(35));
                                        newDataList.add(dataList.get(38));
                                        newDataList.add(dataList.get(33));
                                        newDataList.add(dataList.get(36));
                                        newDataList.add(dataList.get(39));
                                    }

                                    var15 = dataList.get(0);
                                    if(null != var15 && var15 instanceof Date) {
                                        newDataList.add(DateUtil.format((Date)var15, "yyyyMMdd"));
                                    }
                                } else if(fn != 250 && fn != 251 && fn != 252) {
                                    if(fn == 35) {
                                        newDataList = new ArrayList();
                                        newDataList.add(mpedId);
                                        newDataList.add(DateUtil.format((Date)dataList.get(0), "yyyyMMdd"));

                                        for(var19 = 1; var19 < dataList.size(); ++var19) {
                                            newDataList.add(dataList.get(var19));
                                        }
                                    } else if(fn == 36) {
                                        newDataList = new ArrayList();
                                        newDataList.add(mpedId);
                                        newDataList.add(dataList.get(0));
                                        newDataList.add(dataList.get(1));
                                        newDataList.add(dataList.get(2));
                                    } else {
                                        String var30;
                                        if(fn >= 153 && fn <= 156) {
                                            newDataList = new ArrayList();
                                            var30 = "";
                                            boolean var25 = true;
                                            newDataList.add(mpedId);
                                            if(fn == 153) {
                                                var30 = "1";
                                            } else if(fn == 154) {
                                                var30 = "2";
                                            } else if(fn == 155) {
                                                var30 = "5";
                                            } else if(fn == 156) {
                                                var30 = "6";
                                            }

                                            newDataList.add(var30);
                                            newDataList.add(DateUtil.format((Date)dataList.get(0), "yyyyMMdd"));
                                            newDataList.add(dataList.get(1));

                                            for(var35 = 2; var35 < dataList.size(); ++var35) {
                                                if(dataList.get(var35) instanceof Double) {
                                                    var25 = false;
                                                    newDataList.add(dataList.get(var35));
                                                } else {
                                                    newDataList.add((Object)null);
                                                }
                                            }

                                            if(var25) {
                                                return null;
                                            }
                                        } else if(fn == 28) {
                                            newDataList = new ArrayList();
                                            newDataList.add(mpedId);
                                            newDataList.add(DateUtil.format((Date)dataList.get(0), "yyyyMMdd"));

                                            for(var19 = 1; var19 < dataList.size(); ++var19) {
                                                newDataList.add(dataList.get(var19));
                                            }
                                        } else if(fn == 246) {
                                            newDataList = new ArrayList();
                                            newDataList.add(mpedId);
                                            newDataList.add(DateUtil.format((Date)dataList.get(0), "yyyyMMdd"));
                                            newDataList.add((Object)null);
                                            var30 = dataList.get(1).toString();
                                            if(var30.contains("E")) {
                                                newDataList.add((Object)null);
                                            } else {
                                                newDataList.add(var30);
                                            }

                                            for(var16 = 2; var16 < dataList.size(); ++var16) {
                                                newDataList.add(dataList.get(var16));
                                            }
                                        } else if(fn == 210) {
                                            newDataList = new ArrayList();
                                            newDataList.add(mpedId);
                                            newDataList.add(DateUtil.format((Date)dataList.get(0), "yyyyMMdd"));
                                            newDataList.add(dataList.get(1));
                                            newDataList.add((Object)null);
                                            newDataList.add(dataList.get(5));
                                            newDataList.add(dataList.get(3));
                                            newDataList.add(dataList.get(9));
                                            newDataList.add(dataList.get(10));
                                            newDataList.add(dataList.get(7));
                                            newDataList.add(dataList.get(4));
                                            newDataList.add(Integer.valueOf(((Double)dataList.get(2)).intValue()));
                                            newDataList.add(dataList.get(8));
                                            newDataList.add(dataList.get(6));
                                        } else {
                                            Date var32;
                                            if(fn != 26 && fn != 34) {
                                                if(fn != 1) {
                                                    if(curveList.contains(Integer.valueOf(fn))) {
                                                        var32 = (Date)dataList.get(0);
                                                        newDataList = new ArrayList();
                                                        newDataList.add(mpedId);
                                                        newDataList.add(DateUtil.format(var32, "yyyyMMdd"));
                                                        return curveData(dataList, fn, newDataList);
                                                    }

                                                    throw new RuntimeException("Can not find fn type:" + fn);
                                                }

                                                var34 = new ArrayList();
                                                var16 = 3;

                                                for(var35 = 0; var35 < 4; ++var35) {
                                                    newDataList = new ArrayList();
                                                    var20 = DateUtil.format((Date)dataList.get(0), "yyyyMMdd");
                                                    newDataList.add(mpedId + "_" + var20);
                                                    newDataList.add(mpedId);
                                                    newDataList.add(String.valueOf(var35 + 1));
                                                    newDataList.add(dataList.get(1));
                                                    newDataList.add(dataList.get(var16));
                                                    List var23 = (List)((List)dataList.get(var16 + 1)).get(0);

                                                    for(int var24 = 0; var24 < 14; ++var24) {
                                                        if(var24 < var23.size()) {
                                                            newDataList.add(var23.get(var24));
                                                        } else {
                                                            newDataList.add((Object)null);
                                                        }
                                                    }

                                                    newDataList.add("");
                                                    newDataList.add("00");
                                                    newDataList.add("93");
                                                    newDataList.add(new Date());
                                                    newDataList.add("");
                                                    newDataList.add(var20);
                                                    var34.add(newDataList);
                                                    var16 += 2;
                                                }

                                                return var34;
                                            }

                                            newDataList = new ArrayList();
                                            var32 = (Date)dataList.get(0);
                                            newDataList.add(mpedId);
                                            newDataList.add("0");
                                            newDataList.add(var32);

                                            for(var16 = 1; var16 < dataList.size(); ++var16) {
                                                var15 = dataList.get(var16);
                                                if(fn == 34) {
                                                    if(var15 instanceof Date) {
                                                        newDataList.add(LastMonth(var15, var32));
                                                    } else {
                                                        newDataList.add(var15);
                                                    }
                                                } else {
                                                    newDataList.add(var15);
                                                }
                                            }

                                            newDataList.add((Object)null);
                                            newDataList.add((Object)null);
                                            newDataList.add(DateUtil.format(var32, "yyyyMMdd"));
                                        }
                                    }
                                } else {
                                    newDataList = new ArrayList();
                                    newDataList.add(mpedId);
                                    newDataList.add(DateUtil.format((Date)dataList.get(0), "yyyyMMdd"));
                                    newDataList.add("");
                                    if(fn == 250) {
                                        for(var19 = 1; var19 < 5; ++var19) {
                                            newDataList.add(dataList.get(var19));
                                        }
                                    } else if(fn == 251) {
                                        var19 = 1;
                                        var16 = 13;

                                        for(var35 = 0; var35 < 4; ++var35) {
                                            newDataList.add(dataList.get(var19));
                                            newDataList.add(dataList.get(var19 + 1));
                                            newDataList.add(dataList.get(var19 + 2));
                                            var19 += 3;
                                            newDataList.add(dataList.get(var16));
                                            newDataList.add(dataList.get(var16 + 1));
                                            newDataList.add(dataList.get(var16 + 2));
                                            newDataList.add(dataList.get(var16 + 3));
                                            var16 += 4;
                                        }
                                    } else if(fn == 252) {
                                        var19 = 1;
                                        var16 = 2;

                                        for(var35 = 0; var35 < 4; ++var35) {
                                            newDataList.add(dataList.get(var19));
                                            var19 += 2;
                                        }

                                        for(var35 = 0; var35 < 4; ++var35) {
                                            newDataList.add(dataList.get(var16));
                                            var16 += 2;
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        newDataList = new ArrayList();
                        newDataList.add(mpedId);
                        if(fn != 185 && fn != 193) {
                            if(fn != 186 && fn != 194) {
                                if(fn != 187 && fn != 195) {
                                    if(fn == 188 || fn == 196) {
                                        newDataList.add("6");
                                    }
                                } else {
                                    newDataList.add("5");
                                }
                            } else {
                                newDataList.add("2");
                            }
                        } else {
                            newDataList.add("1");
                        }

                        newDataList.add(dataList.get(1));
                        newDataList.add(dataList.get(3));
                        date = dataList.get(4);
                        num = Calendar.getInstance();
                        Calendar i = Calendar.getInstance();
                        i.setTime((Date)dataList.get(0));
                        if(date != null) {
                            num.setTime((Date)date);
                            num.set(1, i.get(1));
                            date = num.getTime();
                        }

                        if(!(date instanceof Date)) {
                            return null;
                        }

                        newDataList.add(date);
                        List dataDate = (List)((List)dataList.get(5)).get(0);

                        for(dl = 0; dl < dataDate.size(); ++dl) {
                            Object j = dataDate.get(dl);
                            if(dl % 2 != 0) {
                                Calendar fee = Calendar.getInstance();
                                Date time = (Date)j;
                                fee.setTime(time);
                                fee.set(1, i.get(1));
                                newDataList.add(fee.getTime());
                            } else {
                                newDataList.add(j);
                            }
                        }

                        Object var22 = dataList.get(0);
                        if(null != var22 && var22 instanceof Date) {
                            newDataList.add(DateUtil.format((Date)var22, "yyyyMMdd"));
                        }
                    }
                }
            }
        }

        return newDataList;
    }

    private static List curveData(List dataList, int fn, List newDataList) {
        Date date = (Date)dataList.get(0);
        int intval = ((Integer)dataList.get(1)).intValue();
        List data = (List)((List)dataList.get(2)).get(0);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int min = calendar.get(11) * 60 + calendar.get(12);
        int initPoint = min / databaseinterval(intval) + 1;
        ArrayList curvePointList = new ArrayList();

        for(Iterator var10 = data.iterator(); var10.hasNext(); ++initPoint) {
            Object obj = var10.next();
            ArrayList objs = new ArrayList();
            objs.addAll(newDataList);
            objs.add(Integer.valueOf(initPoint));
            objs.add(Integer.valueOf(intval));
            CurveDataUtils.addDataType(objs, fn);
            objs.add(obj);
            curvePointList.add(objs);
        }

        return curvePointList;
    }

    private static int databaseinterval(int type) {
        switch(type) {
            case 1:
                return 15;
            case 2:
                return 30;
            case 3:
                return 60;
            case 254:
                return 5;
            case 255:
                return 1;
            default:
                return 0;
        }
    }

    private static Date LastMonth(Object lastDate, Date callDate) {
        Date ld = (Date)lastDate;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(callDate);
        int year = calendar.get(1);
        int month = calendar.get(2);
        Calendar cs = Calendar.getInstance();
        cs.setTime(ld);
        cs.set(1, year);
        cs.set(2, month);
        return cs.getTime();
    }
}