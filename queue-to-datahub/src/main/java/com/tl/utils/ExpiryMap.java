package com.tl.utils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  * @Description: 带有效期的Map。
 * 每个key存入该map时，都需要设置key的有效期！当超期后，该key失效！
 * 此外，该map加入了一个定时器，每间隔一段时间，就自动扫描该map一次，清除失效的key
 * @param <K>
 * @param <V>
 */
public class ExpiryMap<K, V> implements Map<K, V> {
    private boolean isRefresh = false;
    private ConcurrentHashMap workMap = new ConcurrentHashMap();
    private ConcurrentHashMap<Object, Long> expiryMap = new ConcurrentHashMap<>();
 
    public ExpiryMap() {
        super();
        scheduleRemoveInValidKeys();
    }
 
    public ExpiryMap(boolean isRefresh) {
        this.isRefresh = isRefresh;
        scheduleRemoveInValidKeys();
    }
 
    /**
     * 定时清除失效key
     */
    private void scheduleRemoveInValidKeys() {
        int interval = 60;//频率，常规情况下执行多少次，扫描一次失效key
        int threshold = 10000;//map的容量阈值，达到该值后，将较频繁的执行key扫描工作！
        new Timer().schedule(new TimerTask() {
            int i = 0;
            @Override
            public void run() {
                boolean isRun = ++i % interval == 0 || expiryMap.keySet().size() > threshold;
                if (isRun) {
                    removeInValidKeys();
                }
            }
        }, 1000, 1000);//每隔1秒启动一次扫码
    }
    private void removeInValidKeys(){
        expiryMap.keySet().forEach(key->{
            if(expiryMap.get(key) < System.currentTimeMillis()){
                expiryMap.remove(key);
                workMap.remove(key);
            }
        });
        System.gc();
    }
 
    /**
     *
     * @param key key 的有效期！单位为：毫秒
     * @param expiry
     * @return
     */
    public Long addAndGet(K key,long expiry){
        Long value=0l;
        if(containsKey(key)){
            value = (Long)workMap.get(key);
        }
        workMap.put(key,++value);
 
        if(!containsKey(key)||isRefresh){
            expiryMap.put(key,System.currentTimeMillis() + expiry);
        }
        return value;
    }
    /**
     * put方法，需要设置key 的有效期！单位为：毫秒
     * @param key
     * @param value
     * @param expiry key的有效期，单位：毫秒
     * @return
     */
    public V put(K key, V value, long expiry) {
        if (!containsKey(key) || isRefresh) {//更新value，只有需要刷新时间时才需要操作expiryMap
            expiryMap.put(key, System.currentTimeMillis() + expiry);
        }
        workMap.put(key, value);
        return value;
    }
 
    @Override
    public int size() {
        return keySet().size();
    }
 
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }
 
    @Override
    public boolean containsKey(Object key) {
        if (key!=null &&expiryMap.containsKey(key)) {
            boolean flag = expiryMap.get(key) > System.currentTimeMillis();
            if(!flag){//key已经失效，则删除之
                expiryMap.remove(key);
                workMap.remove(key);
            }
            return flag;
        }
        return false;
    }
 
    @Override
    public boolean containsValue(Object value) {
        Collection values = workMap.values();
        if(values!=null){
            return values.contains(value);
        }
        return false;
    }
 
    @Override
    public V get(Object key) {
        if (containsKey(key)) {
            return (V) workMap.get(key);
        }
        return null;
    }
 
    @Deprecated
    @Override
    public V put(K key, V value) {
        throw new RuntimeException("此方法已废弃！请加上key失效时间");
    }
 
    @Override
    public V remove(Object key) {
        boolean flag =  containsKey(key);
        expiryMap.remove(key);
        V v=(V) workMap.remove(key);
        return flag?v:null;
    }
 
    @Deprecated
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new RuntimeException("此方法已废弃！");
    }
 
    @Override
    public void clear() {
        expiryMap.clear();
        workMap.clear();
    }
 
    @Override
    public Set<K> keySet() {
        removeInValidKeys();
        return workMap.keySet();
    }
 
    @Override
    public Collection<V> values() {
        removeInValidKeys();
        return workMap.values();
    }
 
    @Override
    public Set<Entry<K, V>> entrySet() {
        removeInValidKeys();
        return workMap.entrySet();
    }

    public static void main(String[] args) {
        ExpiryMap<String, String> map = new ExpiryMap<>();
        map.put("key1", "val1", 5000);
        while(true) {
            String val = map.get("key1");
            if (val == null) {
                System.out.println("过期了。。。。");
                break;
            }
            System.out.println(val);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}