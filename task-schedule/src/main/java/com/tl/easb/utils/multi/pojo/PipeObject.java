package com.tl.easb.utils.multi.pojo;
/**
 * @author Dongwei-Chen
 * @Date 2020/1/8 9:41
 * @Description 管道对象
 */
public class PipeObject {
    /**
     * 管道对象
     */
    private String queue;
    /**
     * 传递对象
     */
    private Object object;

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }
}
