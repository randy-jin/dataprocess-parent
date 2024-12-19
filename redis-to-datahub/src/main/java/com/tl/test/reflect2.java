package com.tl.test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class reflect2 {
	
	public static void getproperty(){
		try{
			//获取对象类型  ？代表所有的类
		Class<?> classType = Class.forName("com.tl.hades.datahub.reflect1");
		//通过反射获取用户类里所有的属性  (获取所有属性，不是方法，方法下面要组合的)
		Field[] fields = classType.getDeclaredFields();
		for(int i=0;i < fields.length;i++){
			Field field = fields[i];
			//获取属性名，也就是成员名
			String fieldName = field.getName();
			//组合它的get、set方法
			String firstLetter = fieldName.substring(0, 1).toUpperCase(); //取出成员名的第一个字母并大写
			//因为我们的get,set方法有个大写字符，是成员名的首字母，所以需要组合
			String getMethodName = "get"+firstLetter+ fieldName.substring(1);//"getId"
			String setMethodName = "set"+firstLetter+fieldName.substring(1);//"setId"
			//获取get方法（根据组合的内容）
			Method getMethod = classType.getMethod(getMethodName,new Class[]{});//方法名，参数类型
			//获取set方法（根据组合的内容）
			//Method setMethod = classType.getMethod(setMethodName, new Class[]{int.class});//这个就写死了
			Method setMethod = classType.getMethod(setMethodName, new Class[]{field.getType()});//参数类型是此成员的类型
			
			//reflect1 con = new reflect1("张三",1);
			//通过反射创建对象  （跟上面创建对象 作用是一样的，只是反射是动态的）
			Object obj = classType.getConstructor(new Class[]{String.class,int.class}).newInstance(
					new Object[] {"张三",1} ); //调用了有参构造方法
			//如果你不知道有参构造，你只能调用默认的无参构造方法了
			//Object obj1 = classType.getConstructor(new Class[]{}).newInstance(new Object[] {} );
			
			//有了方法与对象，接下来就是用对象调用方法了
			//取值 调用get方法
			Object value = getMethod.invoke(obj, new Object[]{});
			System.out.println("用反射方法获取get成员的值："+value);
			//set方法
			setMethod.invoke(obj, new Object[]{value});
		}
		
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void main(String[] args){
		//reflect2.getproperty();//反射演示成功
		
		//如果reflect1里只有一个add方法，其他什么都没有，我们反射这个类的add方法  如下：
		try{
		Class<?> classType = Class.forName("com.tl.hades.datahub.reflect1");
		Method method = classType.getMethod("add", new Class[]{int.class,int.class});
		Object test = classType.getConstructor(new Class[]{}).newInstance(new Object[]{});
		method.invoke(test, new Object[]{12,100});//执行后返回112
		}catch(Exception e){
			e.printStackTrace();
		}
		
		
	}
	
	
}
