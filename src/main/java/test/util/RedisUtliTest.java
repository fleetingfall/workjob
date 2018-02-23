package test.util; 

import org.junit.Test; 
import org.junit.Before; 
import org.junit.After;
import util.RedisUtli;

/** 
* RedisUtli Tester. 
* 
* @author <Authors name> 
* @since <pre>���� 12, 2018</pre> 
* @version 1.0 
*/ 
public class RedisUtliTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: getJedis() 
* 
*/ 
@Test
public void testGetJedis() throws Exception {
    RedisUtli.getJedis();
} 

/** 
* 
* Method: setString(String key, String value) 
* 
*/ 
@Test
public void testSetString() throws Exception {
    RedisUtli.setString("kc","ck");
} 

/** 
* 
* Method: getString(String key) 
* 
*/ 
@Test
public void testGetString() throws Exception {
    String value=RedisUtli.getString("kc");
    System.out.println(value);
} 

/** 
* 
* Method: setList(String key, String ... value) 
* 
*/ 
@Test
public void testSetList() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getList(String key, int start, int end) 
* 
*/ 
@Test
public void testGetList() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getKeysCount() 
* 
*/ 
@Test
public void testGetKeysCount() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getKeys() 
* 
*/ 
@Test
public void testGetKeys() throws Exception { 
//TODO: Test goes here... 
} 


} 
