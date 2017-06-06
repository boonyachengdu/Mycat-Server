package demo.test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
/**
 * 批量插入JDBC操作类
 * 
 * @author pengjunlin
 *
 */
public class BatchInsert {
	
	private String driver = "net.sf.log4jdbc.DriverSpy";/*"com.mysql.jdbc.Driver"*/;

	private String url = "jdbc:mysql://192.168.178.129:3306/db1";
	
	private String batch_url = "jdbc:mysql://192.168.178.129:3306/db1?useUnicode=true&characterEncoding=utf-8&rewriteBatchedStatements=true";//要5.1.13以上版本的驱动包

	private String user = "root";

	private String password = "123456";
	
	private int limit=10;
	
	private String method="batchInsertWithTransaction";
	
	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}
	
	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}
	

	@Before
	public void deleteAll(){
		Connection conn = null;
		try {
			Class.forName(driver);
			conn = (Connection) DriverManager.getConnection(url, user, password);
			String sql = "DELETE FROM userinfo ;";
			conn.prepareStatement(sql).execute();
		
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 记录执行的时间
	 * 
	 * @MethodName: insertResult 
	 * @Description: 
	 * @param methodName
	 * @param limit
	 * @param timeStr
	 * @throws
	 */
	public void insertResult(String methodName,String limit,String timeStr) {
		Connection conn = null;
		PreparedStatement pstm = null;
		try {
			Class.forName(driver);
			conn = (Connection) DriverManager.getConnection(url, user, password);
			SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String sql = "/*#mycat:db_type=master*/INSERT INTO processtask (pmethod,plimit,ptime,systime) VALUES('"+methodName+"','"+limit+"','"+timeStr+"','"+sdf.format(new Date())+"')";
			System.out.println(sql);
			pstm = (PreparedStatement) conn.prepareStatement(sql);
			pstm.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (pstm != null) {
				try {
					pstm.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 模拟生成批量执行SQL
	 * 
	 * @return
	 */
	public String moniSql(boolean isDelete){
		StringBuffer sql = new StringBuffer(isDelete?"DELETE FROM userinfo ;":"");
		sql.append("/*#mycat:db_type=master*/INSERT INTO userinfo(id,name,phone,address) VALUES");
		Random rand = new Random();
		int a, b, c, d;
		int index=1;
		for (int i = 1; i <= limit; i++) {
			a = rand.nextInt(10);
			b = rand.nextInt(10);
			c = rand.nextInt(10);
			d = rand.nextInt(10);
			if(index==limit){
				sql.append("("+i+",'boonya',"+"'188" + a + "88" + b + c + "66" + d+"','"+"xxxxxxxxxx_" + "188" + a + "88" + b + c
						+ "66" + d+"');");
			}else{
				sql.append("("+i+",'boonya',"+"'188" + a + "88" + b + c + "66" + d+"','"+"xxxxxxxxxx_" + "188" + a + "88" + b + c
						+ "66" + d+"'),");
			}
			index++;
		}
		System.out.println(sql.toString()); 
		return sql.toString();
		
	}

	
	@Test
	public void batchInsertWithTransaction() {
		Connection conn = null;
		PreparedStatement pstm = null;
		try {
			Class.forName(driver);
			conn = (Connection) DriverManager.getConnection(batch_url, user, password);
			conn.setAutoCommit(false);// 即手动提交
		    String sql=moniSql(false);
			pstm = (PreparedStatement) conn.prepareStatement(sql);
			Long startTime = System.currentTimeMillis();
			pstm.execute();
			conn.commit();// 手动提交
			Long endTime = System.currentTimeMillis();
			String timeStr=(endTime - startTime)+""; 
			System.out.println("OK,用时：" + timeStr);
			insertResult("batchInsertWithTransaction", limit+"", timeStr);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (pstm != null) {
				try {
					pstm.close();
				} catch (SQLException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
		}
	}
	
	@Test
	public void batchSQL(){
        int limit=10000;
		synchronized (batch_url) {
			while (limit>0) {
				BatchInsert bi=new BatchInsert();
				bi.setLimit(limit);
				String sql=bi.moniSql(true);
				System.out.println(sql);
				limit-=100;
			}
		}
	}

}
