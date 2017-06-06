package demo.test;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
/**
 * 批量插入线程类
 * 
 * @author pengjunlin
 *
 */
public class BatchInsertThread implements Runnable{
	
	BatchInsert batchInsert;
	
	static int loop=10;
	
	
	
	public BatchInsertThread(BatchInsert batchInsert){
		this.batchInsert=batchInsert;

	}

	
	public static void main(String[] args) {
		Executor executor=Executors.newSingleThreadExecutor();
		
		int limit=100000;
		
		while (limit>0) {
			BatchInsert bi=new BatchInsert();
			bi.setLimit(limit);
			executor.execute(new BatchInsertThread(bi));
			limit-=1000;
			
		}
	}

	public void run() { 
		synchronized (batchInsert) {
			try {
				for (int i = 0; i < loop; i++) {
					System.out.println("第--"+i+"---次---------------------开始");
					//batchInsert.deleteAll();
					batchInsert.batchInsertWithTransaction();
					System.out.println("第--"+i+"---次---------------------结束");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
			}
		}
	}
	

}
