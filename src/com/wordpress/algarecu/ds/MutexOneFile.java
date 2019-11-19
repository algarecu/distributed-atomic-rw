package com.wordpress.algarecu.ds;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Semaphore;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/* A class the implements a semaphor with a count of one at a time 
 * author: algarecu@gmail.com */

public class MutexOneFile {
	static String filename = "threads-lock.txt";
	static Semaphore mySemaphore = new Semaphore(1);

	static class LockingThread extends Thread {
		String threadName = "";
		int opsCount = 1;

		LockingThread(String name){
			this.threadName = name;
		}

		public synchronized void run() {
			try {
				System.out.println(threadName + " : acquiring the file lock...");
				System.out.println(threadName + " : available permits now: " + mySemaphore.availablePermits());

				mySemaphore.acquire();
				System.out.println(threadName + " : got the thread lock!");
								
			    try {
			    	DateFormat dateFormat = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
				    Date today = Calendar.getInstance().getTime();
				    String logDate = dateFormat.format(today);
				    
					System.out.println(threadName + " : DOING OPERATION " + opsCount 
							+ ", available Semaphore permits : "
							+ mySemaphore.availablePermits());
					System.out.println(threadName + " : DOING WRITE TO FILE " + filename 
							+ ", with timestamp : "
							+ logDate);
					opsCount++;
				    
			        BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true));
			        writer.write(threadName.toString() +  ": " + logDate);
			        writer.newLine();
			        writer.close();
				}finally {
					System.out.println(threadName + " : releasing lock...");
					mySemaphore.release();

					System.out.println(threadName + " : available Semaphore permits now: " + mySemaphore.availablePermits());
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws InterruptedException {

		System.out.println("Total available Semaphore permits : " 
				+ mySemaphore.availablePermits());
		
		// Interleave threads to print to a file in an orderly manner (this could be random if sleeps removed)
		LockingThread t1 = new LockingThread("A");
		t1.start();

		LockingThread t2 = new LockingThread("B");
		t2.start();
		
		LockingThread t3 = new LockingThread("C");
		t3.start();

		LockingThread t4 = new LockingThread("D");
		t4.start();

		LockingThread t5 = new LockingThread("E");
		t5.start();

		LockingThread t6 = new LockingThread("F");
		t6.start();

	}
}
