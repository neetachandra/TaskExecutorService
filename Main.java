package openText;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import openText.Main.Task;
import openText.Main.TaskExecutor;
import openText.Main.TaskGroup;
import openText.Main.TaskType;

public class Main {
	
	public static void main(String[] args) throws InterruptedException{
		TaskExecutor executor = new TaskExecutorImpl(3);
		TaskGroup group1 = new TaskGroup(UUID.randomUUID());
		TaskGroup group2 = new TaskGroup(UUID.randomUUID());
		TaskGroup group3 = new TaskGroup(UUID.randomUUID());
		Task task1 = new Task<String>(UUID.randomUUID(), group1, TaskType.READ, ()->{
			Thread.sleep(500);
			return "task1 complete";
		});
		Task task2 = new Task<String>(UUID.randomUUID(), group2, TaskType.READ, ()->{
			Thread.sleep(500);
			return "task2 complete";
		});
		
		Task task3 = new Task<String>(UUID.randomUUID(), group2, TaskType.WRITE, ()->{
			Thread.sleep(200);
			return "task3 complete";
		});
		
		Task task4 = new Task<String>(UUID.randomUUID(), group3, TaskType.WRITE, ()->{
			Thread.sleep(500);
			return "task4 complete";
		});
		
		System.out.println("Task1 " + task1.taskUUID);
		System.out.println("Task2 " + task2.taskUUID);
		System.out.println("Task3 " + task3.taskUUID);
		System.out.println("Task4 " + task4.taskUUID);
		
		CompletableFuture<String> result1 = executor.submitTask(task1);
		
		CompletableFuture<String> result2 = executor.submitTask(task2);
		
		CompletableFuture<String> result3 = executor.submitTask(task3);
		
		CompletableFuture<String> result4 = executor.submitTask(task4);
		
		
			result1.thenAccept(result -> System.out.println(result));
			result2.thenAccept(result -> System.out.println(result));
			result3.thenAccept(result -> System.out.println(result));
			result4.thenAccept(result -> System.out.println(result));
		
		Thread.sleep(2000);
		executor.shutdown();
		
	}
	

	//Enumeration of task types.

	public enum TaskType {
		READ, WRITE;
	}
	
	//Meaning 2 callables are required 1 for Read and another for write. Can use a big queue where one section is 

	public interface TaskExecutor {
		/**
     * Submit new task to be queued and executed.
     *
     * @param task Task to be executed by the executor. Must not be null.
     * @return Future for the task asynchronous computation result.
     */
		<T> CompletableFuture<T> submitTask(Task<T> task);
		void shutdown();
	}


	/**
   * Representation of computation to be performed by the {@link TaskExecutor}.
   *
   * @param taskUUID Unique task identifier.
   * @param taskGroup Task group.
   * @param taskType Task type.
   * @param taskAction Callable representing task computation and returning the result.
   * @param <T> Task computation result value type.
   */
	public record Task<T>(UUID taskUUID, TaskGroup taskGroup, TaskType taskType, Callable<T> taskAction) 
	{
		public Task{
			if(taskUUID==null || taskGroup==null || taskType==null || taskAction==null)
				throw new IllegalArgumentException("All parameters must not be null");
		}
	}
	/**
   * Task group.
   *
   * @param groupUUID Unique group identifier.
   */
	public record TaskGroup(UUID groupUUID) {
		public TaskGroup {
			if(groupUUID==null) {
				throw new IllegalArgumentException("All parameters must not be null");
			}
			
		}

	}
	
	/*
	 * QueueTask is a wrapper for Task as we want result of the task is associated with Task. 
	 * 
	 * @param task represents a task record
	 * @param future represents result of the taskAction 
	 * 
	 */
	
	public record QueuedTask<T>(Task<T> task, CompletableFuture<T> future)
	{
		public QueuedTask{
			if(task==null || future==null)
				throw new IllegalArgumentException("All parameters must not be null");
		}
	}

}