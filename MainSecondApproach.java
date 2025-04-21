package openText;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import openText.Main.QueuedTask;
import openText.Main.Task;
import openText.Main.TaskExecutor;
import openText.Main.TaskGroup;
import openText.Main.TaskType;

public class MainSecondApproach {
	
	public static void main(String[] args) throws InterruptedException{
		MainSecondApproach obj = new MainSecondApproach();
		TaskExecutor executor = obj.new TaskExecutorImpl(3);
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
		
		Task task5 = new Task<String>(UUID.randomUUID(), group2, TaskType.WRITE, ()->{
			Thread.sleep(200);
			return "task5 complete";
		});
		
		System.out.println("Task1 " + task1.taskUUID);
		System.out.println("Task2 " + task2.taskUUID);
		System.out.println("Task3 " + task3.taskUUID);
		System.out.println("Task4 " + task4.taskUUID);
		System.out.println("Task5 " + task5.taskUUID);
		
		Future<String> result1 = executor.submitTask(task1);
		
		Future<String> result2 = executor.submitTask(task2);
		
		Future<String> result3 = executor.submitTask(task3);
		
		Future<String> result4 = executor.submitTask(task4);
		
		Future<String> result5 = executor.submitTask(task5);
		
		
		
		Thread.sleep(2000);
		executor.shutdown();
		
	}
	

	//Enumeration of task types.

	public enum TaskType {
		READ, WRITE;
	}
	

	public interface TaskExecutor {
		/**
     * Submit new task to be queued and executed.
     *
     * @param task Task to be executed by the executor. Must not be null.
     * @return Future for the task asynchronous computation result.
     */
		<T> Future<T> submitTask(Task<T> task);
		void shutdown();
	}
	
	//Implementation of TaskExecutor
	public class TaskExecutorImpl implements TaskExecutor{
		/*@param taskQueue
	     This is universal blocking queue for keeping tasks wrapped in QueuedTask record. It will ensure Fairness among threads.
	     If thread1 is submitted before thread2 then thread1 is guaranteed to be executed before thread2
	     
	     * @param runningGroups 
	     * Hashmap is used to keep the groupId for which thread is running
	     * 
	     * @param dispatcherThread
	     *  This thread is running in the background and take threads from the  taskQueue
	     *  
	     *  @param workerPool
	     *  Executor service provide fixedThreadPool to limit no. of threads in the system
	     */
		
		private final Map<UUID, CompletableFuture<Void>> groupChains = new ConcurrentHashMap<>();
        private final ExecutorService executor;

        public TaskExecutorImpl(int poolSize) {
            this.executor = Executors.newFixedThreadPool(poolSize);
        }

        // Submit a task and get a future result
        public <T> Future<T> submitTask(Task<T> task) {
            Future<T> taskResult = new CompletableFuture<>();
            CompletableFuture<T> completableFuture = new CompletableFuture<T>();

            groupChains.compute(task.taskGroup.groupUUID, (groupId, lastFuture) -> {
                if (lastFuture == null || lastFuture.isDone()) {
                    lastFuture = CompletableFuture.completedFuture(null);
                }

                return lastFuture.thenRunAsync(() -> {
                    try {
                        System.out.printf("Executing %s task %s in group %s [%s]%n",
                                task.taskType, task.taskUUID, task.taskGroup.groupUUID(), Thread.currentThread().getName());

                        T result = task.taskAction.call(); // Execute task
                        completableFuture.complete(result);   // Complete result
                    } catch (Exception e) {
                    	completableFuture.completeExceptionally(e);
                        System.err.printf("Error in task %s: %s%n", task.taskUUID, e.getMessage());
                    }
                }, executor);
            });

            return taskResult = completableFuture;
        }

		@Override
		 public void shutdown() {
            executor.shutdown();
        }

			 

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