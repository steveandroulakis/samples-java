package io.temporal.samples.hello;

import io.temporal.activity.ActivityOptions;
import io.temporal.client.*;
import io.temporal.failure.ApplicationFailure;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class HelloUpdate {
  private static final String TASK_QUEUE = "HelloUpdateTaskQueue";
  private static final String WORKFLOW_ID = "GreetingWorkflow-" + UUID.randomUUID();

  public static void main(String[] args) throws Exception {
    WorkflowClient client = setupWorkflowClient();
    startWorker(client);
    runWorkflowWithUpdates(client);
  }

  private static WorkflowClient setupWorkflowClient() {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    return WorkflowClient.newInstance(service);
  }

  private static void startWorker(WorkflowClient client) {
    WorkerFactory factory = WorkerFactory.newInstance(client);
    Worker worker = factory.newWorker(TASK_QUEUE);

    worker.registerWorkflowImplementationTypes(GreetingWorkflowImpl.class);
    worker.registerActivitiesImplementations(new HelloActivity.GreetingActivitiesImpl());

    factory.start();
    System.out.println("Worker started");
  }

  private static void runWorkflowWithUpdates(WorkflowClient client) throws InterruptedException {
    WorkflowOptions options = WorkflowOptions.newBuilder()
            .setTaskQueue(TASK_QUEUE)
            .setWorkflowId(WORKFLOW_ID)
            .build();

    WorkflowStub workflowStub = client.newUntypedWorkflowStub("GreetingWorkflow", options);

    // Run the workflow with UpdateWithStart
    performUpdateWithStart(workflowStub);

    // Sleep for a bit before sending the next update
    Thread.sleep(1000);

    // Send another update
    performUpdate(workflowStub, "Steve test 2");

    // Sleep for a bit before sending the next update
    Thread.sleep(1000);

    // Send another update
    performUpdate(workflowStub, "Steve test 3");

    // Sleep for a bit before sending the next update
    Thread.sleep(1000);

    // Send another update
    performUpdate(workflowStub, "Steve test 4");

    // Wait for the workflow to complete
    waitForWorkflowCompletion(workflowStub);
  }

  private static void performUpdateWithStart(WorkflowStub workflowStub) {
    System.out.println("Starting workflow with UpdateWithStart");
    UpdateWithStartWorkflowOperation<String> update = UpdateWithStartWorkflowOperation.newBuilder(
                    "addGreeting", String.class, new Object[]{"Steve test 1"})
            .setWaitForStage(WorkflowUpdateStage.COMPLETED)
            .build();

    WorkflowUpdateHandle<String> updateHandle = workflowStub.updateWithStart(update);
    CompletableFuture<String> updateFuture = updateHandle.getResultAsync();
    String result = updateFuture.join();
    System.out.println("UpdateWithStart result: " + result);
  }

  private static void performUpdate(WorkflowStub workflowStub, String greeting) {
    System.out.println("Performing update on existing workflow");
    String updateResult = workflowStub.update("addGreeting", String.class, greeting);
    System.out.println("Update result. Queue size is: " + updateResult);
  }

  private static void waitForWorkflowCompletion(WorkflowStub workflowStub) {
    System.out.println("Waiting for workflow to complete");
    List<String> result = workflowStub.getResult(List.class);
    System.out.println("Workflow completed with result: " + result);
  }

  @WorkflowInterface
  public interface GreetingWorkflow {
    @WorkflowMethod
    List<String> getGreetings();

    @UpdateMethod
    int addGreeting(String name);

    @UpdateValidatorMethod(updateName = "addGreeting")
    void addGreetingValidator(String name);
  }

  public static class GreetingWorkflowImpl implements GreetingWorkflow {
    private final List<String> messageQueue = new ArrayList<>(10);
    private final List<String> receivedMessages = new ArrayList<>(10);
    private final HelloActivity.GreetingActivities activities = Workflow.newActivityStub(
            HelloActivity.GreetingActivities.class,
            ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(2)).build());

    @Override
    public List<String> getGreetings() {
      Workflow.sleep(Duration.ofSeconds(12));
      return messageQueue;
    }

    @Override
    public int addGreeting(String name) {
      if (name.isEmpty()) {
        throw ApplicationFailure.newFailure("Cannot greet someone with an empty name", "EmptyNameFailure");
      }
      String greeting = activities.composeGreeting("Hello", name);
      messageQueue.add(greeting);
      return receivedMessages.size() + messageQueue.size();
    }

    @Override
    public void addGreetingValidator(String name) {
      if (receivedMessages.size() >= 10) {
        throw new IllegalStateException("Only 10 greetings may be added");
      }
    }
  }
}