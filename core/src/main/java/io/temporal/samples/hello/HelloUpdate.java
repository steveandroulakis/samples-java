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
import java.util.concurrent.CompletableFuture;

public class HelloUpdate {
  static final String TASK_QUEUE = "HelloUpdateTaskQueue";
  static final String randomId = java.util.UUID.randomUUID().toString();
  static final String WORKFLOW_ID = "GreetingWorkflow-" + randomId;

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
    private final HelloActivity.GreetingActivities activities =
        Workflow.newActivityStub(
            HelloActivity.GreetingActivities.class,
            ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(2)).build());

    @Override
    public List<String> getGreetings() {
      Workflow.sleep(Duration.ofSeconds(10));
      return messageQueue;
    }

    @Override
    public int addGreeting(String name) {
      if (name.isEmpty()) {
        throw ApplicationFailure.newFailure("Cannot greet someone with an empty name", "Failure");
      }
      messageQueue.add(activities.composeGreeting("Hello", name));
      return receivedMessages.size() + messageQueue.size();
    }

    @Override
    public void addGreetingValidator(String name) {
      if (receivedMessages.size() >= 10) {
        throw new IllegalStateException("Only 10 greetings may be added");
      }
    }
  }

  public static void main(String[] args) throws Exception {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);
    WorkerFactory factory = WorkerFactory.newInstance(client);
    Worker worker = factory.newWorker(TASK_QUEUE);

    worker.registerWorkflowImplementationTypes(GreetingWorkflowImpl.class);
    worker.registerActivitiesImplementations(new HelloActivity.GreetingActivitiesImpl());
    factory.start();

    WorkflowOptions workflowOptions =
        WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(WORKFLOW_ID).build();

    WorkflowStub workflowStub1 = client.newUntypedWorkflowStub("GreetingWorkflow", workflowOptions);

    // UpdateWithStart workflow
    UpdateWithStartWorkflowOperation<String> update1 =
        UpdateWithStartWorkflowOperation.newBuilder(
                "addGreeting", String.class, new Object[] {0, "Steve test 1"})
            .setWaitForStage(WorkflowUpdateStage.COMPLETED)
            .build();

    // Get the handle for the updateWithStart workflow
    WorkflowUpdateHandle<String> updateHandle1 = workflowStub1.updateWithStart(update1);
    System.out.println("UpdateWithStart workflow ID: " + WORKFLOW_ID + " started");

    // Get the result of the updateWithStart workflow's initial update
    CompletableFuture<String> updateWithStartFuture = updateHandle1.getResultAsync();
    String updateWithStartFutureResult = updateWithStartFuture.join();
    System.out.println("Result of updateWithStart: " + updateWithStartFutureResult);

    // Sleep for 5 seconds
    Thread.sleep(5000);

    // Update the workflow
    System.out.println("\nSending update to workflow");
    String updateResult = workflowStub1.update("addGreeting", String.class, 1, "Steve test 2");
    System.out.println("Result of update: " + updateResult);

    System.out.println("\nWaiting for workflow to complete");
    List<String> result = workflowStub1.getResult(List.class);
    System.out.println("Workflow completed with result: " + result);
    System.exit(0);
  }
}
