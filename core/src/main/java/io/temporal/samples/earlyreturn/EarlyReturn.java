package io.temporal.samples.earlyreturn;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.ActivityOptions;
import io.temporal.client.*;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.*;

import java.time.Duration;
import java.util.UUID;

public class EarlyReturn {
    private static final String TASK_QUEUE = "EarlyReturnTaskQueue";
    private static final String UPDATE_NAME = "early-return";

    public static void main(String[] args) {
        WorkflowClient client = setupWorkflowClient();
        startWorker(client);
        runWorkflowWithUpdateWithStart(client);
    }

    private static WorkflowClient setupWorkflowClient() {
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        return WorkflowClient.newInstance(service);
    }

    private static void startWorker(WorkflowClient client) {
        WorkerFactory factory = WorkerFactory.newInstance(client);
        Worker worker = factory.newWorker(TASK_QUEUE);

        worker.registerWorkflowImplementationTypes(TransactionWorkflowImpl.class);
        worker.registerActivitiesImplementations(new TransactionActivitiesImpl());

        factory.start();
        System.out.println("Worker started");
    }

    private static void runWorkflowWithUpdateWithStart(WorkflowClient client) {
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setTaskQueue(TASK_QUEUE)
                .setWorkflowId("early-return-workflow-" + UUID.randomUUID())
                .build();

        Transaction tx = new Transaction("Bob", "Alice", 10000); // Amount in cents
        WorkflowStub workflowStub = client.newUntypedWorkflowStub("TransactionWorkflow", options);

        try {
            System.out.println("Starting workflow with UpdateWithStart");

            UpdateWithStartWorkflowOperation<String> update =
                    UpdateWithStartWorkflowOperation.newBuilder(
                                    UPDATE_NAME, String.class, new Object[] {})
                            .setWaitForStage(WorkflowUpdateStage.COMPLETED)
                            .build();

            WorkflowUpdateHandle<String> updateHandle = workflowStub.updateWithStart(update, tx);
            updateHandle.getResultAsync().get();
            System.out.println("Transaction initialized successfully");

            // The workflow will continue running, completing the transaction.
            String result = workflowStub.getResult(String.class);
            System.out.println("Workflow completed with result: " + result);
        } catch (Exception e) {
            System.out.println("Error during workflow execution: " + e.getMessage());
            // The workflow will continue running, cancelling the transaction.
        }
    }

    @WorkflowInterface
    public interface TransactionWorkflow {
        @WorkflowMethod
        String processTransaction(Transaction tx);

        @UpdateMethod(name = UPDATE_NAME)
        void returnInitResult();
    }

    public static class TransactionWorkflowImpl implements TransactionWorkflow {
        private final TransactionActivities activities = Workflow.newActivityStub(
                TransactionActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(30))
                        .build());

        private boolean initDone = false;
        private Exception initError = null;

        @Override
        public String processTransaction(Transaction tx) {

            // Phase 1: Initialize the transaction
            try {
                activities.initTransaction(tx);
            } catch (Exception e) {
                initError = e;
            } finally {
                initDone = true;
            }

            // Phase 2: Complete or cancel the transaction
            if (initError != null) {
                activities.cancelTransaction(tx);
                return "Transaction cancelled: " + initError.getMessage();
            } else {
                activities.completeTransaction(tx);
                return "Transaction completed successfully";
            }
        }

        @Override
        public void returnInitResult() {
            Workflow.await(() -> initDone);
            if (initError != null) {
                throw Workflow.wrap(initError);
            }
        }
    }

    @ActivityInterface
    public interface TransactionActivities {
        @ActivityMethod
        void initTransaction(Transaction tx);

        @ActivityMethod
        void cancelTransaction(Transaction tx);

        @ActivityMethod
        void completeTransaction(Transaction tx);
    }

    public static class TransactionActivitiesImpl implements TransactionActivities {
        @Override
        public void initTransaction(Transaction tx) {
            if (tx.getAmount() <= 0) {
                throw new IllegalArgumentException("Invalid amount");
            }
            sleep(500);
            System.out.println("Transaction initialized");
        }

        @Override
        public void cancelTransaction(Transaction tx) {
            sleep(1000);
            System.out.println("Transaction cancelled");
        }

        @Override
        public void completeTransaction(Transaction tx) {
            sleep(1000);
            System.out.println("Transaction completed");
        }

        private void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static class Transaction {
        private final String sourceAccount;
        private final String targetAccount;
        private final int amount;

        public Transaction(String sourceAccount, String targetAccount, int amount) {
            this.sourceAccount = sourceAccount;
            this.targetAccount = targetAccount;
            this.amount = amount;
        }

        public int getAmount() {
            return amount;
        }
    }
}