public class SimpleLoggingApp {
    public static void main(String[] args) {
        int counter = 1;
        try {
            System.out.println("Simple Java logging application started");
            
            while (true) {
                System.out.println("Log entry #" + counter + " at " + new java.util.Date());
                
                // Every 10 entries, log to stderr to demonstrate both stdout and stderr capture
                if (counter % 10 == 0) {
                    System.err.println("ERROR: This is a sample error message #" + counter);
                }
                
                counter++;
                Thread.sleep(1500); // Sleep for 1.5 seconds between log entries
            }
        } catch (InterruptedException e) {
            System.err.println("Application interrupted: " + e.getMessage());
        }
    }
}