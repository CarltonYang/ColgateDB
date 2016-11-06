package tutorials.concurrency_tutorial;

public class ZigZagThreads {
    private static final LockManager lm = new LockManager();
    public static LockManager getLockManager() { return lm; }

    public static void main(String args[]) throws InterruptedException {
        int numZigZags = 10;
        for (int i = 0; i < numZigZags; i++) {
            new Thread(new Zigger()).start();
        }
        for (int i = 0; i < numZigZags; i++) {
            new Thread(new Zagger()).start();
        }
    }

    static class Zigger implements Runnable {

        protected String myPattern;
        protected boolean isZigger;

        public Zigger() {
            myPattern = "//////////";
            isZigger = true;
        }

        public void run() {
            getLockManager().acquireLock(isZigger);
            System.out.println(myPattern);
            getLockManager().releaseLock();
        }
    }

    static class Zagger extends Zigger {

        public Zagger() {
            myPattern = "\\\\\\\\\\\\\\\\\\\\";
            isZigger = false;
        }

    }

    static class LockManager {
        private boolean inUse = false;
        private boolean needZig = true;

        public void acquireLock(boolean isZigger) {
            boolean waiting = true;
            while (waiting) {
                synchronized (this) {
                    // check if lock is available
                    if (!inUse && (needZig==isZigger)) {
                        // it's not in use, so we can take it!
                        inUse = true;
                        waiting = false;
                    }
                    if (waiting || (needZig!=isZigger)) {
                        try {
                            this.wait();
                        } catch (InterruptedException e) {}
                    }
                }
            }
        }

        public synchronized void releaseLock() {
            inUse = false;
            if (needZig){
                needZig = false;
            } else {
                needZig = true;
            }
            this.notifyAll();
        }
    }}

