package colgatedb.transactions;

import colgatedb.page.SimplePageId;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */
public class LockScheduleTest {
    private TransactionId tid0 = new TransactionId();
    private TransactionId tid1 = new TransactionId();
    private TransactionId tid2 = new TransactionId();
    private TransactionId tid3 = new TransactionId();
    private TransactionId tid4 = new TransactionId();
    private SimplePageId pid1 = new SimplePageId(0, 1);
    private SimplePageId pid2 = new SimplePageId(0, 2);
    private SimplePageId pid3 = new SimplePageId(0, 3);
    private SimplePageId pid4 = new SimplePageId(0, 4);
    private LockManager lm;
    private Schedule.Step[] steps;
    private Schedule schedule;

    @Before
    public void setUp() {
        lm = new LockManagerImpl();
    }

    @Test
    public void acquireLock() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),
                // important detail: acquired step must be included in schedule and should appear as soon as the
                // lock is acquired.  in this case, the lock is acquired immediately.
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED)
        };
        executeSchedule();
    }

    /**
     * Tricky test case:
     * - T1 has shared lock and T2 waiting on exclusive
     * - then T1 requests upgrade, it should be granted because upgrades get highest priority
     */
    @Test
    public void upgradeRequestCutsInLine() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),     // t1 requests shared
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid1, Schedule.Action.EXCLUSIVE),  // t2 waiting for exclusive
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),  // t1 requests upgrade, should be able to cut line
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),   // t1 gets exclusive ahead of t2
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED)    // now t2 can get exclusive
        };
        executeSchedule();
    }

    private void executeSchedule() {
        try {
            schedule = new Schedule(steps, lm);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertTrue(schedule.allStepsCompleted());
    }

    /**
     * mutiple share test case:
     * - T1 has shared lock and T2, T3 also ask for shared lock
     * - They should all be able to acquire the lock.
     */
    @Test
    public void multipleShareTest() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid1, pid2, Schedule.Action.SHARED),     // t1 requests shared
                new Schedule.Step(tid1, pid2, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid2, pid2, Schedule.Action.SHARED),     // t2 requests shared
                new Schedule.Step(tid2, pid2, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid3, pid2, Schedule.Action.SHARED),     // t3 requests shared
                new Schedule.Step(tid3, pid2, Schedule.Action.ACQUIRED)
        };
        executeSchedule();
    }

    /**
     * mutiple share test case:
     * - T0 has shared lock and T1 asks for an exlusive lock
     * - T2 also ask for shared lock
     * - T2 should not execute before T1 even though it is compatible with the current lock
     */
    @Test
    public void multipleShareTest2() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid3, Schedule.Action.SHARED),     // t1 requests shared
                new Schedule.Step(tid0, pid3, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid3, Schedule.Action.EXCLUSIVE),  // t2 waiting for exclusive
                new Schedule.Step(tid2, pid3, Schedule.Action.SHARED),     // t3 requests shared
                new Schedule.Step(tid0, pid3, Schedule.Action.UNLOCK),
                new Schedule.Step(tid1, pid3, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid3, Schedule.Action.UNLOCK),
                new Schedule.Step(tid2, pid3, Schedule.Action.ACQUIRED)
        };
        executeSchedule();
    }

    /**
     * mutiple share test case:
     * - T0 has shared lock and T1 both have shared lock,
     * - then T0 asks for updating to an exclusive lock
     * - T2 also ask for an exclusive lock
     * - T0 should not get lock before T1 unlocks. And T2 should only get lock after T1 in done with it.
     */
    @Test
    public void multipleShareTest3() {
        steps = new Schedule.Step[]{

                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),     // t0 requests shared
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid1, Schedule.Action.SHARED),     // t0 requests shared
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid2, pid1, Schedule.Action.EXCLUSIVE),  // t2 waiting for exclusive
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),  // t0 requests upgrade, can not acquire lock before T1 releases
                new Schedule.Step(tid1, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),   // t0 gets exclusive ahead of t2, cutting the line
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid2, pid1, Schedule.Action.ACQUIRED)    // now t2 can get exclusive
        };
        executeSchedule();
    }

    /**
     * mutiple share test case:
     * - T0 has exclusive lock and T1,T2,T3 all ask for a shared lock
     * - then T0 unlocks the lock
     * - all three threads should be able to acquire the lock at the same time.
     */
    @Test
    public void multipleShareTest4() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),     // t0 requests exclusive
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid1, Schedule.Action.SHARED),     // t1 requests shared
                new Schedule.Step(tid2, pid1, Schedule.Action.SHARED),  // t2 requests shared
                new Schedule.Step(tid3, pid1, Schedule.Action.SHARED),  // t3 requests shared
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.AcquiredStep(tid1, pid1),
                new Schedule.AcquiredStep(tid2, pid1,5 ),
                new Schedule.AcquiredStep(tid3, pid1,5 )
        };
        executeSchedule();
    }
}
