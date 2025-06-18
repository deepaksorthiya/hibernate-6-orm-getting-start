package com.example;

import com.example.hbutil.Database;
import com.example.hbutil.HibernateUtil;
import com.example.onetone.ContactInfo;
import com.example.onetone.UserProfile;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OneToOneMultiThreadingApp {

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executor = null;
        SessionFactory sessionFactory = HibernateUtil.getSessionFactory(new Class[]{UserProfile.class, ContactInfo.class}, Database.H2);
        try {
            executor = Executors.newVirtualThreadPerTaskExecutor();
            for (int i = 0; i < 100; i++) {
                executor.submit(() -> {
                    System.out.println(Thread.currentThread().getName() + " Started");
                    sessionFactory.inTransaction(session -> {
                        UserProfile userProfile = new UserProfile(null, "user", "user", "user@gmail.com", null);
                        session.persist(userProfile);
                        userProfile.setLastName(userProfile.getId() + userProfile.getLastName());
                        userProfile.setFirstName(userProfile.getId() + userProfile.getFirstName());
                        userProfile.setEmail(userProfile.getId() + userProfile.getEmail());
                    });
                    System.out.println(Thread.currentThread() + " Finished");
                });
            }
        } finally {
            assert executor != null;
            boolean finished = shutdownAndAwaitTermination(executor);
            System.out.println("All thread finished :: " + finished);
            sessionFactory.inTransaction(session -> session.createQuery("select e from UserProfile e", UserProfile.class).getResultList()
                    .forEach(userProfile -> System.out.println("UserProfile (" + userProfile.getFirstName() + ") : " + userProfile.getLastName())));
            HibernateUtil.shutdown();
        }
    }

    static boolean shutdownAndAwaitTermination(ExecutorService pool) {
        // Disable new tasks from being submitted
        boolean finished = false;
        pool.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            finished = pool.awaitTermination(60, TimeUnit.SECONDS);
            if (!finished) {
                // Cancel currently executing tasks forcefully
                pool.shutdownNow();
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ex) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
        return finished;
    }

}
