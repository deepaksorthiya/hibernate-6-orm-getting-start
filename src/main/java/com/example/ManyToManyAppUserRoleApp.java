package com.example;

import com.example.hbutil.Database;
import com.example.hbutil.HibernateUtil;
import com.example.manytomany.AppUser;
import com.example.manytomany.Role;
import com.example.manytomany.UserGroup;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;

import java.util.List;
import java.util.Set;

@Slf4j
public class ManyToManyAppUserRoleApp {

    public static void main(String[] args) {
        try {

            SessionFactory sessionFactory = HibernateUtil.getSessionFactory(new Class[]{UserGroup.class, AppUser.class, Role.class}, Database.H2);
            sessionFactory.inTransaction(session -> {
                Role userRole = new Role("ROLE_USER", "ROLE USER DESCRIPTION");
                Role adminRole = new Role("ROLE_ADMIN", "ROLE ADMIN DESCRIPTION");

                session.persist(userRole);
                session.persist(adminRole);

                AppUser admin1 = new AppUser("admin1@gmgg.com", "admin1", "admin1", "admin1");
                admin1.addRole(userRole);
                admin1.addRole(adminRole);
                session.persist(admin1);

                AppUser admin2 = new AppUser("admin2@gmgg.com", "admin2", "admin2", "admin2");
                admin2.addRole(userRole);
                admin2.addRole(adminRole);
                session.persist(admin2);

                AppUser user1 = new AppUser("user1@gmgg.com", "user1", "user1", "user1");
                user1.addRole(userRole);
                session.persist(user1);

                UserGroup adminGroup = new UserGroup("admin_group", "admin group description");
                UserGroup developerGroup = new UserGroup("developer_group", "developer group description");
                UserGroup userGroup = new UserGroup("user_group", "user group description");

                adminGroup.addAppUser(admin1);
                adminGroup.addAppUser(admin2);
                developerGroup.addAppUser(admin1);
                developerGroup.addAppUser(admin2);
                userGroup.addAppUser(admin1);
                userGroup.addAppUser(admin2);
                userGroup.addAppUser(user1);

                session.persist(adminGroup);
                session.persist(userGroup);
                session.persist(developerGroup);
            });


            // get user group by ID
            UserGroup userGroup = sessionFactory.fromTransaction((session -> session.find(UserGroup.class, 1)));
            log.info("userGroup: {}", userGroup);

            // get user and it's all roles and groups
            sessionFactory.inTransaction(session -> {
                AppUser appUser = session.createQuery("""
                                SELECT u
                                FROM AppUser u
                                JOIN FETCH u.userGroups g
                                JOIN FETCH u.roles r
                                WHERE u.userId = :userId
                                """, AppUser.class)
                        .setParameter("userId", 1)
                        .getSingleResult();
                Set<UserGroup> userGroups = appUser.getUserGroups();
                userGroups.remove(userGroups.iterator().next());
                log.info("AppUser: {}", appUser);
                log.info("Roles: {}", appUser.getRoles());
                log.info("Groups: {}", userGroups);
            });

            // Get Role and its users
            Role userR = sessionFactory.fromTransaction(session -> session.createQuery("""
                            SELECT r
                            FROM Role r
                            JOIN FETCH r.appUsers
                            WHERE r.roleId = :roleId
                            """, Role.class)
                    .setParameter("roleId", 1L)
                    .getSingleResult());
            log.info("role: {}", userR);

            // add-remove role to users efficiently
            sessionFactory.inTransaction(session -> {
                AppUser appUser = session.createQuery("""
                                SELECT u
                                FROM AppUser u
                                JOIN FETCH u.roles
                                WHERE u.userId = :userId
                                """, AppUser.class)
                        .setParameter("userId", 1L)
                        .getSingleResult();
                Role role = session.find(Role.class, 2L);
                Set<Role> roles = appUser.getRoles();
                // remove role from given user
                roles.remove(role);

                // add role to given user
                Role manager = new Role("ROLE_MANAGER", "ROLE MANAGER DESCRIPTION");
                session.persist(manager);
                roles.add(manager);
                log.info("AppUser :: {}", appUser);
            });

            // remove role and its mapping with user not user
            sessionFactory.inTransaction(session -> {
                int count = session.createMutationQuery("""
                                DELETE FROM Role r
                                WHERE r.roleId = :roleId
                                """)
                        .setParameter("roleId", 1L)
                        .executeUpdate();
                log.info("Row Count :: {}", count);
            });

            // remove all groups from a given user
            sessionFactory.inTransaction(session -> {
                AppUser appUser = session.createQuery("""
                                SELECT u
                                FROM AppUser u
                                JOIN FETCH u.userGroups g
                                JOIN FETCH g.appUsers a
                                WHERE u.userId = :userId
                                """, AppUser.class)
                        .setParameter("userId", 1L)
                        .getSingleResult();
                Set<UserGroup> userGroups = appUser.getUserGroups();
                List<UserGroup> ugs = userGroups.stream().toList();
                for (UserGroup ug : ugs) {
                    appUser.removeFromGroup(ug);
                }
            });

            // remove all groups from a given user
//            sessionFactory.fromTransaction(session -> {
//                int count = session.createNativeQuery("""
//                                DELETE
//                                FROM users_group_app_users_mapping ugm
//                                WHERE ugm.user_id = :userId
//                                """)
//                        .setParameter("userId", 1L)
//                        .executeUpdate();
//                log.info("Row Count :: {}", count);
//                return count;
//            });

        } finally {
            HibernateUtil.shutdown();
        }
    }
}
