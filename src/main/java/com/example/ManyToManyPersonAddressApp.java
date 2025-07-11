package com.example;

import com.example.hbutil.Database;
import com.example.hbutil.HibernateUtil;
import com.example.manytomany.Address;
import com.example.manytomany.Person;
import com.example.manytomany.PersonAddress;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;

import java.util.List;

@Slf4j
public class ManyToManyPersonAddressApp {

    public static void main(String[] args) {
        try {
            SessionFactory sessionFactory = HibernateUtil.getSessionFactory(new Class[]{Person.class, Address.class, PersonAddress.class}, Database.H2);
            sessionFactory.inTransaction(session -> {
                Person person1 = new Person("ABC-123");
                Person person2 = new Person("DEF-456");

                Address address1 = new Address("12th Avenue", "12A", "4005A");
                Address address2 = new Address("18th Avenue", "18B", "4007B");

                session.persist(person1);
                session.persist(person2);

                session.persist(address1);
                session.persist(address2);

                person1.addAddress(address1);
                person1.addAddress(address2);

                person2.addAddress(address1);

                session.persist(person1);
                session.persist(person2);
            });

            sessionFactory.inTransaction(session -> {
                List<PersonAddress> personAddresses = session
                        .createQuery(
                                """
                                        SELECT pa
                                        from PersonAddress pa
                                        join fetch pa.person p
                                        join fetch pa.address a
                                        where pa.person.id = :id and pa.address.id = :id
                                        """
                                , PersonAddress.class)
                        .setParameter("id", 1L)
                        .getResultList();
                for (PersonAddress pa : personAddresses) {
                    session.remove(pa);
                }
            });

        } finally {
            HibernateUtil.shutdown();
        }
    }
}
