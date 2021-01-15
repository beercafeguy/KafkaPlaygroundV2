package com.beercafeguy.avro.specific;

import com.hemchandra.avro.Customer;

public class SpecificRecordDemo {
    public static void main(String[] args) {

        //step 1: Create Specific Record
        Customer.Builder customerBuilder=Customer.newBuilder();
        customerBuilder.setFirstName("Hem");
        customerBuilder.setLastName("Chandra");
        customerBuilder.setHeight(159.8f);
        customerBuilder.setAge(30);
        customerBuilder.setWeight(65f);
        customerBuilder.setAutomatedEmail(false);

        Customer customer=customerBuilder.build();
        System.out.println("Created Customer:"+customer.toString());

        // write to file

        // read from file

        //
    }
}
