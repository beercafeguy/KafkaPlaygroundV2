package com.beercafeguy.avro.specific;

import com.hemchandra.avro.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

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

        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try(DataFileWriter<Customer> writer=new DataFileWriter<>(datumWriter)){

            writer.create(customer.getSchema(),new File("specific_customer.avro"));
            writer.append(customer);
            System.out.println("Written specific_customer.avro successfully");
        } catch (IOException e) {
            e.printStackTrace();
        }

        // read from file

        final File file=new File("specific_customer.avro");
        final DatumReader<Customer> reader=new SpecificDatumReader<>();
        Customer customerFromDisk;
        try(DataFileReader<Customer> dataFileReader=new DataFileReader<>(file,reader)){

            customerFromDisk=dataFileReader.next();
            System.out.println("Read from disk completed");
            System.out.println(customerFromDisk.toString());
            System.out.println("First Name :"+customerFromDisk.getFirstName());
            //System.out.println("City: "+customerFromDisk.get("city"));
            // above line will throw AvroRuntimeException
        } catch (IOException e) {
            e.printStackTrace();
        }
        //
    }
}
