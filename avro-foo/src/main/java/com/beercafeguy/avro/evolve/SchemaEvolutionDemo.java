package com.beercafeguy.avro.evolve;

import com.example.CustomerV1;
import com.example.CustomerV2;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SchemaEvolutionDemo {
    public static void main(String[] args) {

        CustomerV1 customerV1=CustomerV1.newBuilder()
                .setAge(30)
                .setFirstName("Hem")
                .setHeight(169f)
                .setWeight(45.7f)
                .setLastName("Chandra")
                .setAutomatedEmail(false)
                .build();

        System.out.println("Customer V1: "+customerV1);

        final DatumWriter<CustomerV1> datumWriter=new SpecificDatumWriter<>(CustomerV1.class);
        try(DataFileWriter<CustomerV1> writer=new DataFileWriter<>(datumWriter)){
            writer.create(customerV1.getSchema(),new File("customer-v1.avro"));
            writer.append(customerV1);
            System.out.println("V1 write completed");
        } catch (IOException e) {
            e.printStackTrace();
        }


        //read using v2
        final File file=new File("customer-v1.avro");
        final DatumReader<CustomerV2> datumReader=new SpecificDatumReader<>(CustomerV2.class);
        try {
            final DataFileReader<CustomerV2> reader=new DataFileReader<>(file,datumReader);
            while(reader.hasNext()){
                CustomerV2 customerV2=reader.next();
                System.out.println("Customer V2 :"+customerV2);
                System.out.println("Customer Mob Number:"+customerV2.getPhoneNumber());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
