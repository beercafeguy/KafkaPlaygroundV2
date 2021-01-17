package com.beercafeguy.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;


import java.io.File;
import java.io.IOException;

public class GenericRecordDemo {
    public static void main(String[] args) {

        Schema.Parser parser=new Schema.Parser();
        Schema schema=parser.parse("{\n" +
                "  \"type\": \"record\",\n" +
                "  \"namespace\": \"com.beercafeguy.avro\",\n" +
                "  \"name\": \"Customer\",\n" +
                "  \"doc\": \"Avro schema for simple customer\",\n" +
                "  \"fields\": [\n" +
                "    { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First name of the customer\"},\n" +
                "    { \"name\": \"middle_name\", \"type\": [\"null\",\"string\"],\"default\": null, \"doc\": \"First name of the customer\"},\n" +
                "    { \"name\": \"last_name\", \"type\": \"string\",\"doc\": \"Last name of the customer\"},\n" +
                "    { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age of the customer\"},\n" +
                "    { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height in CMs\"},\n" +
                "    { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight in kgs\"},\n" +
                "    { \"name\": \"city\", \"type\": \"string\",\"default\":\"No City\", \"doc\": \"City of residence\"},\n" +
                "    { \"name\": \"automated_email\", \"type\": \"boolean\",\"default\": true, \"doc\": \"Do we need to send automated emails for promotion\"}\n" +
                "  ]\n" +
                "}");

        // customer with defaults
        GenericRecordBuilder genericRecordBuilder=new GenericRecordBuilder(schema);
        genericRecordBuilder.set("first_name","Hem");
        genericRecordBuilder.set("last_name","Chandra");
        genericRecordBuilder.set("age",29);
        genericRecordBuilder.set("height",5.8d);
        genericRecordBuilder.set("weight",65);

        GenericData.Record customerRecord=genericRecordBuilder.build();
        System.out.println(customerRecord);


        // customer with defaults
        GenericRecordBuilder fullCustomerBuilder=new GenericRecordBuilder(schema);
        fullCustomerBuilder.set("first_name","Ankur");
        fullCustomerBuilder.set("middle_name","Bong");
        fullCustomerBuilder.set("last_name","Das");
        fullCustomerBuilder.set("age",39);
        fullCustomerBuilder.set("height",5.9d);
        fullCustomerBuilder.set("weight",75);
        fullCustomerBuilder.set("automated_email",false);


        GenericData.Record fullRecord=fullCustomerBuilder.build();
        System.out.println(fullRecord);


        //trigger errors

        // This will throw NullPointerException as we are setting a field which is not part of schema
        //GenericRecordBuilder errorBuilder=new GenericRecordBuilder(schema);
        //errorBuilder.set("region","Nainital");


        //Writing to a file
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        try(DataFileWriter<GenericRecord> writer=new DataFileWriter<>(datumWriter)){

            writer.create(customerRecord.getSchema(),new File("default_customer.avro"));
            writer.append(customerRecord);
            System.out.println("Written default_customer.avro successfully");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //reading a file

        final File file=new File("default_customer.avro");
        final DatumReader<GenericRecord> reader=new GenericDatumReader<>();
        GenericRecord customerFromDisk;
        try(DataFileReader<GenericRecord> dataFileReader=new DataFileReader<GenericRecord>(file,reader)){

            customerFromDisk=dataFileReader.next();
            System.out.println("Read from disk completed");
            System.out.println(customerFromDisk.toString());
            System.out.println("First Name :"+customerFromDisk.get("first_name"));
            System.out.println("City: "+customerFromDisk.get("city"));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
