package org.gbif.dissco;

import org.gbif.api.service.registry.MachineTagService;
import org.gbif.registry.ws.client.OrganizationClient;
import org.gbif.ws.client.ClientFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility to add Machine Tags to the GBIF registry based on a tab file provided by Wouter A.
 *
 * java DisscoTags /tmp/dissco-tags.tsv http://api.gbif-uat.org/v1/ trobertson ***
 */
public class DisscoTags {
  static Map<String, Integer> fieldsNames = new HashMap() {{
    put ("country", 0);
    put ("institutionNameRaw", 1);
    put ("disscoMember", 2);
    put ("cetafMember", 3);
    put ("ROR", 4);
    put ("GRID", 5);
    put ("institutionNameROR", 6);
    put ("institutionCodeROR", 7);
    put ("gbifPublisherID", 8);
    put ("gbifPublisherName", 9);
  }};

  static Set<String> skipFields = new HashSet() {{
    add("gbifPublisherID");
    add("gbifPublisherName");
  }};

  public static void main(String[] args) throws Exception {
    File file = new File(args[0]);

    System.out.println(file);

    String wsUrl = args[1];
    String username = args[2];
    String password = args[3];

    ClientFactory clientFactory = new ClientFactory(username, password, wsUrl);
    MachineTagService service = clientFactory.newInstance(OrganizationClient.class);


    try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
      br.lines().skip(1).forEach(line -> {
        String[] cols = line.split("\t", -1);

        String organisationUUID = cols[fieldsNames.get("gbifPublisherID")];

        if (isNotEmpty(organisationUUID)) {
          System.out.println("Starting " + organisationUUID);

          List<String> fields = fieldsNames.keySet().stream().filter(s -> !skipFields.contains(s))
                  .collect(Collectors.toList());

          for (String field : fields) {
            String value = cols[fieldsNames.get(field)];

            if (isNotEmpty(value)) {
              String template = "{ \"namespace\":\"test.dissco.eu\", \"name\":\"%s\", \"value\":\"%s\"}";
              String tag = String.format(template, field, value);
              System.out.println(tag);

              try {
                service.addMachineTag(UUID.fromString(organisationUUID), "test.dissco.eu", field, value);
              } catch (Exception e) {
                System.err.println(e);
                e.printStackTrace();
              }
            }
          }
        }

        try {
          // go easy on UAT DB
          Thread.sleep(250);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      });
    }
  }



  static boolean isNotEmpty(String s) {
    return s!=null && s.length()>0;
  }

}
