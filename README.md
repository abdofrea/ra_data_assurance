
# GGSN vs Billing in Mongo DB

This is a Python Script used to chrunk the huge amount of Data CDRs from both side GGSN and Billing, 


## Before to Start
this script requires a Mongo DB and at least 8Gbyte of RAM, it is tested for 800 million GGSN CDRs + 500 Million Billing CDRs, the output is depending in the number of active subscriber in the network, more illusteration bellow, in addition a steady and continues flow of CDRs in CSV format.


## Deployment

Due to the huge amount of Data Usage CDRs, where it can reach over 1 billion in some cases, and as a Revenue Assurance functionality is to ensure the right amount of usage and billing, therefore this script is used in order to generate one documents per user per day in the network to easily interoperate the result and capture any possible revenue leakage.

```json
 {
  "msisdn": "123456789",
  "GGSN": {
    "1": {
      "10": {
        "usage": 1839676
      }
    }
  },
  "billing": {
    "1": {
      "Data_Offer": {
        "10": {
          "usage": 1840734
        }
      }
    }
  }
}
```

the first attrebute is **msisdn** is the subscriber number,   
**GGSN** is accumulative usage of GGSN CDRS and it consists of layers 
the first Layer is for **Radio Access Type (RAT)**, the inner layer is for **Service Identifier (SI)**.  
**billing** is accumulative usage of actual usage from billing CDRS and it consists of layers 
the first Layer is for **Radio Access Type (RAT)**, then used **Offer** name, and the inner layer is for **Service Identifier (SI)**.  
Each document in average is 300 byte if the script generates a 10 Million documnets (depinding on the number of active subscriber) this will result a 2.5 Gbyte of data per day,,



## Use case

- To Audit GGSN Vs Billing CDRs,
- To Audit Service Identefire and ensure no free SI is abused like signaling or free websites etc...
- To audit the users who using the RAT of 4G LTE, without having Billing CDR, a sign unlicensed subscriber..
- Can be extended to Audit TAP-IN vs PGW and TAP-OUT vs SGW CDRs.
- Generates report per RAT, per SI. etc...
- This script can be part of larger ETL orchestrating system such Apache Airflow or other tool.

## Authors
Abdulwahed Freaa
- abdalwahed.frea@gmail.com
- [LinkedIN](https://www.linkedin.com/in/abdalwahed-frea-97a249194)

