import pymongo
import os, csv
import time
from datetime import datetime, timedelta


conn = pymongo.MongoClient("mongodb://0.0.0.0:27017/") ## Mongo DB connection
mydb = conn["ggsn_occ"] ## Mongo DB name

def get_col_name(time_stamp): ## used to get the collection name for each CDRs
    table_name = "ggsn_occ_" + str(time_stamp)[:10].replace('-', '')
    return table_name


def get_batches_of_msisdns(list_of_msisdns):
    batch_len = 8000 ## length of number of batch of numbers to be operated on
    output_list = []
    i=0
    while i < len(list_of_msisdns):
        int_list = []
        j = 0
        while (i < len(list_of_msisdns)) and (j < batch_len):
            int_list.append(list_of_msisdns[i])
            i += 1 # outer loop
            j += 1 # inner loop
        output_list.append(int_list)
    return output_list ## return list of list [ [list 1], [list 2 ], [list 3] ]

def get_ggsn_line(line):
    msisdn = line[1]
    rat = line[2]
    si = line[3]
    coll_name = get_col_name(line[4])
    return msisdn, rat, si, coll_name

def get_billing_line(line):
    msisdn = line[0]
    coll_name = get_col_name(line[1])
    rat = line[5]
    offer = line[2]
    si = line[7]
    return msisdn, offer, rat, si, coll_name

while True:
    ############## GGSN CDRs
    ggsn_all_files = list(os.listdir('/cdrs_file/directory/GGSN_CDRS/')) ## ggsn files directory
    ggsn_all_files = list(filter(lambda x: x.endswith('.csv'), ggsn_all_files)) ## dont read temp files
    x=0
    pre_dict = {}
    for elem in ggsn_all_files:
        x+=1
        csv_Reader = csv.reader(open("/cdrs_file/directory/GGSN_CDRS//"+elem)) ## open each GGSN CSV files
        for line in csv_Reader:
            msisdn, rat, si, coll_name = get_ggsn_line(line) ## extract the follosing info from each line Number(MSISDN), Radio Acceess Type (RAT), Service Identefire (SI) and Collection name based on CDR Time Stamp
            pre_dict.setdefault(coll_name,{})
            pre_dict[coll_name].setdefault(msisdn,{"msisdn":msisdn, "GGSN":{}, "billing":{}})
            pre_dict[coll_name][msisdn]['GGSN'].setdefault(rat, {})
            pre_dict[coll_name][msisdn]['GGSN'][rat].setdefault(si, {"usage":0})
            pre_dict[coll_name][msisdn]['GGSN'][rat][si]["usage"]+=(int(line[6])+int(line[7])) ## uplink + downlink usage

    ###################### Billing CDRs
    billing_all_files = list(os.listdir('/cdrs_file/directory/billing_cdrs//'))## ggsn files directory
    billing_all_files = list(filter(lambda x: x.endswith('.csv'), occ_all_files))## dont read temp files
    x = 0
    for elem in billing_all_files:
        x += 1
        csv_Reader = csv.reader(open("/cdrs_file/directory/billing_cdrs//" + elem))
        for line in csv_Reader:
            msisdn, offer, rat, si, coll_name = get_billing_line(line) ## extract the follosing info from each line Number(MSISDN), Used Offer Name, Radio Acceess Type (RAT), Service Identefire (SI) and Collection name based on CDR Time Stamp
            pre_dict.setdefault(coll_name, {})
            pre_dict[coll_name].setdefault(msisdn, {"msisdn": msisdn, "GGSN": {}, "billing": {}})
            pre_dict[coll_name][msisdn]['billing'].setdefault(rat, {})
            pre_dict[coll_name][msisdn]['billing'][rat].setdefault(offer, {})
            pre_dict[coll_name][msisdn]['billing'][rat][offer].setdefault(si, {"usage":0})
            pre_dict[coll_name][msisdn]['billing'][rat][offer][si]['usage']+=int(line[4])
    for coll_table in pre_dict.keys():
        ### For each Collection in the pre_dict create the collection and its index
        mycol = mydb[coll_table]
        mycol.create_index('msisdn', unique=True)
        ####
        total_msisdns = len(pre_dict[coll_table].keys())
        batches_of_msisdns = get_batches_of_msisdns(list(pre_dict[coll_table].keys())) ## in order to not overuse the memory we divide the numbers of each collection to batched in our case we used 8000 per round
        for each_batch in batches_of_msisdns:
            to_be_updated = []
            collection_update = list(mycol.find({"msisdn":{"$in":each_batch}},{'_id':0}))## fetch for numbers allredy exist in the Mongo DB Collection
            msisdn_update = [str(elem["msisdn"]) for elem in collection_update] ### Make list of the numbers to be updated
            msisdn_insert = list(set(each_batch) - set(msisdn_update)) ### the rest  of the numbers will be inserted directly
            to_be_inserted = [pre_dict[coll_table][msisdn] for msisdn in msisdn_insert] ## make list of the document JSON that will be inserted directly
            ### the below section is to update the content of each document.for both GGSN and Billing Side.
            for collection in collection_update:
                val = {"msisdn":collection["msisdn"], "billing": collection["billing"], "GGSN": collection["GGSN"]}
                for rat in pre_dict[coll_table][collection["msisdn"]]['billing'].keys():
                    val["billing"].setdefault(rat,{})
                    for offer in pre_dict[coll_table][collection["msisdn"]]['billing'][rat].keys():
                        val["billing"][rat].setdefault(offer, {})
                        for si in pre_dict[coll_table][collection["msisdn"]]['billing'][rat][offer].keys():
                            val["billing"][rat][offer].setdefault(si, {'usage': 0})
                            val["billing"][rat][offer][si]['usage']+=pre_dict[coll_table][collection["msisdn"]]['billing'][rat][offer][si]["usage"]
                for cgis in pre_dict[coll_table][collection["msisdn"]]['GGSN'].keys():
                    for rat in pre_dict[coll_table][collection["msisdn"]]['GGSN'].keys():
                        val["GGSN"].setdefault(rat, {})
                        for si in pre_dict[coll_table][collection["msisdn"]]['GGSN'][rat].keys():
                            val["GGSN"][rat].setdefault(si, {'usage': 0})
                            val["GGSN"][rat][si]["usage"] += pre_dict[coll_table][collection["msisdn"]]['GGSN'][rat][si]["usage"]
                to_be_updated.append(val)
            mycol.delete_many({"msisdn": {"$in": msisdn_update}})
            if len(to_be_inserted):
                total_msisdns -= len(to_be_inserted)
                print(total_msisdns,coll_table,"to_be_inserted",len(to_be_inserted))
                mycol.insert_many(to_be_inserted)
            if len(to_be_updated):
                total_msisdns -= len(to_be_updated)
                print(total_msisdns,coll_table,"to_be_updated", len(to_be_updated))
                mycol.insert_many(to_be_updated)
    for elem in billing_all_files:
        os.remove("/cdrs_file/directory/GGSN_CDRS//" + elem) ## you can either remove or move thesefiles
    for elem in ggsn_all_files:
        os.remove("/cdrs_file/directory/billing_cdrs//" + elem) ## you can either remove or move thesefiles
    time.sleep(2) ### sleep untill the next batch
