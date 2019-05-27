import boto3
import json
import datetime
import pandas as pd
import urllib.parse

s3 = boto3.resource('s3')

def lambda_handler(event, context):
    event_object_string = str(json.dumps(event))
    event_object = json.loads(event_object_string)
    source_bucket = event_object['Records'][0]['s3']['bucket']['name']
    source_key = urllib.parse.unquote(event_object['Records'][0]['s3']['object']['key'])
    source_bucket_split = source_bucket.split("-")
    source_key_split = source_key.split("/")
    source_file_split = source_key_split[7].split("-")
    year = source_key_split[3]
    month = str(int(source_key_split[4]))
    day = str(int(source_key_split[5]))
    hour = str(int(source_key_split[6]))
    company = source_bucket_split[3]
    id = ""
    un_encoded_file_obj = ""
    json_string = ""

    file_obj = s3.Object(source_bucket,source_key).get()['Body'].read().decode('utf-8')
    if(source_key_split[2] == 'integrated-event-details'):
        un_encoded_file_obj = urllib.parse.unquote_plus(file_obj).replace("]\n[",",").replace("\n","")
        json_string = (un_encoded_file_obj)
    else:
        un_encoded_file_obj = "[" + urllib.parse.unquote_plus(file_obj).replace("}\n{","},{").replace("\n","") + "]"
        json_string = (un_encoded_file_obj)


    integration_web = []
    integration_list = []
    integration_daily = []
    df = pd.read_json(json_string)
    df1 = pd.DataFrame(index = None)

    for row in df.itertuples():
        if(source_key_split[2] == 'integrated-events'):
            id = source_file_split[12] + source_file_split[13] + source_file_split[14] + source_file_split[15]  + source_file_split[16]
            if row[14] not in integration_list:
                integration_list.append(row[14])
            if(row[14] == 'web'):
                integration_web.append(row)
            else:
                integration_daily.append(row)

        if(source_key_split[2] == 'integrated-event-details'):
            id = source_file_split[13] + source_file_split[14] + source_file_split[15] + source_file_split[16]  + source_file_split[17]
            if row[8] not in integration_list:
                integration_list.append(row[8])
            if(row[8] == 'web'):
                integration_web.append(row)
            else:
                integration_daily.append(row)

    for row in integration_list:
        if(row == 'web'):
            df1 = pd.DataFrame(integration_web)
            destination_bucket = "bifrost-customer-" + company + "-prd"
            destination_key = (source_key_split[0] + "/raw/" + source_key_split[1] + "/" + source_key_split [2] +
                            "/_year=" + year + "/_month=" + month +
                            "/_day=" + day + "/_hour=" + hour +
                            "/journey-"+ source_key_split [2] + "-" + company + "-" + year + "-" + month + "-" + day + "-" + hour + "-" + id +
                            ".json")
        else:
            df1= pd.DataFrame(integration_daily, index = None)
            date = str(datetime.date.fromordinal(datetime.date.today().toordinal()-1))
            date_split = date.split('-')
            year  = str(int(date_split[0]))
            month = str(int(date_split[1]))
            day   = str(int(date_split[2]))

            destination_bucket = "bifrost-customer-" + company + "-prd"
            destination_key = (source_key_split[0] + "/raw/" + source_key_split[1] + "/" + source_key_split [2] +
                       "/_year=" + year + "/_month=" + month +
                       "/_day=" + day + "/_hour=" + '0' +
                       "/journey-"+ source_key_split [2] + "-" + company + "-" + year + "-" + month + "-" + day + "-" + '0' + "-" + id +
                       ".json")

    del df1['Index']
    out = df1.to_json(orient = 'records')
    s3.Object(destination_bucket, destination_key).put(Body=out)
    s3.Object(source_bucket,source_key).delete()    
