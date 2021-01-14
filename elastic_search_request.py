import requests
import json
import pandas as pd
import pyodbc
from datetime import datetime
  

server = 'myperfectsqlserver.database.windows.net' 
database = 'MyAzureSQLDatabase' 
username = 'martincarlsson' 
password = 'sadgjkjdgsgdfD#SFgsdjfnksdjn3fSDF' 

try :
    now = datetime.now()
    print(now.strftime("%y-%m-%d %H:%M:%S"),' : Connecting to Elastic DB')
 
    resp = requests.post('http://distribution.virk.dk/cvr-permanent/virksomhed/_search?scroll=1m&size=50',
                        headers = {'Content-Type': 'application/json',},
                        data = ' { "query":{"match_all":{}}}',
                        auth=('Imus.dk_CVR_I_SKYEN', '34324534-b6a4-4e80-800e-616686a644a9')      
                        )
    print(resp)
    now = datetime.now()
    print(now.strftime("%y-%m-%d %H:%M:%S"),' : Connection Established')
    resp = json.loads(resp.content)

    sid = resp['_scroll_id']
    row_cnt=0

    now = datetime.now()
    print(now.strftime("%y-%m-%d %H:%M:%S"),' : Connecting to Azure SQL DB')

    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    now = datetime.now()
    print(now.strftime("%y-%m-%d %H:%M:%S"),' : Connection Established')

    now = datetime.now()
    print(now.strftime("%y-%m-%d %H:%M:%S"),' : Start querying Elasticsearch DB')

    while (True):
        now = datetime.now()
        print(now.strftime("%y-%m-%d %H:%M:%S")," : Scrolling.....")

        headers = {
        'Content-Type': 'application/json',
        }

        params = {
        'scroll' : '1m',
        'scroll_id' : str(sid)
                }


        response = requests.post('http://distribution.virk.dk/_search/scroll',
                                headers=headers, params=params, 
                                auth=('Imus.dk_CVR_I_SKYEN', '34324534-b6a4-4e80-800e-616686a644a9')      
                                )
        response = json.loads(response.content)

        #sid = response['_scroll_id']
        out_doc = response['hits']['hits']

        doc=[]
        for json_object in out_doc:
            doc_lst=[]
            doc_lst.append(json_object['_source']['Vrvirksomhed']['cvrNummer'])
            doc_lst.append(json_object['_source'])
            doc.append(doc_lst)

        doc_df= pd.DataFrame(doc, columns= ['cvrNummer','Documents'])

        row_cnt += len(doc_df.axes[0])

        for index, row in doc_df.iterrows():
            cursor.execute("INSERT INTO [dbo].[cvr_data] ([cvr],[document]) values (?,?)" ,format(row['cvrNummer']),format(row['Documents']))

        cnxn.commit()
        now = datetime.now()
        print (now.strftime("%y-%m-%d %H:%M:%S"),' : {} rows inserted into the table'.format(row_cnt))

        sid = response['_scroll_id']

        if not (out_doc):
            now = datetime.now()
            print(now.strftime("%y-%m-%d %H:%M:%S"),' : Scrolling Ends and Insertion into table Completed')
            break;
        
    now = datetime.now()
    print(now.strftime("%y-%m-%d %H:%M:%S"),' : END query Elastic Search Table') 

    cursor.close()

    now = datetime.now()
    print(now.strftime("%y-%m-%d %H:%M:%S"),' : Execution Completed successfully')

except Exception as ep:
    print('Exception stack Trace at :' + str(ep))

