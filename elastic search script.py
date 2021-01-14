# import requests module 
import requests 
from requests.auth import HTTPBasicAuth 
import json
import pandas as pd
import pyodbc
  

server = 'myperfectsqlserver.database.windows.net' 
database = 'MyAzureSQLDatabase' 
username = 'martincarlsson' 
password = 'sadgjkjdgsgdfD#SFgsdjfnksdjn3fSDF' 

headers = {
    'Content-Type': 'application/json',
}

params = (
    ('scroll', '1m'),
)

data = ' { "query":{"match_all":{}}, "size":2000 }'

#Making a Request

response = requests.post('http://distribution.virk.dk/cvr-permanent/virksomhed/_search', 
                            headers=headers, params=params, data=data, 
                            auth=('Imus.dk_CVR_I_SKYEN', '34324534-b6a4-4e80-800e-616686a644a9'))


#print(response.json())

# print request object 
documents = response.json()
# print (type(documents))
#print (documents.keys())
# print (type(documents.get('hits')))
out_doc = (documents.get('hits')).get('hits')

#print( type(out_doc))

#Serializing json    
#json_object = json.dumps(out_doc) #,indent =4)

#print(json.dumps(out_doc[0] ,indent =4))

doc=[]

for json_object in out_doc:
    doc_lst=[]
    doc_lst.append(json_object['_source']['Vrvirksomhed']['cvrNummer'])
    doc_lst.append(json_object['_source'])
    doc.append(doc_lst)

doc_df= pd.DataFrame(doc, columns= ['cvrNummer','Documents'])

print('Sample Data')
print(doc_df)

#doc_df.to_excel(r'/Users/sagnikbanerjee/Desktop/elastic_search_doc.xlsx',index = False, header=True)
print ('Start Insertion')
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

#cursor.execute("TRUNCATE TABLE [dbo].[cvr_data]")

for index, row in doc_df.iterrows():
    cursor.execute("INSERT INTO [dbo].[cvr_data] ([cvr],[document]) values (?,?)" ,format(row['cvrNummer']),format(row['Documents']))

cnxn.commit()
cursor.close()

print('Execution Completed')



