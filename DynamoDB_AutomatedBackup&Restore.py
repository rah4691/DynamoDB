import boto3
import pandas as pd
import time
import shutup     #for batch process hide the library
shutup.please()   #hide the function

client = boto3.client(
    'dynamodb',
    aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxx',
    aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxx',
    region_name='us-east-1'
    )
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxx',
    aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxx',
    region_name='us-east-1'
    )



backuptablenamesaveresponse=[]
#
#Create the DynamoDB table with Index
num = 20
# # # # LMXBrtyui dev   PBrtntrdyh test
for i in range(1, num):
    table = dynamodb.create_table(
        TableName='dev_' + str(i) +'_'+'LMXBrtyui'+ '_Activity',
        KeySchema=[
            {
                'AttributeName': 'username',
                'KeyType': 'HASH'
            },
         ],
        AttributeDefinitions=[
            {
                'AttributeName': 'username',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }

    )

temp = input ("Source_Tenant_ID: ")
envinputold = input ("Source_Existing_Environment_Name: ")


def list_tables1(temp):
    global newdf,lendf
    response = client.list_tables()
    df = pd.DataFrame(response['TableNames'], columns=['TableName'])
    newdf = df[df["TableName"].str.contains(temp)]
    newdf=newdf.reset_index(drop=True)
    lendf = len(newdf)
    print(newdf)

list_tables1(temp)


jiratemp = input ("Jira_ID: ")

saveresponse=[]

def create_backup(tablename):
    print("Backing up table:", tablename)
    backup_name = tablename+'_'+ jiratemp+''
    response = client.create_backup(TableName=tablename, BackupName=backup_name)
    saveresponse.append(response["BackupDetails"])


for i in range(0,lendf,1):
  tablename=newdf.loc[i][0]
  create_backup(tablename)


newtemp = input ("Target tenant_ID for tables to be deleted: ")

def list_tables_to_delete(newtemp):
    global newdf1,lendf1
    response = client.list_tables()
    df1 = pd.DataFrame(response['TableNames'], columns=['TableName'])
    newdf1 = df1[df1["TableName"].str.contains(newtemp)]
    newdf1 = newdf1.reset_index(drop=True)
    lendf1=len(newdf1)
    print(newdf1)
    return newdf1

list_tables_to_delete(newtemp)

envinput = input("Target_Environment_Name: ")


for i in range(0,lendf,1):
    global tableName2,tableName
    tableName2 = newdf1.loc[i][0]
    print("Deleting table Names:", tableName2)
    client.delete_table(TableName=tableName2)
    # time.sleep(4)
    tableName = newdf.loc[i][0]
    backuptablename = list(filter(lambda p: tableName in p["BackupArn"], saveresponse))
    tableName1 = tableName.replace(temp, newtemp)
    tableName1 = tableName1.replace(envinputold, envinput)
    print(tableName2,"is restoring from",backuptablename[0]["BackupArn"])
    Backupresponse = client.restore_table_from_backup(
        TargetTableName=tableName1,
        BackupArn=backuptablename[0]["BackupArn"]

    )

    print("Restore in Process......")
    if((backuptablename[0]["BackupStatus"])=='CREATING'):
          time.sleep(60)
print("Restore Done")
