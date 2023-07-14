import requests
import os
import pandas as pd

# returns pandas dataframe with url response json payload
def response(payload):
# actual payload of returned RQL is in '$.data.items.data' this is put in pandas df
  response = requests.request("POST", url, headers=headers, data=payload).json()['data']['items']
# '$.data.items.data' is a list of dicts, it is being looped through to create a pandas df
  return pd.json_normalize([item['data'] for item in response] )

url = "https://api0.prismacloud.io/search/config"

token = os.getenv("prisma_token")

# check if s3 buckets acl is publicly exposed
rql1 = "{\r\n    \"withResourceJson\":true,\r\n    \"query\":\"config from cloud.resource where cloud.type = 'aws' AND api.name='aws-s3api-get-bucket-acl' AND json.rule = ((((acl.grants[?(@.grantee=='AllUsers')] size > 0) or policyStatus.isPublic is true) and publicAccessBlockConfiguration does not exist and accountLevelPublicAccessBlockConfiguration does not exist) or ((acl.grants[?(@.grantee=='AllUsers')] size > 0) and ((publicAccessBlockConfiguration.ignorePublicAcls is false and accountLevelPublicAccessBlockConfiguration does not exist) or (publicAccessBlockConfiguration does not exist and accountLevelPublicAccessBlockConfiguration.ignorePublicAcls is false) or (publicAccessBlockConfiguration.ignorePublicAcls is false and accountLevelPublicAccessBlockConfiguration.ignorePublicAcls is false))) or (policyStatus.isPublic is true and ((publicAccessBlockConfiguration.restrictPublicBuckets is false and accountLevelPublicAccessBlockConfiguration does not exist) or (publicAccessBlockConfiguration does not exist and accountLevelPublicAccessBlockConfiguration.restrictPublicBuckets is false) or (publicAccessBlockConfiguration.restrictPublicBuckets is false and accountLevelPublicAccessBlockConfiguration.restrictPublicBuckets is false))))\",\r\n    \"timeRange\":{\"type\":\"to_now\",\"value\":\"epoch\"},\r\n    \"heuristicSearch\":true\r\n}"
# check s3 bucket is used to store the cloud trail
rql2 = "{\r\n    \"withResourceJson\":true,\r\n    \"query\":\"config from cloud.resource where api.name = 'aws-cloudtrail-describe-trails'\",\r\n    \"timeRange\":{\"type\":\"to_now\",\"value\":\"epoch\"},\r\n    \"heuristicSearch\":true\r\n}"

headers = {
  'Content-Type': 'application/json; charset=UTF-8',
  'Accept': 'application/json; charset=UTF-8',
  'x-redlock-auth': token
}

# put the json response in pandas
df1 = response(rql1)
df2 = response(rql2)

print("s3 buckets used to strore Cloudtrail: ",len(df2))

# s3 cloud trail buckets present (not df2.empty) and none is exposed (df1.empty)
if df1.empty and not df2.empty:
  print("s3 buckets not publicly exposed (pass): ", len(df2) - len(df1))
  print("s3 buckets publicly exposed (fail): ", len(df1))

# s3 cloud trail buckets present (not df2.empty) and some are exposed (not df1.empty)
if not df1.empty and not df2.empty:
# two tables are joined on bucket names to get the buckets appearing in both
  result = df1.merge(df2,left_on='bucketName',right_on='s3BucketName')
  print("s3 buckets not publicly exposed (pass): ", len(df2['s3BucketName']) - len(result))
  print("s3 buckets publicly exposed (fail): ", len(result))
