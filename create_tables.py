import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import pandas as pd
import json


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    pd.DataFrame({"Param":
                      ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                  "Value":
                      [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                 })


#----------------------------------------------------------------------
    import boto3

    ec2 = boto3.resource('ec2',
                        region_name = "us-west-2",
                        aws_access_key_id = KEY,
                        aws_secret_access_key = SECRET   )

    s3 = boto3.resource('s3',
                        region_name = "us-west-2",
                        aws_access_key_id = KEY,
                        aws_secret_access_key = SECRET   )


    iam = boto3.client('iam',
                        region_name = "us-west-2",
                        aws_access_key_id = KEY,
                        aws_secret_access_key = SECRET   )



    redshift = boto3.client('redshift',
                            region_name = "us-west-2",
                            aws_access_key_id = KEY,
                            aws_secret_access_key = SECRET   )

    
    
#----------------------------------------------------------------------
    from botocore.exceptions import ClientError

    #1.1 Create the role, 
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)


        
    print("1.2 Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)


#----------------------------------------------------------------------
#
#    response = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

   
#----------------------------------------------------------------------
    # Attach Policy
#    print('1.2 Attaching Policy')
#    iam.attach_role_policy(RoleName = DWH_IAM_ROLE_NAME,
#                           PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
#                          ) ['ResponseMetadata']  ['HTTPStatusCode']

#----------------------------------------------------------------------
    # Get and print the IAM role ARN

#    print("1.3 Get the IAM role ARN")
#    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
#    print(roleArn)    

#----------------------------------------------------------------------
    try:
        response = redshift.create_cluster(        
            # TODO: add parameters for hardware
            ClusterType = DWH_CLUSTER_TYPE,
            NodeType = DWH_NODE_TYPE,
            NumberOfNodes = int(DWH_NUM_NODES),        

            # TODO: add parameters for identifiers & credentials
            DBName = DWH_DB,
            ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
            MasterUsername = DWH_DB_USER,
            MasterUserPassword = DWH_DB_PASSWORD,


            # TODO: add parameter for role (to allow s3 access)
            IamRoles = [roleArn]
        )
    except Exception as e:
        print(e)


#----------------------------------------------------------------------
    def prettyRedshiftProps(props):
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)

#----------------------------------------------------------------------
#    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
#    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
#    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
#    print("DWH_ROLE_ARN :: ", roleArn)
#----------------------------------------------------------------------
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)



#----------------------------------------------------------------------
#----------------------------------------------------------------------

    
    
    
    print(*config['CLUSTER'].values())
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()