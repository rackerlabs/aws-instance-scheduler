import json
import boto3
import jmespath
import time
from boto3.dynamodb.conditions import Key
import pprint
import datetime


def get_config():
    table_name = "ec2scheduler-ConfigTable-181J12SOFVGCB"
    INSTANCE_TABLE_NAME = "type"
    _config = "config"

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    response = table.query(
        KeyConditionExpression=Key(INSTANCE_TABLE_NAME).eq(_config)
    )
    return response["Items"][0]


def get_session_for_account(aws_account, cross_account_role):
    # get a token for the cross account role and use it to create a session
    sts = boto3.client("sts")
    try:
        session_name = "asg-scheduler-{}".format(aws_account)
        # assume a role
        token = sts.assume_role(
            RoleArn=cross_account_role, RoleSessionName=session_name
        )
        credentials = token["Credentials"]
        # create a session using the assumed role credentials
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )
    except Exception as ex:
        print(
            "Can not assume role {} for account {}, ({}))".format(
                cross_account_role, aws_account, str(ex)
            )
        )
        return None


def is_asg_exist(client, asg_name):
    response = client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg_name]
    )
    jmes = "AutoScalingGroups[*].AutoScalingGroupARN"
    if jmespath.search(jmes, response):
        return True
    else:
        return False


def is_asg_instances_healthy(client, asg_name):
    response = client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg_name]
    )
    jmes = "AutoScalingGroups[*].Instances[*].HealthStatus[]"
    pprint.pprint(jmespath.search(jmes, response))
    if "Unhealthy" in jmespath.search(jmes, response):
        return False
    else:
        return True


def resume_asg(client, asg_name):
    try:
        client.resume_processes(
            AutoScalingGroupName=asg_name,
            ScalingProcesses=["Launch", "Terminate"]
        )
        return True
    except Exception as ex:
        print("Error resume asg scaling, ({})", str(ex))
        return False


"""
INSTANCE_TABLE_TIMESTAMP = "timestamp"
INSTANCE_TABLE_PURGE = "purge_in_next_cleanup"
INSTANCE_TABLE_ACCOUNT_REGION = "account-region"
INSTANCE_TABLE_NAME = "service"

def get_instance(item):
    state_info = {i: item[i] for i in item if
                i not in [INSTANCE_TABLE_TIMESTAMP, INSTANCE_TABLE_NAME,
                            INSTANCE_TABLE_ACCOUNT_REGION, INSTANCE_TABLE_PURGE]}
    return state_info

def get_tables():
    _service = "ec2"
    _table_name = "ec2scheduler-StateTable-1LSA9KDUQ7ZLD"

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(_table_name)
    response = table.query(
            KeyConditionExpression=Key(INSTANCE_TABLE_NAME).eq(_service)
            )
    items = response.get("Items", {})
    for item in items:
        region = item.get(INSTANCE_TABLE_ACCOUNT_REGION)
        pprint.pprint(get_instance(item))
"""


def wait_and_resume_asg(event, context):
    config = get_config()
    # using event['asg_name'] directly is causing weird error
    asg_name = event["asg_name"]

    for account_role in config["cross_account_roles"]:
        aws_account = account_role.split(":")[4]

        session = get_session_for_account(aws_account, account_role)
        client = session.client("autoscaling")

        if is_asg_exist(client, asg_name):
            print(asg_name)

            now_plus_10 = (
                datetime.datetime.now() + datetime.timedelta(minutes=10)
            )
            while datetime.datetime.now() < now_plus_10:
                if is_asg_instances_healthy(client, asg_name):
                    break
                else:
                    print("waiting...")
                    time.sleep(30)

            if resume_asg(client, asg_name):
                return {"statusCode": 200, "body": json.dumps("Successful")}
            else:
                return {"statusCode": 500, "body": json.dumps("Error")}


if __name__ == "__main__":
    wait_and_resume_asg("", "")
