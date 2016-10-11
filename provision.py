import argparse
import logging
import re
import time

import boto3
import paramiko
import slackweb
from retrying import retry
from scp import SCPClient

instance_types = ['t2.nano', 't2.micro', 't2.small', 't2.medium', 't2.large', 'm4.large', 'm4.xlarge', 'm4.2xlarge',
                  'm4.4xlarge', 'm4.10xlarge', 'm3.medium', 'm3.large', 'm3.xlarge', 'm3.2xlarge', 'c4.large',
                  'c4.xlarge', 'c4.2xlarge', 'c4.4xlarge', 'c4.8xlarge', 'c3.large', 'c3.xlarge', 'c3.2xlarge',
                  'c3.4xlarge', 'c3.8xlarge', 'g2.2xlarge', 'g2.8xlarge', 'r3.large', 'r3.xlarge', 'r3.2xlarge',
                  'r3.4xlarge', 'r3.8xlarge', 'i2.xlarge', 'i2.2xlarge', 'i2.4xlarge', 'i2.8xlarge', 'd2.xlarge',
                  'd2.2xlarge', 'd2.4xlarge', 'd2.8xlarge']

memory_sizes = [ 0.5, 1, 2, 4, 8,
                 8, 16, 32, 64, 160,
                 3.75, 7.5, 15, 30,
                 3.75, 7.5, 15, 30, 60,
                 3.75, 7.5, 15, 30, 60,
                 15, 60,
                 15.25, 30.5, 61, 122, 244,
                 30.5, 61, 122, 244,
                 30.5, 61, 122, 244
                 ]

boto3.setup_default_session(region_name='eu-west-1')
logging.basicConfig(filename='provision.log', level=logging.DEBUG, format='%(asctime)s - %(message)s')

ec2 = boto3.resource('ec2')
ec2_client = boto3.client('ec2')
s3_client = boto3.client('s3')
slack = slackweb.Slack(url='https://hooks.slack.com/services/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')


def write_message(message):
    print message
    logging.info(message)
    slack.notify(text="Adwords Scorer: {}".format(message), channel='#background-jobs', username='cahootsy-bot',
                 icon_emoji=':snowboarder:')


def get_from_tag(ec2obj, tag):
    for o in ec2obj.filter(Filters=[{'Name': 'tag:Name', 'Values': [tag]}]):
        return o

    return None


def get_from_name(ec2obj, name):
    for o in ec2obj.filter(Filters=[{'Name': 'name', 'Values': [name]}]):
        return o

    return None


def get_with_description_matching(ec2obj, description):
    result = []
    for o in ec2obj.all():
        if re.match(description, o.description):
            result.append(o)

    return result


def get_from_image_id(ec2obj, image_id):
    result = []
    for o in ec2obj.filter(Filters=[{'Name': 'image-id', 'Values': [image_id]}]):
        result.append(o)

    return result


def create_spot_instance(image_id, image_type, spot_price):
    write_message("Creating {} spot instance using image ID {} at ${}".format(image_type, image_id, spot_price))
    spot_instance_request_id = ec2_client.request_spot_instances(
        SpotPrice=str(spot_price),
        LaunchSpecification={
            'ImageId': image_id,
            'KeyName': 'utility-remote-connect',
            'InstanceType': image_type,
            'Placement': {
                'AvailabilityZone': 'eu-west-1a',
            },
            'SubnetId': 'subnet-921cc5cb',
            'EbsOptimized': True,
            'Monitoring': {
                'Enabled': False
            },
            'IamInstanceProfile': {
                'Name': 'CTO'
            }

        }
    )['SpotInstanceRequests'][0]['SpotInstanceRequestId']

    write_message("Waiting for spot instance {} to be created".format(spot_instance_request_id))
    waiter = ec2_client.get_waiter('spot_instance_request_fulfilled')
    waiter.wait(SpotInstanceRequestIds=[spot_instance_request_id])
    write_message("Spot instance {} created".format(spot_instance_request_id))

    write_message("Fetching ID for spot instance request {}".format(spot_instance_request_id))
    instance_id = ec2_client.describe_spot_instance_requests(
        SpotInstanceRequestIds=[spot_instance_request_id]
    )['SpotInstanceRequests'][0]['InstanceId']
    write_message("Created instance ID {}".format(instance_id))

    return instance_id


def create_instance_image(instance_id, name, description):
    write_message("Creating image for {} with name {}".format(instance_id, name))
    image_id = ec2_client.create_image(
        InstanceId=instance_id,
        Name=name,
        Description=description
    )['ImageId']

    write_message("Waiting for image {} to be created".format(image_id))
    waiter = ec2_client.get_waiter('image_available')
    waiter.wait(ImageIds=[image_id])
    write_message("Image {} created".format(image_id))

    return image_id


def create_ebs_snapshot(volume_id, description):
    write_message("Creating spot instance of volume {} with description \"{}\"".format(volume_id, description))
    snapshot_id = ec2_client.create_snapshot(
        VolumeId=volume_id,
        Description=description
    )['SnapshotId']

    write_message("Waiting for snapshot {} to complete".format(snapshot_id))
    waiter = ec2_client.get_waiter('snapshot_completed')
    waiter.wait(SnapshotIds=[snapshot_id])
    write_message("Snapshot {} completed".format(snapshot_id))

    return snapshot_id


def get_spot_price_for(instance_type):
    return ec2_client.describe_spot_price_history(
        InstanceTypes=[instance_type],
        AvailabilityZone='eu-west-1a',
        MaxResults=100,
        Filters=[{'Name': 'product-description', 'Values': ['Linux/UNIX (Amazon VPC)']}]
    )['SpotPriceHistory'][0]['SpotPrice']


def delete_instance(instance):
    volume_ids = map(lambda x: x.id, list(instance.volumes.all()))

    write_message("Terminating old instance {}".format(instance.id))
    ec2_client.terminate_instances(InstanceIds=[instance.id])

    waiter = ec2_client.get_waiter('instance_terminated')
    waiter.wait(InstanceIds=[instance.id])

    for v in ec2.volumes.all():
        if v.id in volume_ids:
            write_message("Deleting old volume {}".format(v.id))
            ec2_client.delete_volume(VolumeId=v.id)


def delete_old_image(image):
    write_message("Deleting old image {}".format(image.id))
    ec2_client.deregister_image(
        ImageId=image.id
    )

    for s in get_with_description_matching(ec2.snapshots, ".* for " + image.id + " from .*"):
        write_message("Deleting snapshot {} for image {}".format(s.id, image.id))
        ec2_client.delete_snapshot(SnapshotId=s.id)


def delete_old_resources():
    image = get_from_name(ec2.images, 'Adwords copy of SOLR Production')

    if image is not None:
        # Delete any instances based on this Image
        for i in get_from_image_id(ec2.instances, image.id):
            delete_instance(i)


def provision(source_instance_name, image_type):
    delete_old_resources()

    image = get_from_name(ec2.images, 'Adwords copy of SOLR Production')
    if image is None:
        solr_production_instance = get_from_tag(ec2.instances, source_instance_name)

        current_time = time.strftime('%Y-%m-%d %H:%M:%S')

        image_id = create_instance_image(solr_production_instance.id, 'Adwords copy of SOLR Production',
                                         'Image of {} on {}'.format(source_instance_name, current_time))
    else:
        image_id = image.id

    min_spot_price = get_spot_price_for(image_type)
    spot_price = float(min_spot_price) + 0.005

    write_message("Current price ${}, bidding at ${}".format(min_spot_price, spot_price))
    instance_id = create_spot_instance(image_id, image_type, spot_price)

    write_message("Waiting for instance {} to start up".format(instance_id))
    waiter = ec2_client.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instance_id])

    write_message("Instance {} started".format(instance_id))

    return ec2.Instance(instance_id)


@retry(stop_max_attempt_number=3, wait_fixed=10000)
def connect_to_host_shell(host_name, user_name):
    write_message("Connecting to {} as {}".format(host_name, user_name))
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(host_name, username=user_name)
    return ssh


def copy_adwords_script_to_host(ssh_client):
    write_message("Putting adwords script onto host")
    scp = SCPClient(ssh_client.get_transport())
    try:
        scp.put(['check_search_results.py', 'my_queue.py', 'my_thread.py', 'requirements.txt'])
    finally:
        scp.close()


def run_shell(ssh_client, command):
    write_message("Running: {}".format(command))
    stdin, stdout, stderr = ssh_client.exec_command(command)

    for l in stdout.readlines():
        logging.debug(l.rstrip())


def run_adwords_script(ssh_client):
    run_shell(ssh_client, 'sudo apt-get update -y')
    run_shell(ssh_client, 'sudo apt-get install -y python-dev libffi-dev')
    run_shell(ssh_client, 'sudo pip install -r requirements.txt')
    run_shell(ssh_client, 'nohup python check_search_results.py > run.log 2>&1 &')


def set_memory_for_solr(ssh_client, image):
    image_index = instance_types.index(image)
    image_memory_size = int(memory_sizes[image_index] * 1024 * 0.75)

    run_shell(ssh_client, "sudo sed -i -- 's/Xmx[0-9]*M/Xmx{}M/g' /etc/supervisor/conf.d/solr.conf".format(image_memory_size))
    run_shell(ssh_client, "sudo supervisorctl reload")
    run_shell(ssh_client, "sudo supervisorctl restart solr")


def parse_arguments():
    parser = argparse.ArgumentParser(description='Provision a SOLR instance for evaluating Adwords')

    parser.add_argument('-i', '--image', help='the type of image to build (e.g. c4.large)', default='c4.large',
                        choices=instance_types)

    return parser.parse_args()


try:
    args = parse_arguments()

    new_instance = provision('solr-production.cahootsy.com', args.image)

    ssh_client = connect_to_host_shell(new_instance.public_ip_address, 'ubuntu')

    set_memory_for_solr(ssh_client, args.image)
    copy_adwords_script_to_host(ssh_client)
    run_adwords_script(ssh_client)
finally:
    s3_client.upload_file('provision.log', 'cahootsy-production', 'adwords/provision.log')
