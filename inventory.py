import logging 
import os 
import json 
import boto3

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s', 
    level=os.environ.get("LOGLEVEL", "INFO")) 
logger = logging.getLogger(__name__) 
def custom_paginator(client, method, **operation_parameters): 
    """ 
    I am a custom paginator to efficiently rreturn the results 
    """ 
    if client.can_paginate(method): 
        paginator = client.get_paginator(method) 
        response_iterator = paginator.paginate(**operation_parameters) 
        for page in response_iterator: 
            yield page 
    else: 
        response = getattr(client, method)(**operation_parameters) 
        yield response 
        while "NextToken" in response: 
            response = getattr(client, method)(**operation_parameters, 
                NextToken=response["NextToken"]) 
            yield response 
            
def get_tag_from_list(tag_list, tag_name, tag_key_name='Key', tag_value_name='Value'): 
    """ 
    I retun a tag value from the key passed in 
    """ 
    return_value = 'Key not found. Key Name: ' + tag_name 
    tag_list = [tag[tag_value_name] for tag in tag_list if tag[tag_key_name] == tag_name] 
    if len(tag_list) == 1: 
        return_value = tag_list[0] 
    if len(tag_list) >1: 
        return_value = 'Multiple Keys With Key Name: ' + tag_name 
    return return_value 
def get_instances(response_filter=None): 
    """ 
    I get instances 
    """ 
    if response_filter: 
        kwargs = response_filter 
    else: 
        kwargs = {} 
    instance_dict = {} 
    client = boto3.client('ec2') 
    instance_generator = custom_paginator(client, 'describe_instances', **kwargs) 
    instance_id = instance_name = data_type = None 
    for reservations in instance_generator: 
        for reservation in reservations['Reservations']: 
            instance_id = instance_name = data_type= None 
            for instance in reservation['Instances']: 
                instance_id = instance['InstanceId'] 
                instance_name = (get_tag_from_list(instance['Tags'], 'Name')) 
                data_type  = (get_tag_from_list(instance['Tags'], 'DataType')) 
            instance_dict[instance_id] = {} 
            instance_dict[instance_id]['Name'] = instance_name 
            instance_dict[instance_id]['DataType'] = data_type 
    return instance_dict 
def get_volumes(response_filter=None): 
    """ 
    I get all the volumes 
    """ 
    if response_filter: 
        kwargs = response_filter 
    else: 
        kwargs = {} 
    volume_dict = {} 
    client = boto3.client('ec2') 
    volume_generator = custom_paginator(client, 'describe_volumes', **kwargs) 
    for volumes in volume_generator: 
        volume_id = volume_state = volume_name = volume_datatype = volume_attachments = None 
        volume_attachments = [] 
        for volume in volumes['Volumes']: 
            volume_id = (volume['VolumeId']) 
            volume_state = (volume['State']) 
            if 'Tags' in volume: 
                volume_name = get_tag_from_list(volume['Tags'],'Name') 
                volume_datatype = get_tag_from_list(volume['Tags'],'DataType') 
            else: 
                logger.debug('Volume has no tags %s', volume_id) 
            volume_attachments = [] 
            for attachment in volume['Attachments']: 
                volume_attachments.append(attachment['InstanceId']) 
            volume_dict[volume_id] = {} 
            volume_dict[volume_id]['Name'] = volume_name 
            volume_dict[volume_id]['State'] = volume_state 
            volume_dict[volume_id]['DataType'] = volume_datatype 
            volume_dict[volume_id]['Attachments'] = volume_attachments 
    return volume_dict 
def get_snapshots(response_filter=None): 
    """ 
    I get all the snapshots. 
    """ 
    if response_filter: 
        kwargs = response_filter 
    else: 
        kwargs = {} 
    snapshot_dict = {} 
    client = boto3.client('ec2') 
    snapshot_generator = custom_paginator(client, 'describe_snapshots', **kwargs) 
    for snapshots in snapshot_generator: 
        snapshot_id = snapshot_date = snapshot_state = snapshot_volume_id = None 
        for snapshot in snapshots['Snapshots']: 
            snapshot_id = (snapshot['SnapshotId']) 
            snapshot_date = (snapshot['StartTime']) 
            snapshot_volume_id = (snapshot['VolumeId']) 
            snapshot_state = (snapshot['State']) 
            snapshot_dict[snapshot_id] = {} 
            snapshot_dict[snapshot_id]['Date'] = snapshot_date 
            snapshot_dict[snapshot_id]['VolumeId'] = snapshot_volume_id 
            snapshot_dict[snapshot_id]['State'] = snapshot_state 
    return snapshot_dict 
def get_backup_jobs(response_filter=None): 
    """ 
    I get all the backup jobs. 
    """ 
    if response_filter: 
        kwargs = response_filter 
    else: 
        kwargs = {} 
    backupjob_dict = {} 
    client = boto3.client('backup') 
    backupjob_generator = custom_paginator(client, 'list_backup_jobs', **kwargs) 
    for backupjobs in backupjob_generator: 
        for backupjob in backupjobs['BackupJobs']: 
            backup_job_id = resource_arn = creation_date = state = '' 
            try: 
                backup_job_id = backupjob['BackupJobId'] 
                resource_arn = backupjob['ResourceArn'] 
                creation_date = backupjob['CreationDate'] 
                state = backupjob['State'] 
            except KeyError as missing_key: 
                logger.error("Missing Key Error: %s", missing_key) 
            backupjob_dict[backup_job_id] = {} 
            backupjob_dict[backup_job_id]['ResourceArn'] = resource_arn 
            backupjob_dict[backup_job_id]['CreationDate'] = creation_date 
            backupjob_dict[backup_job_id]['State'] = state 
    return backupjob_dict 
def map_instance_volume_snapshot(): 
    """ 
    I map instances to volumes to snapshots 
    """ 
    #backup_tags = ['HIPAA','IRIS_HIPAA','IRIS_NON_HIPAA','NON_HIPAA'] 
    backup_tags = ['NON_HIPAA'] 
    kwargs = {'Filters':[{'Name':'tag:DataType','Values': backup_tags}]} 
    instances = get_instances(kwargs) 
    kwargs = {'Filters':[{'Name':'attachment.instance-id', 'Values': list(instances.keys())}]} 
    volumes = get_volumes(kwargs) 
    kwargs = {'Filters':[{'Name':'volume-id','Values':list(volumes.keys())}],'OwnerIds':['self']} 
    snapshots = get_snapshots(kwargs) 
    for volume_id, volume_value in volumes.items(): 
        snapshot_list = [] 
        for snapshot_id, snapshot_value in snapshots.items(): 
            if snapshot_value['VolumeId'] == volume_id: 
                snapshot_list.append({ 
                    'StartDate': (snapshot_value['Date'].strftime("%m/%d/%Y")), 
                    'Status': snapshot_value['State'], 
                    'SnapshotId': snapshot_id, 
                    'VolumeId': volume_id}) 
        volume_value['Snapshots'] = snapshot_list 
    for instance, instance_value in instances.items(): 
        volume_list = [] 
        for volume_id, volume_value in volumes.items(): 
            for attachment in volume_value['Attachments']: 
                if instance == attachment: 
                    volume_list.append({ 
                        'VolumeId': volume_id, 
                        'VolumeDetails': volume_value}) 
        instance_value['Volumes'] = volume_list 
    logger.info(json.dumps(instances, sort_keys=False, indent=4, default=str)) 
def main(): 
    """ Main entry point of the module """ 
    logger.info("Entry point initialized.") 
    kwargs = {'ByState':'FAILED'} 
    backupjobs = get_backup_jobs(kwargs) 
    logger.info(json.dumps(backupjobs, sort_keys=False, indent=4, default=str)) 
    map_instance_volume_snapshot() 
if __name__ == "__main__": 
    main() 
