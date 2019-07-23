# inspired by https://www.sentialabs.io/2018/08/24/Custom-ECS-Container-Instance-Scaling-Metric.html

import boto3
import os
import datetime
import dateutil
import logging

from functools import reduce

region_name = os.environ['AWS_REGION']
ecs = boto3.client('ecs', region_name=region_name)
cw = boto3.client('cloudwatch', region_name=region_name)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Resource:
    def __init__(self, cpu, memory):
        self.cpu = cpu
        self.memory = memory

    def __str__(self):
        return f"[cpu: {self.cpu}, memory: {self.memory}]"


def retrieve_ecs_services(ecs_cluster_name):
    services_arns = ecs.list_services(cluster=ecs_cluster_name)['serviceArns']
    return ecs.describe_services(cluster=ecs_cluster_name, services=services_arns)['services']


def retrieve_ec2_instances(ecs_cluster_name):
    ec2_cluster_instances_arns = ecs.list_container_instances(cluster=ecs_cluster_name)['containerInstanceArns']
    return ecs.describe_container_instances(
        cluster=ecs_cluster_name,
        containerInstances=ec2_cluster_instances_arns)['containerInstances']


def retrieve_task_definition(service):
    return ecs.describe_task_definition(taskDefinition=service['taskDefinition'])['taskDefinition']


def get_task_resource_requirements(task_definition):
    # todo: check max cpu/memory from container definitions if not defined at task level
    # container_definitions = task_def['containerDefinitions']
    res = Resource(int(task_definition['cpu']), int(task_definition['memory']))
    logger.info(f"{task_definition['family']}:{task_definition['revision']} [cpu: {res.cpu}, memory: {res.memory}]")
    return res


def get_ec2_resources(resources):
    metrics = {
        metric['name'].lower(): metric['integerValue']
        for metric in resources if metric['name'] == 'CPU' or metric['name'] == 'MEMORY'
    }
    return Resource(metrics['cpu'], metrics['memory'])


def get_schedulable_tasks_per_instance(required_resources, ec2_instance_remaining_resources):
    return min(ec2_instance_remaining_resources.cpu // required_resources.cpu,
               ec2_instance_remaining_resources.memory // required_resources.memory)


def compute_metric(scalability_index, schedulable_task, max_schedulable_task_per_instance):
    if schedulable_task < scalability_index:
        logger.info('scale out required')
        return 1

    if schedulable_task >= max_schedulable_task_per_instance + scalability_index:
        logger.info('scale in required')
        return -1

    logger.info('no scale change required')
    return 0


def put_cloudwatch_metric(ecs_cluster_name, scale):
    cw.put_metric_data(Namespace='AWS/ECS',
                       MetricData=[{
                           'MetricName': 'RequiresScaling',
                           'Dimensions': [{'Name': 'ClusterName', 'Value': ecs_cluster_name}],
                           'Timestamp': datetime.datetime.now(dateutil.tz.tzlocal()),
                           'Value': scale
                       }])


# AWS lambda entry point
def handler(event, context):
    ecs_cluster_name = event['ecs_cluster_name']
    scalability_index = event['scalability_index']
    logger.info(f"computing autoscaling for {ecs_cluster_name}, scalability index {scalability_index}")

    ecs_services = retrieve_ecs_services(ecs_cluster_name)
    task_definitions = [retrieve_task_definition(service) for service in ecs_services]
    required_resources_per_task = [get_task_resource_requirements(task_def) for task_def in task_definitions]
    min_required_available_resources = reduce(
        lambda acc, current: Resource(max(acc.cpu, current.cpu), max(acc.memory, current.memory)),
        required_resources_per_task
    )
    logger.info(f"required available resources: {min_required_available_resources}")

    ec2_instances = retrieve_ec2_instances(ecs_cluster_name)
    ec2_remaining_resources = [get_ec2_resources(ec2_instance['remainingResources']) for ec2_instance in ec2_instances]
    logger.info(f"remaining resources per instances: {str(ec2_remaining_resources)}")
    ec2_total_resources = [get_ec2_resources(ec2_instance['registeredResources']) for ec2_instance in ec2_instances][0]

    max_schedulable_task_per_instance = get_schedulable_tasks_per_instance(min_required_available_resources,
                                                                           ec2_total_resources)
    schedulable_tasks = reduce(lambda acc, current: acc + current, [
        get_schedulable_tasks_per_instance(min_required_available_resources, ec2_instance_remaining_resource)
        for ec2_instance_remaining_resource in ec2_remaining_resources
    ], 0)
    logger.info(f"schedulable task {schedulable_tasks}")

    metric = compute_metric(scalability_index, schedulable_tasks, max_schedulable_task_per_instance)
    put_cloudwatch_metric(ecs_cluster_name, metric)


if __name__ == "__main__":
    event = {"ecs_cluster_name": "popcorn-dev", "scalability_index": 1}
    handler(event, None)
