import boto3


class Collector:
    ec2_pricing = {
        't2.nano': 0.0073,
        't2.micro': 0.0146,
        't2.small': 0.0292,
        't2.medium': 0.0584,
        't2.large':	0.1168,
        't2.xlarge': 0.2336,
        't2.2xlarge': 0.4672,
        'm5.large': 0.12,
        'm5.xlarge': 0.24,
        'm5.2xlarge': 0.48,
        'm5.4xlarge': 0.96,
        'c5.large': 0.098,
        'c5.xlarge': 0.196,
        'c5.2xlarge': 0.392,
        'c5.4xlarge': 0.784,
        'c5.9xlarge': 1.764,
        'c5.18xlarge': 3.528,
        'c4.large': 0.115,
        'c4.xlarge': 0.231,
        'c4.2xlarge': 0.462,
        'c4.4xlarge': 0.924,
        'c4.8xlarge': 1.848
    }
    cloudformation_client = None
    ec2_client = None
    logger = None
    config = None

    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        self.cloudformation_client = boto3.client(
            service_name='cloudformation',
            aws_access_key_id=config.get('boto', 'AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=config.get('boto', 'AWS_SECRET_ACCESS_KEY'),
            region_name=config.get('boto', 'AWS_DEFAULT_REGION')
        )
        self.ec2_client = boto3.client(
            service_name='ec2',
            aws_access_key_id=config.get('boto', 'AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=config.get('boto', 'AWS_SECRET_ACCESS_KEY'),
            region_name=config.get('boto', 'AWS_DEFAULT_REGION')
        )

    def is_stack_countable(self, state):
        if state in [
                'CREATE_IN_PROGRESS',
                'CREATE_COMPLETE',
                'ROLLBACK_IN_PROGRESS',
                'ROLLBACK_COMPLETE',
                'UPDATE_IN_PROGRESS',
                'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS',
                'UPDATE_COMPLETE',
                'UPDATE_ROLLBACK_IN_PROGRESS',
                'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS',
                'UPDATE_ROLLBACK_COMPLETE',
                'REVIEW_IN_PROGRESS']:
            return True
        return False

    def get_instance_price(self, instance_type):
        return self.ec2_pricing[instance_type]

    def retrieve_instance_id(self, list_of_resources):
        for resource in list_of_resources:
            if resource['ResourceType'] == 'AWS::EC2::Instance':
                return resource['PhysicalResourceId']

    def retrieve_instance_size(self, instance_id):
        response = self.ec2_client.describe_instances(
            InstanceIds=[
                instance_id,
            ]
        ).get(
            'Reservations', []
        )
        return response[0]['Instances'][0]['InstanceType']

    def retrieve_qa_stacks(self):
        ret = {}
        response = self.cloudformation_client.describe_stacks()
        for stack in response['Stacks']:
            if not self.is_stack_countable(stack['StackStatus']):
                continue

            resources = self.cloudformation_client.describe_stack_resources(
                StackName=stack['StackName']
            )
            instance_type = self.retrieve_instance_size(
                self.retrieve_instance_id(resources['StackResources'])
            )

            user = 'Undefined'
            for output in stack['Outputs']:
                if output['OutputKey'] == 'triggeredBy' and output['OutputValue'] != '':
                    user = output['OutputValue']
                    break
            ret[stack['StackName']] = {user: instance_type}
        if not ret:
            self.logger.info('No running stacks found in {} region'.format(self.config.get('boto','AWS_DEFAULT_REGION')))
        return ret

    def get_users_expenses(self):
        result = []
        for stack, data in self.retrieve_qa_stacks().items():
            user = data.keys()[0]
            instance_type = data.values()[0]
            print("user: {}, value {}, price: {}".format(user, instance_type, self.get_instance_price(instance_type)))
            # if user not in result:
            #     result[user] = 0
            result.append({
                'user': user,
                'total_spent': self.get_instance_price(instance_type)
            }
            )
        return result
