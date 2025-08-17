import boto3

# Create SSM client
ssm_client = boto3.client('ssm')

def lambda_handler(event, context):
    # Replace with your EC2 instance ID
    instance_id = 'i-0c38a9ca0eda3d534'
    
    # Command to run your Python script (adjust path as needed)
    command = "bash /home/ec2-user/ecom_ingest/ingest_runner.sh"
    
    try:
        response = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={'commands': [command]},
        )
        
        command_id = response['Command']['CommandId']
        return {
            'status': 'success',
            'command_id': command_id
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }
