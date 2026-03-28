import json
import os
import boto3

s3 = boto3.client('s3')

VALID_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif']

def is_valid_image(key):
    """check if the file has a valid image extension."""
    _, ext = os.path.splitext(key.lower())
    return ext in VALID_EXTENSIONS

def lambda_handler(event, context):
    print("=== image validator invoked ===")
    
    # Debug: Print the entire event
    print(f"DEBUG: Full event structure:")
    print(json.dumps(event, indent=2))
    
    # Check if Records exists
    if 'Records' not in event:
        print("ERROR: No 'Records' key in event!")
        return {'statusCode': 400, 'body': 'No Records in event'}
    
    print(f"DEBUG: Number of records: {len(event['Records'])}")
    
    try:
        for record in event['Records']:
            sns_message = record['Sns']['Message']
            print(f"DEBUG: SNS Message (first 200 chars): {sns_message[:200]}")
            
            s3_event = json.loads(sns_message)
            print(f"DEBUG: S3 event has {len(s3_event.get('Records', []))} records")

            for s3_record in s3_event['Records']:
                bucket = s3_record['s3']['bucket']['name']
                key = s3_record['s3']['object']['key']
                
                print(f"DEBUG: Checking file: {key}")
                print(f"DEBUG: Is valid image? {is_valid_image(key)}")

                if is_valid_image(key):
                    print(f"[VALID] {key} is a valid image file")
                    
                    filename = key.split('/')[-1]
                    output_key = f"processed/valid/{filename}"
                    
                    print(f"DEBUG: About to copy to s3://{bucket}/{output_key}")
                    
                    s3.copy_object(
                        Bucket=bucket,
                        Key=output_key,
                        CopySource={'Bucket': bucket, 'Key': key}   
                    )
                    
                    print(f"DEBUG: Successfully copied file!")
                else:
                    print(f"[INVALID] {key} is not a valid image type")
                    raise ValueError(f"Invalid image type: {key}")

        return {'statusCode': 200, 'body': 'validation complete'}
        
    except Exception as e:
        print(f"EXCEPTION: {type(e).__name__}: {str(e)}")
        import traceback
        print(f"TRACEBACK:\n{traceback.format_exc()}")
        raise