import json
import os
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("=== metadata extractor invoked ===")
    
    # Debug: Print the entire event
    print(f"DEBUG: Full event structure:")
    print(json.dumps(event, indent=2))
    
    # Check if Records exists
    if 'Records' not in event:
        print("ERROR: No 'Records' key in event!")
        return {'statusCode': 400, 'body': 'No Records in event'}
    
    print(f"DEBUG: Number of records: {len(event['Records'])}")
    
    if len(event['Records']) == 0:
        print("ERROR: Records array is empty!")
        return {'statusCode': 400, 'body': 'Empty Records array'}
    
    try:
        for i, record in enumerate(event['Records']):
            print(f"DEBUG: Processing record {i}")
            
            if 'Sns' not in record:
                print(f"ERROR: No 'Sns' key in record {i}")
                continue
            
            sns_message = record['Sns']['Message']
            print(f"DEBUG: SNS Message (first 200 chars): {sns_message[:200]}")
            
            s3_event = json.loads(sns_message)
            print(f"DEBUG: S3 event has {len(s3_event.get('Records', []))} records")

            for j, s3_record in enumerate(s3_event['Records']):
                print(f"DEBUG: Processing S3 record {j}")
                
                bucket = s3_record['s3']['bucket']['name']
                key = s3_record['s3']['object']['key']
                size = s3_record['s3']['object']['size']
                event_time = s3_record['eventTime']

                print(f"[METADATA] File: {key}")
                print(f"[METADATA] Bucket: {bucket}")
                print(f"[METADATA] Size: {size} bytes")
                print(f"[METADATA] Upload Time: {event_time}")

                metadata = {
                    "file": key,
                    "bucket": bucket,
                    "size": size,
                    "upload_time": event_time
                }

                filename = os.path.splitext(key.split('/')[-1])[0]
                output_key = f"processed/metadata/{filename}.json"
                
                print(f"DEBUG: About to write to s3://{bucket}/{output_key}")
                
                s3.put_object(
                    Bucket=bucket,
                    Key=output_key,
                    Body=json.dumps(metadata),
                    ContentType='application/json'
                )
                
                print(f"DEBUG: Successfully wrote metadata file!")

        return {'statusCode': 200, 'body': 'metadata extracted'}
        
    except Exception as e:
        print(f"EXCEPTION: {type(e).__name__}: {str(e)}")
        import traceback
        print(f"TRACEBACK:\n{traceback.format_exc()}")
        raise