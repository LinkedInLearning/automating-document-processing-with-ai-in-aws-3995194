import boto3
import json
import time

# Initialize AWS clients
textract = boto3.client('textract')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    print(f"Raw event received: {json.dumps(event, default=str)}")
    
    try:
        # Get the S3 bucket and file name from the event
        bucket = event.get('bucket')
        key = event.get('key')
        
        # Validate inputs
        if not bucket or not isinstance(bucket, str):
            raise ValueError(f"Invalid or missing bucket: {bucket}")
        if not key or not isinstance(key, str):
            raise ValueError(f"Invalid or missing key: {key}")
        
        print(f"Processing file: s3://{bucket}/{key}")
        
        # Start the Textract document analysis with TABLES and FORMS
        response = textract.start_document_analysis(
            DocumentLocation={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key
                }
            },
            FeatureTypes=['TABLES', 'FORMS']
        )
        
        job_id = response['JobId']
        print(f"Started Textract job with ID: {job_id}")
        
        # Wait for and get the result
        result = get_textract_results(job_id)
        
        print(f"Textract results: {json.dumps(result, default=str)}")
        
        return {
            'statusCode': 200,
            'jobId': job_id,
            'results': result
        }
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        raise Exception(json.dumps({
            'statusCode': 500,
            'error': str(e)
        }))

def get_textract_results(job_id):
    max_attempts = 30
    attempt = 0
    
    while attempt < max_attempts:
        try:
            response = textract.get_document_analysis(JobId=job_id)
            status = response['JobStatus']
            print(f"Job {job_id} status: {status}, attempt {attempt + 1}/{max_attempts}")
            
            if status == 'SUCCEEDED':
                return process_textract_response(response)
            elif status == 'FAILED':
                raise Exception(f"Textract job {job_id} failed: {response.get('StatusMessage', 'Unknown error')}")
        except Exception as e:
            print(f"Error checking job status: {str(e)}")
            raise
        
        time.sleep(2)
        attempt += 1
    
    raise Exception('Textract job timed out')

def process_textract_response(response):
    extracted_data = {
        'text': [],
        'tables': [],
        'forms': {}
    }
    
    # Filter blocks by confidence (80% threshold)
    filtered_blocks = [block for block in response['Blocks'] if 'Confidence' not in block or block['Confidence'] >= 80]
    print(f"Filtered {len(filtered_blocks)} blocks out of {len(response['Blocks'])} total")
    
    def get_child_blocks(block, blocks):
        try:
            child_ids = [rel['Ids'] for rel in block.get('Relationships', []) if rel['Type'] == 'CHILD']
            if not child_ids:
                print(f"No child relationships for block {block.get('Id', 'unknown')}")
                return []
            child_ids = child_ids[0]  # Assuming one CHILD relationship
            return [b for b in blocks if b['Id'] in child_ids]
        except Exception as e:
            print(f"Error in get_child_blocks for block {block.get('Id', 'unknown')}: {str(e)}")
            return []

    for block in filtered_blocks:
        block_id = block.get('Id', 'unknown')
        try:
            if block['BlockType'] == 'LINE':
                if 'Text' in block:
                    extracted_data['text'].append(block['Text'])
                    print(f"Added LINE text from block {block_id}: {block['Text']}")
                else:
                    print(f"Skipping LINE block {block_id} - no 'Text' key")
            elif block['BlockType'] == 'TABLE':
                table_data = {'rows': []}
                cells = get_child_blocks(block, filtered_blocks)
                row_data = {}
                for cell in cells:
                    cell_id = cell.get('Id', 'unknown')
                    row_index = cell.get('RowIndex', 0)
                    col_index = cell.get('ColumnIndex', 0)
                    child_blocks = get_child_blocks(cell, filtered_blocks)
                    text = ' '.join([child.get('Text', '') for child in child_blocks])
                    if text.strip():
                        if row_index not in row_data:
                            row_data[row_index] = {}
                        row_data[row_index][col_index] = text
                        print(f"Added cell text from {cell_id}: {text}")
                    else:
                        print(f"Skipping empty cell {cell_id}")
                for row in sorted(row_data.keys()):
                    table_data['rows'].append([row_data[row].get(col, '') for col in sorted(row_data[row].keys())])
                if table_data['rows']:
                    extracted_data['tables'].append(table_data)
                    print(f"Added table from block {block_id}")
            elif block['BlockType'] == 'KEY_VALUE_SET' and 'EntityTypes' in block:
                if 'KEY' in block['EntityTypes']:
                    key_blocks = get_child_blocks(block, filtered_blocks)
                    key_text = ' '.join([child.get('Text', '') for child in key_blocks])
                    if key_text.strip():
                        value_block_ids = [rel['Ids'][0] for rel in block.get('Relationships', []) if rel['Type'] == 'VALUE']
                        for value_block in filtered_blocks:
                            if value_block['Id'] in value_block_ids:
                                value_blocks = get_child_blocks(value_block, filtered_blocks)
                                value_text = ' '.join([child.get('Text', '') for child in value_blocks])
                                if value_text.strip():
                                    extracted_data['forms'][key_text] = value_text
                                    print(f"Added form key-value from {block_id}: {key_text} = {value_text}")
                                else:
                                    print(f"Skipping empty value for key {key_text} in block {block_id}")
                    else:
                        print(f"Skipping empty key in block {block_id}")
        except Exception as e:
            print(f"Error processing block {block_id}: {str(e)}")
            continue
    
    return extracted_data

# Lambda Configuration Suggestions:
# - Increase memory to 256 MB or 512 MB for better performance
# - Set timeout to 15 seconds or more to handle larger documents
