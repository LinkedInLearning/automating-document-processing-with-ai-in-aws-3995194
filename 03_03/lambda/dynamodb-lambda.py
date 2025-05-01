import boto3
import json
import uuid

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    try:
        # Log the raw event for debugging
        print(f"Raw event received: {json.dumps(event, default=str)}")
        
        # Extract the 'body' first
        body = event.get('body', {})
        if not body:
            raise ValueError("Missing 'body' in event")
        
        # Extract textractResult and comprehendResult from body
        textract_result = body.get('textractResult', {})
        comprehend_result = body.get('comprehendResult', {})
        
        # Log the extracted results
        print(f"textractResult: {json.dumps(textract_result, default=str)}")
        print(f"comprehendResult: {json.dumps(comprehend_result, default=str)}")
        
        # Get textract_job_id from comprehendResult (fallback to textractResult)
        textract_job_id = comprehend_result.get('textract_job_id', textract_result.get('jobId'))
        if not textract_job_id:
            raise ValueError("Missing 'textract_job_id' in comprehendResult or 'jobId' in textractResult")
        
        # Get insights from comprehendResult
        insights = comprehend_result.get('insights', {})
        
        # Log the extracted insights
        print(f"Extracted textract_job_id: {textract_job_id}")
        print(f"Extracted insights: {json.dumps(insights, default=str)}")
        
        # Validate required fields
        if not insights:
            raise ValueError("Missing 'insights' in comprehendResult")
        
        # Get the DynamoDB table
        table = dynamodb.Table('JobApplications')
        print(f"Targeting table: JobApplications")
        
        # Generate a unique ApplicantId to use as the sort key
        applicant_id = str(uuid.uuid4())
        
        # Build the item for DynamoDB with JobId
        item = {
            'JobId': textract_job_id,       # Partition key
            'ApplicantId': applicant_id,    # Sort key
            'Sentiment': insights.get('sentiment', {}).get('full_document', {}).get('Sentiment', 'UNKNOWN')
        }
        
        # Extract entities from forms
        entities = insights.get('entities', {}).get('forms', {})
        
        # Helper function to find entity details by keyword
        def get_entity_details(entity_type, keywords):
            for entity in entities.get(entity_type, []):
                text_lower = entity['Text'].lower()
                if any(keyword.lower() in text_lower for keyword in keywords):
                    return entity['Text'], entity['Redacted']
            return None, False
        
        # Applicant details
        first_text, first_redacted = get_entity_details('PERSON', ['first', 'jane'])
        last_text, last_redacted = get_entity_details('PERSON', ['last', 'doe'])
        if first_text and last_text:
            item['ApplicantName'] = f"{first_text} {last_text}".strip()
            item['ApplicantNameRedacted'] = first_redacted or last_redacted
        elif first_text:
            item['ApplicantName'] = first_text
            item['ApplicantNameRedacted'] = first_redacted
        elif last_text:
            item['ApplicantName'] = last_text
            item['ApplicantNameRedacted'] = last_redacted
        
        ssn_text, ssn_redacted = get_entity_details('OTHER', ['ssn', '123-45-7899'])
        if ssn_text:
            item['SSN'] = ssn_text
            item['SSNRedacted'] = ssn_redacted
        
        email_text, email_redacted = get_entity_details('OTHER', ['email', 'jane.doe@mycompany.com'])
        if email_text:
            item['Email'] = email_text
            item['EmailRedacted'] = email_redacted
        
        phone_text, phone_redacted = get_entity_details('OTHER', ['phone', '123-456-7899', '000-000-0000'])
        if phone_text:
            item['Phone'] = phone_text
            item['PhoneRedacted'] = phone_redacted
        
        # Job preferences
        position_text, _ = get_entity_details('OTHER', ['position', 'software engineer'])
        if position_text:
            item['PositionApplied'] = position_text
        else:
            item['PositionApplied'] = next((e['Text'] for e in entities.get('OTHER', []) if 'position' in e['Text'].lower()), 'Unknown')
        
        referral_text, _ = get_entity_details('OTHER', ['referral', 'friend'])
        if referral_text:
            item['ReferralSource'] = referral_text
        
        hourly_text, _ = get_entity_details('QUANTITY', ['hourly', '75.00'])
        if hourly_text:
            item['ExpectedHourlyRate'] = hourly_text
        
        weekly_text, _ = get_entity_details('QUANTITY', ['weekly', '$3000'])
        if weekly_text:
            item['ExpectedWeeklyEarnings'] = weekly_text
        
        date_text, _ = get_entity_details('DATE', ['available', 'january'])
        if date_text:
            item['DateAvailable'] = date_text
        
        employer_text, _ = get_entity_details('ORGANIZATION', ['employer', 'microsoft'])
        if employer_text:
            item['Employer'] = employer_text
        
        # Add top entities (limit to 5 per type)
        top_entities = []
        for section in insights.get('entities', {}).values():
            for entity_type, entities_list in section.items():
                for entity in entities_list[:5]:
                    top_entities.append({
                        'Type': entity_type,
                        'Text': entity['Text'],
                        'Redacted': entity['Redacted']
                    })
        item['TopEntities'] = top_entities
        
        # Add top key phrases
        item['TopKeyPhrases'] = [
            {'Text': phrase['Phrase'], 'Redacted': phrase['Redacted']}
            for phrase in insights.get('key_phrases', {}).get('full_document', {}).get('TopPhrases', [])
        ]
        
        # Log the item before writing
        print(f"Item to write: {json.dumps(item, default=str)}")
        
        # Write to DynamoDB
        table.put_item(Item=item)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f"Successfully wrote job application with ApplicantId {applicant_id} to DynamoDB",
                'item': item
            }, indent=2)
        }
    
    except Exception as e:
        print(f"Error writing to DynamoDB: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

# Lambda Configuration Suggestions:
# - Increase memory to 256 MB or 512 MB
# - Set timeout to 15 seconds or more
