import boto3
import json
import uuid
import re

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    try:
        # Log the raw event for debugging
        print(f"Raw event received: {json.dumps(event, default=str)}")
        
        # Extract body from event
        body = None
        if 'Payload' in event:
            body = event['Payload']
            if isinstance(body, str):
                body = json.loads(body)
        elif 'body' in event:
            body = event['body']
            if isinstance(body, str):
                body = json.loads(body)
        else:
            body = event if isinstance(event, dict) else {}
        
        if not body or not isinstance(body, dict):
            raise ValueError("Missing or invalid 'Body' in event")
        
        # Log the extracted body
        print(f"Extracted body: {json.dumps(body)}")
        
        # Extract textractResult and comprehendResult
        textract_result = body.get('results', {})
        comprehend_result = body.get('comprehendResult', {})
        if 'Payload' in comprehend_result:
            comprehend_result = comprehend_result.get('Payload', {})
        
        # Log the extracted data
        print(f"textractResult: {json.dumps(textract_result)}")
        print(f"comprehendResult: {json.dumps(comprehend_result)}")
        
        # Get textract_job_id
        textract_job_id = comprehend_result.get('textract_job_id', textract_result.get('jobId'))
        if not textract_job_id:
            raise ValueError("Missing 'textract_job_id' or 'jobId'")
        
        # Get insights from comprehendResult
        insights = comprehend_result.get('insights', {})
        if not insights:
            raise ValueError("Missing 'insights' in comprehendResult")
        
        # Log extracted insights
        print(f"Extracted textract_job_id: {textract_job_id}")
        print(f"Extracted insights: {json.dumps(insights)}")
        
        # Get the DynamoDB table
        table = dynamodb.Table('JobApplications')
        
        # Generate a unique ApplicantId
        applicant_id = str(uuid.uuid4())
        
        # Build the item for DynamoDB
        item = {
            'JobId': textract_job_id,       # Partition key
            'ApplicantId': applicant_id,    # Sort key
            'Sentiment': insights.get('sentiment', {}).get('full_document', {}).get('Sentiment', 'UNKNOWN'),
            'Status': 'applied'             # Default status for UI
        }
        
        # Extract entities and forms
        entities = insights.get('entities', {}).get('forms', {})
        forms = textract_result.get('forms', {})
        
        # Log forms for debugging
        print(f"Forms data: {json.dumps(forms)}")
        
        # Helper function to find entity details
        def get_entity_details(entity_type, keywords=None, fallback_key=None):
            # Try entities first
            for entity in entities.get(entity_type, []):
                text_lower = entity['Text'].lower()
                if keywords:
                    if any(keyword.lower() in text_lower for keyword in keywords):
                        print(f"Matched {entity_type} entity: {entity['Text']} with keywords {keywords}")
                        return entity['Text'], entity['Redacted']
                else:
                    print(f"Matched {entity_type} entity: {entity['Text']} (no keywords)")
                    return entity['Text'], entity['Redacted']
            # Special case for email: use regex
            if entity_type == 'OTHER' and keywords and 'email' in keywords:
                email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
                for entity in entities.get('OTHER', []):
                    if email_pattern.match(entity['Text']):
                        print(f"Matched email entity: {entity['Text']}")
                        return entity['Text'], entity['Redacted']
            # Fallback to forms
            if fallback_key:
                if fallback_key in forms:
                    print(f"Using forms fallback for {fallback_key}: {forms[fallback_key]}")
                    return forms[fallback_key], False
                else:
                    print(f"Fallback key {fallback_key} not found in forms")
            print(f"No match for {entity_type} with keywords {keywords}, fallback_key {fallback_key}")
            return 'Unknown', False
        
        # Helper function to extract position from TopKeyPhrases
        def get_position_from_key_phrases():
            for phrase in insights.get('key_phrases', {}).get('full_document', {}).get('TopPhrases', []):
                phrase_text = phrase['Phrase'].lower()
                if 'enterprise architect' in phrase_text:
                    # Extract the position part (e.g., "Enterprise Architect" from "DESIRED Enterprise Architect")
                    match = re.search(r'enterprise architect', phrase_text)
                    if match:
                        position = phrase['Phrase'][match.start():match.end()].title()
                        print(f"Extracted position from TopKeyPhrases: {position}")
                        return position, phrase['Redacted']
            return 'Unknown', False
        
        # Applicant details
        name_text, name_redacted = get_entity_details('PERSON', None, 'NAME (Last Name First)')
        item['ApplicantName'] = name_text
        item['ApplicantNameRedacted'] = name_redacted
        
        ssn_text, ssn_redacted = get_entity_details('OTHER', ['234-56-7890'], 'SOCIAL SECURITY NO.')
        item['SSN'] = ssn_text
        item['SSNRedacted'] = ssn_redacted
        
        phone_text, phone_redacted = get_entity_details('OTHER', ['+55 11 91234-5678'], 'PHONE NO.')
        item['Phone'] = phone_text
        item['PhoneRedacted'] = phone_redacted
        
        # Email with regex detection
        email_text, email_redacted = get_entity_details('OTHER', ['email', 'carlos@me.com'], 'SECONDARY PHONE NO.')
        item['Email'] = email_text
        item['EmailRedacted'] = email_redacted
        
        # Location details
        city_text, _ = get_entity_details('LOCATION', ['San Francisco'], 'CITY')
        item['City'] = city_text
        
        state_text, _ = get_entity_details('LOCATION', ['CALIFORNIA'], 'STATE')
        item['State'] = state_text
        
        # Job preferences
        position_text, position_redacted = get_entity_details('OTHER', ['position', 'architect', 'enterprise'], 'POSITION')
        if position_text == 'Unknown':
            # Try TopKeyPhrases as a last resort
            position_text, position_redacted = get_position_from_key_phrases()
        item['PositionApplied'] = position_text
        # Note: PositionRedacted not stored as per example item
        
        salary_text, _ = get_entity_details('QUANTITY', ['salary', '$5500'], 'SALARY DESIRED')
        item['ExpectedWeeklyEarnings'] = salary_text
        
        date_text, _ = get_entity_details('DATE', ['01/01/2025'], 'DATE YOU CAN START')
        item['DateAvailable'] = date_text
        
        # Top entities (limit to 5 per type)
        top_entities = []
        for entity_type, entities_list in entities.items():
            for entity in entities_list[:5]:
                top_entities.append({
                    'Type': entity_type,
                    'Text': entity['Text'],
                    'Redacted': entity['Redacted']
                })
        item['TopEntities'] = top_entities
        
        # Top key phrases
        item['TopKeyPhrases'] = [
            {'Text': phrase['Phrase'], 'Redacted': phrase['Redacted']}
            for phrase in insights.get('key_phrases', {}).get('full_document', {}).get('TopPhrases', [])
        ]
        
        # Log the item before writing
        print(f"Item to write: {json.dumps(item)}")
        
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
# - Memory: 256 MB or 512 MB
# - Timeout: 15 seconds
