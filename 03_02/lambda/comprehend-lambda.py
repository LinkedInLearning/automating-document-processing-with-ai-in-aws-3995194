import boto3
import json

# Initialize AWS clients
comprehend = boto3.client('comprehend')

def lambda_handler(event, context):
    try:
        print(f"Raw event received: {json.dumps(event, default=str)}")
        
        # Try to get results from different possible locations
        if 'results' in event:
            textract_results = event['results']
            textract_job_id = event['jobId']
        elif 'textractOutput' in event:
            textract_results = event['textractOutput']['results']
            textract_job_id = event['textractOutput']['jobId']
        else:
            raise KeyError("Could not find 'results' in event structure")
            
        print(f"Textract job ID: {textract_job_id}")
        print(f"Textract results keys: {list(textract_results.keys())}")
        
        # Prepare the insights output
        insights = {
            'entities': {},
            'sentiment': {},
            'key_phrases': {}
        }
        
        # Function to detect PII and return offsets
        def detect_pii(text):
            pii_response = comprehend.detect_pii_entities(
                Text=text,
                LanguageCode='en'
            )
            return [(entity['BeginOffset'], entity['EndOffset']) for entity in pii_response['Entities']]
        
        # Function to check if text is PII based on offsets
        def is_pii(text, pii_offsets, start_offset):
            text_start = start_offset
            text_end = start_offset + len(text)
            for begin, end in pii_offsets:
                if (begin <= text_start < end) or (begin < text_end <= end) or (text_start <= begin < text_end):
                    return True
            return False
        
        # 1. Process 'text' for sentiment and key phrases
        full_text = ' '.join(textract_results['text'])
        if full_text.strip():
            pii_offsets = detect_pii(full_text)
            
            # Detect sentiment (no redaction needed for analysis)
            sentiment_response = comprehend.detect_sentiment(
                Text=full_text[:5000],  # Comprehend 5000-byte limit
                LanguageCode='en'
            )
            insights['sentiment']['full_document'] = {
                'Sentiment': sentiment_response['Sentiment'],
                'Scores': sentiment_response['SentimentScore']
            }
            
            # Detect key phrases with PII flagging
            key_phrases_response = comprehend.detect_key_phrases(
                Text=full_text[:5000],
                LanguageCode='en'
            )
            insights['key_phrases']['full_document'] = {
                'TopPhrases': [
                    {
                        'Phrase': phrase['Text'],
                        'Score': phrase['Score'],
                        'Redacted': is_pii(phrase['Text'], pii_offsets, phrase['BeginOffset'])
                    }
                    for phrase in key_phrases_response['KeyPhrases'][:10]  # Top 10 phrases
                ]
            }
        
        # 2. Process 'forms' for entities with PII flagging
        forms_dict = textract_results['forms']
        forms_text = ' '.join([f"{key}: {value}" for key, value in forms_dict.items()])
        if forms_text.strip():
            pii_offsets = detect_pii(forms_text)
            entities_response = comprehend.detect_entities(
                Text=forms_text[:5000],
                LanguageCode='en'
            )
            entity_groups = {}
            current_offset = 0
            for entity in entities_response['Entities']:
                entity_type = entity['Type']
                if entity_type not in entity_groups:
                    entity_groups[entity_type] = []
                # Calculate approximate offset based on entity text position
                try:
                    entity_offset = forms_text.index(entity['Text'], current_offset)
                    current_offset = entity_offset + len(entity['Text'])
                except ValueError:
                    # If text isn't found, use last offset
                    entity_offset = current_offset
                entity_groups[entity_type].append({
                    'Text': entity['Text'],
                    'Score': entity['Score'],
                    'Redacted': is_pii(entity['Text'], pii_offsets, entity_offset)
                })
            insights['entities']['forms'] = entity_groups
        
        # 3. Process 'tables' for entities with PII flagging
        for table_idx, table in enumerate(textract_results['tables']):
            table_text = ' '.join([' '.join(row) for row in table['rows']])
            if table_text.strip():
                pii_offsets = detect_pii(table_text)
                entities_response = comprehend.detect_entities(
                    Text=table_text[:5000],
                    LanguageCode='en'
                )
                entity_groups = {}
                current_offset = 0
                for entity in entities_response['Entities']:
                    entity_type = entity['Type']
                    if entity_type not in entity_groups:
                        entity_groups[entity_type] = []
                    try:
                        entity_offset = table_text.index(entity['Text'], current_offset)
                        current_offset = entity_offset + len(entity['Text'])
                    except ValueError:
                        entity_offset = current_offset
                    entity_groups[entity_type].append({
                        'Text': entity['Text'],
                        'Score': entity['Score'],
                        'Redacted': is_pii(entity['Text'], pii_offsets, entity_offset)
                    })
                insights['entities'][f'table_{table_idx}'] = entity_groups
        
        # Return the enriched insights with PII indicators
        return {
            'statusCode': 200,
            'textract_job_id': textract_job_id,
            'insights': insights
        }
    
    except Exception as e:
        print(f"Error processing Comprehend analysis: {str(e)}")
        raise Exception(json.dumps({
            'statusCode': 500,
            'error': str(e)
        }))

# Lambda Configuration Suggestions:
# - Increase memory to 256 MB or 512 MB for Comprehend processing
# - Set timeout to 15 seconds or more
