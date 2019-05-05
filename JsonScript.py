import json
import ast

with open('C:/Users/tusha/Desktop/airbnb-ratings.json') as json_data:
    data = json.load(json_data)
    arr = []
    for element in data:

        if 'datasetid' in element:
            element.pop('datasetid')

        if 'recordid' in element:
            element.pop('recordid')

        fields_array = element['fields']

        if 'host_about' in fields_array:
            fields_array.pop('host_about')

        if 'summary' in fields_array:
            fields_array.pop('summary')

        if 'transit' in fields_array:
            fields_array.pop('transit')

        if 'neighborhood_overview' in fields_array:
            fields_array.pop('neighborhood_overview')

        if 'space' in fields_array:
            fields_array.pop('space')

        if 'description' in fields_array:
            fields_array.pop('description')

        if 'picture_url' in fields_array:
            fields_array.pop('picture_url')

        if 'host_picture_url' in fields_array:
            fields_array.pop('host_picture_url')

        if 'xl_picture_url' in fields_array:
            fields_array.pop('xl_picture_url')

        if 'medium_url' in fields_array:
            fields_array.pop('medium_url')

        if 'thumbnail_url' in fields_array:
            fields_array.pop('thumbnail_url')

        if 'host_thumbnail_url' in fields_array:
            fields_array.pop('host_thumbnail_url')

        if 'host_url' in fields_array:
            fields_array.pop('host_url')

        if 'interaction' in fields_array:
            fields_array.pop('interaction')

        if 'house_rules' in fields_array:
            fields_array.pop('house_rules')

        if 'notes' in fields_array:
            fields_array.pop('notes')
        
        if 'host_acceptance_rate' in fields_array:
            rate = fields_array['host_acceptance_rate']
            if rate == 'N/A':
                rate = -1
            else:
                rate = rate.replace('%','')
            fields_array['host_acceptance_rate'] =  int(rate)            	    
            
        if 'host_verifications' in fields_array:
            fields_array['host_verifications'] = ast.literal_eval(fields_array['host_verifications'])
        
        arr.append(fields_array)
print('Size ', len(arr))

with open('C:/Users/tusha/Desktop/updated_file.json', 'w') as data_file:
    data = json.dump(arr, data_file)
