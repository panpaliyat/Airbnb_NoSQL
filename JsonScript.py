import json

with open('C:/Users/tusha/Desktop/airbnb-ratings.json') as json_data:
    data = json.load(json_data)
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

print('Size ', len(data))

with open('C:/Users/tusha/Desktop/updated_file.json', 'w') as data_file:
    data = json.dump(data, data_file)
