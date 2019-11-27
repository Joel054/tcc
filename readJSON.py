import json

def read_json():
    with open('config.json') as file_json:    
        data = json.load(file_json)
        print (data['exec1']['func'])
        print (data['exec1']['input'])
        print (data['exec1']['size'])
        print (data['exec1']['output'])
        print (data['exec1']['idtopic'])
