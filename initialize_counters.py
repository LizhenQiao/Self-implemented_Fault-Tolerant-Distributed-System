import json

filenames = ['client1_counter.json', 'client2_counter.json', 'client3_counter.json']
for filename in filenames:
    with open(filename, 'w') as f:
        json.dump(1, f)
