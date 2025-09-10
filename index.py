from load import load_data


print('Pipeline is starting')


ind = load_data()

if ind == 1:
    print('Pipeline execution completed')
else:
    print('Pipeline execution failed')
