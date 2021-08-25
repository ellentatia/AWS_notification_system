# Train issues notification system
# The notification system alert maintenance coordinators when the disturbances 'trash on the railway' or 'train mal function' are too frequent and supass the threshold
# Topic Set Up

## Initialize SNS client
sns = boto3.client('sns', region_row='eu-central-1',
		    aws_access_key_id = AWS_KEY_ID,
		    aws_secret_access_key_AWS_SECRET)	

## Create topics and store their ARNs

trash_arn = sns.create_topic(Name ='trash_notification')['TopicArn']
malfunction_arn = sns.create_topic(Name = 'malfunction_notication')['TopicArn']

## Subcribe users to the topics:
### contacts table has the name, telefone, email and department of maintenance coordinators
contacts = pd.read_csv('http://maintenance_coordinators.s3.amazonaws.com/contacts.csv')

### Create subscribe-user method
def subcribe_user(user_row):
	if user_row['Department'] == 'trash':
		sns.subscribe(TopicArn = trash_arn, Protocol = 'sms', Endpoint = str(user_row['Phone']))
		sns.subscribe(TopicArn = trash_arn, Protocol = 'email', Endpoint = user_row['Email'])
	else:
		sns.subscribe(TopicArn = malfunction_arn, Protocol = 'sms', Endpoint = str(user_row['Phone']))
		sns.subscribe(TopicArn = malfunction_arn, Protocol = 'email', Endpoint = user_row['Email'])

### Apply the subscribe_user method to every row
contacts.apply(subscribe_user, axis = 1)			  

# Get the Aggregated Numbers
## Load January's report into a DataFrame
### final report contains a count of requests for every service name
df = pd.read_csv('http://bahndisturbances_report.s3.amazonaws.com/2021/final_report.csv)

## Set the index so we can access counts by service name directly
df.set_index('disturbace_cause', inplace = True)

## Get the aggregated numbers
trash_disturbances_count = df.at['Trash on Railway', 'count']
malfunction_disturbances_count = df.at['Train malfuntion', 'count']

# Send Alerts
if trash_disturbances_count > 100:
	message = 'Trash disturbances count is now {}'.format(trash_disturbances_count)
	sns.publish(TopicArn = trash_arn,
	            Message = message,
	            Subject = 'Trash Alert')
if malfunction_disturbances_count > 30:
	message = 'Train malfunction count is now {}'.format(malfunction_disturbances_count)
	sns.publish(TopicArn = malfunction_arn,
	            Message = message,
	            Subject = 'Train Malfunction Alert')
	            

