# Case Study Building a notification System

# Topic Set Up

## Initialize SNS client
sns = boto3.client('sns', region_row='us-last-1',
		    aws_access_key_id = AWS_KEY_ID,
		    aws_secret_access_key_AWS_SECRET)	

## Create topics and store their ARNs
trash_arn = sns.create_topic(Name ='trash_notification')['TopicArn']
streets_arn = sns.create_topic(Name = 'Streets_notication')['TopicArn']

## Subcribe users to the topics:
### Create subscribe-user method
def subcribe_user(user_row):
	if user_row['Department'] == 'trash':
		sns.subscribe(TopicArn = trash_arn, Protocol = 'sns', Endpoint = str(user_row['Phone']))
		sns.subscribe(TopicArn = trash_arn, Protocol = 'email', Endpoint = user_row['Email'])

### Apply the subscribe_user method to every row
contacts.apply(subscribe_user, axis = 1)			  

# Get the Aggregated Numbers
## Load January's report into a DataFrame
df = pd.read_csv('http://get-reports.s3.amazonaws.com/2019/final_report.csv)

## Set the index so we can access counts by service name directly
df.set_index('service_name', inplace = True)

## Get the aggregated numbers
trash_violations_count = df.at['Illegal Dumping', 'count']
street_violations_count = df.at['Pothole', 'count']

# Send Alerts
if trash_violations_count > 100:
	message = 'Trash violations count is now {}'.format(trash_violation_count)
	sns.publish(TopicArn = trash_arn,
	            Message = message,
	            Subject = 'TrashAlert')
	

