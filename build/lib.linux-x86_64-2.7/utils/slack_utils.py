import requests
import json

def send_slack(name="Spark_Monit", receiver="#data engineering", message=None, 
				web_hook='https://hooks.slack.com/services/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'):

	payload={"channel": receiver, 
		   "username": name, 
		   "text": str(message), 
		   "icon_emoji": ":fausto:"}
	
	return requests.post(web_hook, data=json.dumps(payload))
