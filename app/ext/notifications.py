


import sendgrid
from slackclient import SlackClient

class Notifications(object):

	sendgrid_client = None
	sendgrid_email_to = None
	sendgrid_email_title = None
	sendgrid_email_from = None

	slack_client = None
	slack_channel = None
	slack_bot_name = None

	def __init__(self, config):
		self.config = config

		if self.config.environment == 'production':

			if self.config.sendgrid_api_key and len(self.config.sendgrid_api_key):
				self.sendgrid_email_to = self.config.sendgrid_email_to
				self.sendgrid_email_from = self.config.sendgrid_email_from
				self.sendgrid_email_title = self.config.sendgrid_email_title
				self.sendgrid_client = sendgrid.SendGridClient(
					self.config.sendgrid_api_key
				)

			if self.config.slack_api_key and len(self.config.slack_api_key):
				self.slack_channel = self.config.slack_channel
				self.slack_bot_name = self.config.slack_bot_name
				self.slack_client = SlackClient(
					self.config.slack_api_key
				)

	def send_exception(self, e):
		message_string = 'Cassandra to Elasticsearch Sync failed: :warning:\n{}'.format(e)

		# email
		if self.sendgrid_client is not None:
			message = sendgrid.Mail()
			message.add_to(self.sendgrid_email_to)
			message.set_subject(self.sendgrid_email_title)
			message.set_text(message_string)
			message.set_from(self.sendgrid_email_from)
			status, msg = self.sendgrid_client.send(message)

		# slack
		if self.slack_client is not None:
			print self.slack_client.api_call(
			    "chat.postMessage",
			    channel=self.slack_channel,
			    text=message_string,
			    username=self.slack_bot_name,
			    icon_emoji=':robot_face:'
			)

		raise e


